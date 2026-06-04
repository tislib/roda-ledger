use crate::tx_ring::ring::TxRing;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use storage::entities::WalEntry;

/// The single producer for a [`TxRing`]. Owns its `Arc<TxRing>`, so it is
/// `'static` and can be stored (e.g. inside the transactor's
/// `Rc<RefCell<TransactorState>>`). Exactly one is yielded from
/// [`TxRing::new`] and it is not `Clone`, so the single-writer invariant holds
/// by construction.
///
/// Flow: `reserve` grants a window of free slots, `push` fills them, `commit`
/// publishes the cursor to readers/releaser, `rollback` discards uncommitted
/// pushes, and `reserve` again re-grants from the (possibly advanced) release
/// index.
pub struct TxRingWriter {
    ring: Arc<TxRing>,
    cursor: usize, // local write head, no atomics
    limit: usize,  // highest index this writer may push to before the next reserve
}

impl TxRingWriter {
    pub(super) fn new(ring: Arc<TxRing>) -> Self {
        let start = ring.write.load(Ordering::Relaxed);
        Self {
            ring,
            cursor: start,
            limit: start,
        }
    }

    // Slots still available to push before the next reserve.
    pub fn capacity(&self) -> usize {
        self.limit.wrapping_sub(self.cursor)
    }

    // Pushed-but-uncommitted entries: the half-open window `[commit, cursor)`.
    pub fn pending(&self) -> usize {
        self.cursor
            .wrapping_sub(self.ring.write.load(Ordering::Relaxed))
    }

    /// Write a value into the next granted slot.
    ///
    /// # Panics
    /// Panics if the caller exceeds the granted capacity (`capacity() == 0`).
    pub fn push(&mut self, val: WalEntry) {
        assert_ne!(
            self.cursor, self.limit,
            "TxRingWriter: push beyond granted capacity"
        );
        let idx = self.cursor & (self.ring.capacity - 1);
        unsafe {
            *self.ring.slots[idx].get() = val;
        }
        self.cursor = self.cursor.wrapping_add(1);
    }

    /// Walk the half-open logical range `[start, end)` in push order, lending
    /// each entry to `handler` by reference — never a copy. Indices are absolute
    /// ring positions, masked to physical slots, so the range may wrap.
    ///
    /// Caller contract: `[start, end)` must lie inside this writer's own
    /// pushed-but-uncommitted region `[commit, cursor)`. Those slots are owned
    /// exclusively by the writer and not yet published, so no reader can observe
    /// them and the borrow cannot race a write. It never escapes the call.
    pub(crate) fn walk<F: FnMut(&WalEntry)>(&self, start: usize, end: usize, mut handler: F) {
        debug_assert!(
            end.wrapping_sub(start) <= self.ring.capacity,
            "TxRingWriter::walk: range wider than the ring"
        );
        let mask = self.ring.capacity - 1;
        let mut idx = start;
        while idx != end {
            // SAFETY: per the contract `[start, end)` is the writer's exclusive,
            // unpublished window, so this shared borrow cannot alias a write.
            let entry = unsafe { &*self.ring.slots[idx & mask].get() };
            handler(entry);
            idx = idx.wrapping_add(1);
        }
    }

    /// Walk the pushed-but-uncommitted entries, skipping the first `skip`:
    /// the range `[commit + skip, cursor)`. A `skip` past `pending()` walks
    /// nothing. No copy.
    pub(crate) fn walk_pending<F: FnMut(&WalEntry)>(&self, skip: usize, handler: F) {
        let walked = self.pending().saturating_sub(skip);
        self.walk(self.cursor.wrapping_sub(walked), self.cursor, handler);
    }

    /// Mutate the pushed-but-uncommitted entry at offset `skip` from the commit
    /// point (`[commit + skip]`). Used to back-patch a record after later ones
    /// are known (e.g. a metadata CRC). No copy.
    ///
    /// # Panics (debug)
    /// Panics if `skip >= pending()` — the slot is outside the live window.
    pub(crate) fn patch_pending<F: FnOnce(&mut WalEntry)>(&mut self, skip: usize, f: F) {
        debug_assert!(
            skip < self.pending(),
            "TxRingWriter::patch_pending: offset past the pending window"
        );
        let idx = self.ring.write.load(Ordering::Relaxed).wrapping_add(skip) & (self.ring.capacity - 1);
        // SAFETY: `[commit, cursor)` is the writer's exclusive, unpublished
        // window; with `&mut self` no walk/reader borrow is live concurrently.
        let entry = unsafe { &mut *self.ring.slots[idx].get() };
        f(entry);
    }

    // Publish the current cursor so readers and the releaser observe it.
    pub fn commit(&mut self) {
        self.ring.write.store(self.cursor, Ordering::Release);
    }

    /// Move the write head back by `count`, discarding that many pushed-but-
    /// uncommitted entries so their slots are reused on the next `push`.
    /// Clamped to `pending()`; returns how many entries were rolled back.
    pub fn rollback(&mut self, count: usize) -> usize {
        let n = count.min(self.pending());
        self.cursor = self.cursor.wrapping_sub(n);
        n
    }

    /// Publish progress, then grant *all* currently-free slots (everything the
    /// releaser has freed) from the current head. Returns the new `capacity()`.
    pub fn reserve(&mut self) -> usize {
        self.commit();
        let granted = self.ring.free_from(self.cursor);
        self.limit = self.cursor.wrapping_add(granted);
        granted
    }
}

impl Drop for TxRingWriter {
    fn drop(&mut self) {
        self.commit();
    }
}
