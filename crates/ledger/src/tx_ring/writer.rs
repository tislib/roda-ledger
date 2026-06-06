use crate::tx_ring::ring::TxRing;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use storage::entities::WalEntry;

/// The single producer for a [`TxRing`]. Owns its `Arc<TxRing>`, so it is
/// `'static` and storable (e.g. in the transactor's `Rc<RefCell<…>>`).
///
/// All positions are absolute `ring_index`es — monotonic `usize`s starting at 0,
/// masked to physical slots only on access. Flow: `reserve` grants free slots,
/// `push` fills them (returning the index it wrote), `commit` publishes the head,
/// `rollback_to` discards an uncommitted tail.
pub struct TxRingWriter {
    ring: Arc<TxRing>,
    cursor: usize, // next ring_index to write
    limit: usize,  // exclusive upper bound before the next reserve
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

    // Free slots still available to push before the next reserve.
    pub fn capacity(&self) -> usize {
        self.limit.wrapping_sub(self.cursor)
    }

    // The next ring_index this writer will write to (the current head).
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Write `val` at the head and return the `ring_index` it landed at.
    /// Panics if the granted capacity is exhausted (`capacity() == 0`).
    pub fn push(&mut self, val: WalEntry) -> usize {
        assert_ne!(
            self.cursor, self.limit,
            "TxRingWriter: push beyond granted capacity"
        );
        let ring_index = self.cursor;
        unsafe {
            *self.ring.slots[ring_index & (self.ring.capacity - 1)].get() = val;
        }
        self.cursor = ring_index.wrapping_add(1);
        ring_index
    }

    /// Walk the half-open ring_index range `[start, end)`, lending each entry to
    /// `handler` by reference — no copy. `[start, end)` must lie in this writer's
    /// uncommitted region `[commit, cursor)`, so the borrow cannot race a reader.
    pub(crate) fn walk<F: FnMut(&WalEntry)>(&self, start: usize, end: usize, mut handler: F) {
        debug_assert!(
            end.wrapping_sub(start) <= self.ring.capacity,
            "TxRingWriter::walk: range wider than the ring"
        );
        let mask = self.ring.capacity - 1;
        let mut idx = start;
        while idx != end {
            // SAFETY: `[start, end)` is the writer's exclusive, unpublished window.
            let entry = unsafe { &*self.ring.slots[idx & mask].get() };
            handler(entry);
            idx = idx.wrapping_add(1);
        }
    }

    // Publish the head so readers and the releaser observe it.
    pub fn commit(&mut self) {
        self.ring.write.store(self.cursor, Ordering::Release);
    }

    /// Move the head back to `ring_index`, discarding the uncommitted tail
    /// `[ring_index, cursor)` so those slots are reused on the next push.
    pub fn rollback_to(&mut self, ring_index: usize) {
        debug_assert!(
            self.ring.write.load(Ordering::Relaxed) <= ring_index && ring_index <= self.cursor,
            "TxRingWriter::rollback_to: ring_index outside the uncommitted window"
        );
        self.cursor = ring_index;
    }

    /// Grant all currently-free slots from the head **without** publishing — picks
    /// up space the releaser has freed, leaving the uncommitted tail in place.
    /// Returns the new `capacity()`.
    pub fn grant(&mut self) -> usize {
        let granted = self.ring.free_from(self.cursor);
        self.limit = self.cursor.wrapping_add(granted);
        granted
    }

    /// Publish progress (`commit`), then `grant` all free slots from the head.
    /// Returns the new `capacity()`.
    pub fn reserve(&mut self) -> usize {
        self.commit();
        self.grant()
    }
}

impl Drop for TxRingWriter {
    fn drop(&mut self) {
        self.commit();
    }
}
