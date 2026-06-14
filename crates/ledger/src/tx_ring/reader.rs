use crate::tx_ring::ring::TxRing;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use storage::entities::WalEntry;

/// The single consumer for a [`TxRing`]: it both **reads** published entries and
/// **releases** their slots back to the writer (the old reader + releaser merged
/// into one SPSC endpoint). Owns its `Arc<TxRing>`.
///
/// Read window is `[released, write_index)`. Because the writer is gated on the
/// release index that only this reader advances, entries in that window are never
/// overwritten — so reads copy out directly, with no per-entry window check.
pub struct TxRingReader {
    ring: Arc<TxRing>,
    release_cursor: usize, // local release head, monotonic
}

impl TxRingReader {
    pub(super) fn new(ring: Arc<TxRing>) -> Self {
        Self {
            ring,
            release_cursor: 0,
        }
    }

    /// Walk published entries from `from` (inclusive) up to the `write_index`
    /// snapshot taken at entry, handing each a **copy** to `handler`; stop early
    /// when `handler` returns `false`. `from` must be `>= released`.
    pub fn walk(&self, from: usize, mut handler: impl FnMut(WalEntry) -> bool) {
        debug_assert!(
            from.wrapping_sub(self.release_cursor) <= self.ring.capacity,
            "TxRingReader::walk: reading below the release index"
        );
        let write = self.ring.write.load(Ordering::Acquire);
        let mask = self.ring.capacity - 1;
        let mut idx = from;
        while idx != write {
            // SAFETY: idx is in `[released, write)`, which the gated writer never
            // overwrites; we copy out by value, never lending a slot reference.
            let entry = unsafe { *self.ring.slots[idx & mask].get() };
            if !handler(entry) {
                break;
            }
            idx = idx.wrapping_add(1);
        }
    }

    /// Advance the release index to an absolute logical index; forward only.
    pub fn release_to(&mut self, index: usize) {
        debug_assert!(
            index.wrapping_sub(self.release_cursor) <= self.ring.capacity,
            "TxRingReader: release moved backward or skipped a lap"
        );
        debug_assert!(
            self.ring.write.load(Ordering::Acquire).wrapping_sub(index) <= self.ring.capacity,
            "TxRingReader: release passed the write cursor"
        );
        self.release_cursor = index;
        self.ring.release.store(index, Ordering::Release);
    }

    pub fn released(&self) -> usize {
        self.release_cursor
    }

    /// Copy of slot `idx` — random-access read for tests/inspection. The caller
    /// must keep `idx` inside `[released, write_index)`.
    /// used only for testing purposes
    #[cfg(test)]
    pub fn get(&self, idx: usize) -> WalEntry {
        // SAFETY: in-window slots are never overwritten by the gated writer.
        unsafe { *self.ring.slots[idx & (self.ring.capacity - 1)].get() }
    }

    pub fn write_index(&self) -> usize {
        self.ring.write.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.ring.capacity
    }
}
