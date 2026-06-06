use crate::tx_ring::releaser::TxRingReleaser;
use crate::tx_ring::writer::TxRingWriter;
use bytemuck::Zeroable;
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};
use storage::entities::{TxEntry, WalEntry};

pub struct TxRing {
    pub(super) slots: Box<[UnsafeCell<WalEntry>]>,
    pub(super) write: CachePadded<AtomicUsize>, // write frontier; only the writer advances it
    pub(super) release: CachePadded<AtomicUsize>, // consumed high-water; only the releaser advances it
    pub(super) capacity: usize,                   // power of two
}

// Writer gates on `release`, so a published slot is never overwritten while still readable.
unsafe impl Send for TxRing {}
unsafe impl Sync for TxRing {}

impl TxRing {
    pub fn new(capacity: usize) -> (Arc<Self>, TxRingWriter, TxRingReleaser) {
        assert!(
            capacity.is_power_of_two(),
            "TxRing: capacity must be a power of two"
        );
        let ring = Arc::new(Self {
            slots: (0..capacity)
                .map(|_| UnsafeCell::new(empty_entry()))
                .collect(),
            write: CachePadded::new(AtomicUsize::new(0)),
            release: CachePadded::new(AtomicUsize::new(0)),
            capacity,
        });
        let writer = TxRingWriter::new(Arc::clone(&ring));
        let releaser = TxRingReleaser {
            ring: Arc::clone(&ring),
            cursor: 0,
        };
        (ring, writer, releaser)
    }

    pub fn write_index(&self) -> usize {
        self.write.load(Ordering::Acquire)
    }

    pub fn release_index(&self) -> usize {
        self.release.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Walk published entries from `last_ring_index` (inclusive) up to the
    /// `write_index` snapshot taken at entry, handing each a **copy** to
    /// `handler`; stop early when `handler` returns `false`.
    ///
    /// Copies out (never lends a slot reference) because the writer may reclaim
    /// a slot once the releaser advances — same window discipline as
    /// [`Self::get`], which this reuses. **Panics** (via `get`) if the reader
    /// has fallen below `release_index`, i.e. left the live window.
    pub fn walk_entries(&self, last_ring_index: usize, mut handler: impl FnMut(WalEntry) -> bool) {
        let write = self.write.load(Ordering::Acquire);
        let mut idx = last_ring_index;
        while idx != write {
            if !handler(self.get(idx)) {
                break;
            }
            idx = idx.wrapping_add(1);
        }
    }

    /// Copy of slot `idx`.
    ///
    /// Reads the slot first, *then* validates that `idx` was inside the live window
    /// `[release_index, write_index)`. Checking before reading would be a TOCTOU race: the
    /// writer could advance `release` and overwrite the slot between the check and the read.
    ///
    /// # Panics
    /// Panics if `idx` was outside the live window, i.e. the value may be stale or torn.
    pub fn get(&self, idx: usize) -> WalEntry {
        let value = unsafe { *self.slots[idx & (self.capacity - 1)].get() };
        fence(Ordering::Acquire);
        let release = self.release.load(Ordering::Relaxed);
        let write = self.write.load(Ordering::Relaxed);
        assert!(
            release <= idx && idx < write,
            "TxRing::get({idx}) read outside live window [{release}, {write})"
        );
        value
    }

    // Free slots ahead of `frontier`, bounded by the releaser's progress.
    pub(super) fn free_from(&self, frontier: usize) -> usize {
        let occupied = frontier.wrapping_sub(self.release.load(Ordering::Acquire));
        self.capacity.saturating_sub(occupied)
    }
}

// Placeholder filling every slot until the writer overwrites it; never observed by a
// caller that respects the [release, write) window.
fn empty_entry() -> WalEntry {
    WalEntry::Entry(TxEntry::zeroed())
}
