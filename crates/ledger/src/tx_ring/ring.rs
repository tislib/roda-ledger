use crate::tx_ring::reader::TxRingReader;
use crate::tx_ring::writer::TxRingWriter;
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage::entities::WalBinaryRecord;

/// Lock-free SPSC slot buffer. Lives behind an `Arc` held by exactly two handles
/// — the [`TxRingWriter`] and the [`TxRingReader`] — and is dropped when both are
/// dropped. The element type is fixed to a `Copy` 40-byte `WalEntry`, so reads
/// copy out by value and never lend a reference into a live slot.
pub struct TxRing {
    pub(super) slots: Box<[UnsafeCell<WalBinaryRecord>]>,
    pub(super) write: CachePadded<AtomicUsize>, // write frontier; only the writer advances it
    pub(super) release: CachePadded<AtomicUsize>, // consumed high-water; only the reader advances it
    pub(super) capacity: usize,                   // power of two
}

// The writer gates on `release`, so a published slot is never overwritten while
// still readable; the reader only reads in `[release, write)`.
unsafe impl Send for TxRing {}
unsafe impl Sync for TxRing {}

impl TxRing {
    /// Create an SPSC ring and hand out its single producer ([`TxRingWriter`]) and
    /// single consumer ([`TxRingReader`], which also releases slots). The ring is
    /// not returned — it lives behind an `Arc` shared by the two handles and is
    /// dropped once both are dropped.
    #[allow(clippy::new_ret_no_self)] // intentional: hands out the SPSC handle pair
    pub fn new(capacity: usize) -> (TxRingWriter, TxRingReader) {
        assert!(
            capacity.is_power_of_two(),
            "TxRing: capacity must be a power of two"
        );
        let ring = Arc::new(Self {
            slots: (0..capacity)
                .map(|_| UnsafeCell::new(WalBinaryRecord::empty()))
                .collect(),
            write: CachePadded::new(AtomicUsize::new(0)),
            release: CachePadded::new(AtomicUsize::new(0)),
            capacity,
        });
        let writer = TxRingWriter::new(Arc::clone(&ring));
        let reader = TxRingReader::new(ring);
        (writer, reader)
    }

    // Free slots ahead of `frontier`, bounded by the reader's release progress.
    pub(super) fn free_from(&self, frontier: usize) -> usize {
        let occupied = frontier.wrapping_sub(self.release.load(Ordering::Acquire));
        self.capacity.saturating_sub(occupied)
    }
}
