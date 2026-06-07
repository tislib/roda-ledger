use crate::tx_ring::ring::TxRing;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use storage::WAL_RECORD_SIZE;
use storage::entities::{WalBinaryRecord, WalEntry};

/// Outcome of [`TxRingReader::read_next_batch`] — the first and last complete
/// transaction whose records were written this call (both `None` when none was ready).
pub struct Batch {
    pub first_tx: Option<u64>,
    pub last_tx: Option<u64>,
}

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
            let entry = unsafe { (*self.ring.slots[idx & mask].get()).to_wal_entry() };
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
    pub fn get(&self, idx: usize) -> WalEntry {
        // SAFETY: in-window slots are never overwritten by the gated writer.
        unsafe { (*self.ring.slots[idx & (self.ring.capacity - 1)].get()).to_wal_entry() }
    }

    /// Read the next run of **complete** transactions from the internal cursor, hand
    /// their records to `write` as one contiguous byte slice, then advance the cursor and
    /// release those slots — reading and releasing are one step. The buffer **never
    /// leaves** `write`. At most `max_tx_count` transactions are included (the run is cut
    /// right after that many closing `TxMetadata` records) and a transaction is never
    /// split — so the caller sizes `max_tx_count` to the active segment's remaining room
    /// and a tx never straddles two segments. Returns the first/last tx written, or
    /// `{None, None}` if no complete transaction is ready yet.
    pub fn read_next_batch(&mut self, max_tx_count: usize, write: impl FnOnce(&[u8])) -> Batch {
        let write_idx = self.ring.write.load(Ordering::Acquire);
        let from = self.release_cursor;
        let available = write_idx.wrapping_sub(from);
        if available == 0 || max_tx_count == 0 {
            return Batch {
                first_tx: None,
                last_tx: None,
            };
        }
        let phys = from & (self.ring.capacity - 1);
        let to_wrap = self.ring.capacity - phys; // records until the physical buffer end
        let len = available.min(to_wrap);
        // SAFETY: `[from, from+len)` is published, contiguous (bounded to not wrap), and
        // `from == release` so the gated writer never overwrites it. `UnsafeCell<T>` has
        // the same layout as `T`; the run is one allocation.
        let slice: &[WalBinaryRecord] = unsafe {
            std::slice::from_raw_parts(self.ring.slots[phys].get() as *const WalBinaryRecord, len)
        };

        // Walk forward, counting complete txs (each `TxMetadata` closes one); cut right
        // after the `max_tx_count`-th, or at the last complete tx the window holds.
        let mut first_tx = None;
        let mut last_tx = None;
        let mut tx_count = 0;
        let mut end = 0; // records up to and including the last counted metadata
        for (i, rec) in slice.iter().enumerate() {
            if let Some(tx_id) = rec.metadata_tx_id() {
                first_tx.get_or_insert(tx_id);
                last_tx = Some(tx_id);
                end = i + 1;
                tx_count += 1;
                if tx_count >= max_tx_count {
                    break;
                }
            }
        }
        if end == 0 {
            // No closing metadata in this window. If the physical wrap truncated us before
            // it (a tx straddling the ring boundary), the pre-wrap followers are still
            // published — write them and advance, so the metadata (and last_tx) arrive on
            // the next, post-wrap call; the tx stays contiguous on disk. Otherwise the
            // published window genuinely holds no complete tx yet — back off.
            if len < available {
                end = len;
            } else {
                return Batch {
                    first_tx: None,
                    last_tx: None,
                };
            }
        }

        // SAFETY: `WalBinaryRecord` is `#[repr(transparent)]` over `[u8; WAL_RECORD_SIZE]`,
        // so the first `end` records are exactly `end * WAL_RECORD_SIZE` contiguous bytes.
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(slice.as_ptr() as *const u8, end * WAL_RECORD_SIZE)
        };
        write(bytes);

        self.release_to(from.wrapping_add(end));
        Batch { first_tx, last_tx }
    }

    pub fn write_index(&self) -> usize {
        self.ring.write.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.ring.capacity
    }
}
