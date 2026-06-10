//! In-memory transaction and account index — ADR-008 / ADR-022.
//!
//! Three pre-allocated, fixed-size structures; zero heap allocation after
//! construction, no cold start on segment rotation:
//!
//!   `circle1`       — maps `tx_id → (TxMetadata, location in circle2)` (direct-mapped)
//!   `circle2`       — `IndexedTxEntry` storage: each follower's raw `WalEntry`
//!                     stored as-is, plus the per-account `prev_link` chain
//!   `account_heads` — maps `account_id → latest circle2 index` (direct-mapped)
//!
//! Both circle sizes must be powers of two so modulo reduces to a bitmask.

use bytemuck::Zeroable;
use storage::entities::{TxEntry, TxMetadata, WalEntry};

// ── TxSlot (circle1) ──────────────────────────────────────────────────────────

/// One slot in `circle1` (transaction index).
///
/// `meta.tx_id == 0` means the slot is empty. A mismatch between the stored
/// `meta.tx_id` and the queried `tx_id` signals eviction — fall back to disk.
#[derive(Copy, Clone, Debug)]
pub struct TxSlot {
    pub meta: TxMetadata, // commit record (carries tx_id); meta.tx_id == 0 ⇒ empty
    pub offset: u32,      // start index in circle2
    pub entry_count: u16, // number of followers for this tx
    pub _pad: [u8; 2],
}

impl Default for TxSlot {
    fn default() -> Self {
        Self {
            meta: TxMetadata::zeroed(),
            offset: 0,
            entry_count: 0,
            _pad: [0; 2],
        }
    }
}

// ── IndexedTxEntry (circle2) ──────────────────────────────────────────────────

/// One slot in `circle2`: a follower's raw `WalEntry`, stored as-is, wrapped
/// with the bookkeeping the index needs.
///
/// - `tx_id` groups the slot with its transaction and detects eviction.
/// - `prev_link` is the 1-based per-account history chain pointer — set only
///   for `WalEntry::Entry` followers; `0` otherwise and at chain ends.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexedTxEntry {
    pub record: WalEntry,
    pub tx_id: u64,
}

impl IndexedTxEntry {
    /// Account id of the wrapped record, if it is a balance entry.
    pub fn account_id(&self) -> Option<u64> {
        match self.record {
            WalEntry::Entry(e) => Some(e.account_id),
            _ => None,
        }
    }
}

impl Default for IndexedTxEntry {
    fn default() -> Self {
        // Placeholder for empty slots; never read (tx_id == 0 marks empty).
        Self {
            record: WalEntry::Entry(TxEntry::zeroed()),
            tx_id: 0,
        }
    }
}

// ── TransactionIndexer ────────────────────────────────────────────────────────

/// In-memory transaction and account index (ADR-008 / ADR-022).
///
/// All backing buffers are pre-allocated in `new` and never resized. Both
/// circle sizes must be powers of two; enforced at construction.
pub struct TransactionIndexer {
    circle1: Vec<TxSlot>,
    circle2: Vec<IndexedTxEntry>,
    circle1_mask: usize,
    circle2_mask: usize,
    write_head2: usize, // monotonically increasing; masked on access
}

impl TransactionIndexer {
    /// Allocate all buffers. All sizes must be powers of two. Panics otherwise.
    pub fn new(circle1_size: usize, circle2_size: usize) -> Self {
        assert!(
            circle1_size.is_power_of_two(),
            "circle1_size must be a power of two, got {circle1_size}"
        );
        assert!(
            circle2_size.is_power_of_two(),
            "circle2_size must be a power of two, got {circle2_size}"
        );

        Self {
            circle1: vec![TxSlot::default(); circle1_size],
            circle2: vec![IndexedTxEntry::default(); circle2_size],
            circle1_mask: circle1_size - 1,
            circle2_mask: circle2_size - 1,
            write_head2: 0,
        }
    }

    /// Index a committed transaction: its `TxMetadata` and all followers
    /// (stored as raw `WalEntry`). `WalEntry::Entry` followers are chained into
    /// the per-account `prev_link` history; all other followers are stored too.
    pub fn insert_transaction(&mut self, meta: &TxMetadata, followers: &[WalEntry]) {
        let tx_id = meta.tx_id;
        let slot_idx = (tx_id as usize) & self.circle1_mask;
        self.circle1[slot_idx] = TxSlot {
            meta: *meta,
            offset: (self.write_head2 & self.circle2_mask) as u32,
            entry_count: followers.len() as u16,
            _pad: [0; 2],
        };

        for follower in followers {
            let idx = self.write_head2 & self.circle2_mask;
            self.circle2[idx] = IndexedTxEntry {
                record: *follower,
                tx_id,
            };
            self.write_head2 += 1;
        }
    }

    /// Look up a transaction's metadata + followers by `tx_id`.
    ///
    /// Returns `None` when the `circle1` slot has been evicted or the `circle2`
    /// slots overwritten — the caller must fall back to disk.
    pub fn get_transaction(&self, tx_id: u64) -> Option<(TxMetadata, Vec<WalEntry>)> {
        let slot = &self.circle1[(tx_id as usize) & self.circle1_mask];
        if slot.meta.tx_id != tx_id {
            return None; // empty or evicted by a colliding tx_id
        }
        let count = slot.entry_count as usize;
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let idx = (slot.offset as usize + i) & self.circle2_mask;
            let e = &self.circle2[idx];
            if e.tx_id != tx_id {
                return None; // circle2 slot overwritten — evicted
            }
            entries.push(e.record);
        }
        Some((slot.meta, entries))
    }
}

// ── compile-time size assertions ──────────────────────────────────────────────

const _: () = assert!(std::mem::size_of::<TxSlot>() == 48);

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use storage::entities::{EntryKind, FailReason, WalEntryKind};

    fn small() -> TransactionIndexer {
        // circle1=16, circle2=64, account_heads=16 — small enough for fast tests
        TransactionIndexer::new(16, 64)
    }

    fn meta(tx_id: u64) -> TxMetadata {
        TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: 0,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        }
    }

    fn entry(account_id: u64, amount: u64, kind: EntryKind, computed_balance: i64) -> WalEntry {
        WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind,
            _pad0: [0; 6],
            _pad1: [0; 8],
            account_id,
            amount,
            computed_balance,
        })
    }

    fn as_entry(w: &WalEntry) -> &TxEntry {
        match w {
            WalEntry::Entry(e) => e,
            _ => panic!("expected WalEntry::Entry"),
        }
    }

    // ── get_transaction ───────────────────────────────────────────────────────

    #[test]
    fn get_transaction_single_entry_hit() {
        let mut idx = small();
        idx.insert_transaction(&meta(1), &[entry(100, 500, EntryKind::Credit, 500)]);

        let (m, entries) = idx.get_transaction(1).expect("cache hit expected");
        assert_eq!(m.tx_id, 1);
        assert_eq!(entries.len(), 1);
        let e = as_entry(&entries[0]);
        assert_eq!(e.account_id, 100);
        assert_eq!(e.amount, 500);
        assert_eq!(e.computed_balance, 500);
        assert_eq!(e.kind, EntryKind::Credit);
    }

    #[test]
    fn get_transaction_multiple_entries_hit() {
        let mut idx = small();
        idx.insert_transaction(
            &meta(2),
            &[
                entry(200, 1_000, EntryKind::Debit, -1_000),
                entry(201, 1_000, EntryKind::Credit, 1_000),
            ],
        );

        let (_m, entries) = idx.get_transaction(2).expect("cache hit expected");
        assert_eq!(entries.len(), 2);
        assert_eq!(as_entry(&entries[0]).account_id, 200);
        assert_eq!(as_entry(&entries[1]).account_id, 201);
    }

    #[test]
    fn get_transaction_miss_for_unknown_tx() {
        let idx = small();
        assert!(idx.get_transaction(99).is_none());
    }

    #[test]
    fn get_transaction_miss_after_circle1_eviction() {
        let mut idx = small(); // circle1_size = 16 → mask = 15
        // tx_id 1 and tx_id 17 both map to slot 1
        idx.insert_transaction(&meta(1), &[entry(100, 100, EntryKind::Credit, 100)]);
        idx.insert_transaction(&meta(17), &[entry(101, 200, EntryKind::Debit, -200)]);

        assert!(idx.get_transaction(1).is_none()); // evicted
        assert!(idx.get_transaction(17).is_some());
    }

    #[test]
    fn get_transaction_miss_after_circle2_eviction() {
        let mut idx = TransactionIndexer::new(16, 4); // circle2 wraps quickly

        idx.insert_transaction(&meta(1), &[entry(100, 10, EntryKind::Credit, 10)]);
        for tx_id in 2..=4 {
            idx.insert_transaction(&meta(tx_id), &[entry(tx_id * 10, 1, EntryKind::Credit, 1)]);
        }
        // one more entry overwrites circle2 slot 0 (where tx 1 was)
        idx.insert_transaction(&meta(5), &[entry(50, 1, EntryKind::Credit, 1)]);

        assert!(idx.get_transaction(1).is_none());
    }

    #[test]
    fn get_transaction_returns_non_entry_followers_as_is() {
        use storage::entities::{TxLink, TxLinkKind};
        let mut idx = small();
        let link = WalEntry::Link(TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            _pad1: [0; 8],
            to_tx_id: 42,
            _pad2: [0; 16],
        });
        idx.insert_transaction(&meta(7), &[link]);
        let (_m, entries) = idx.get_transaction(7).expect("hit");
        assert_eq!(entries.len(), 1);
        match entries[0] {
            WalEntry::Link(l) => assert_eq!(l.to_tx_id, 42),
            _ => panic!("expected the stored link, as-is"),
        }
    }

    // ── new() panics on non-power-of-two ─────────────────────────────────────

    #[test]
    #[should_panic(expected = "circle1_size must be a power of two")]
    fn new_panics_on_non_power_of_two_circle1() {
        TransactionIndexer::new(15, 16);
    }

    #[test]
    #[should_panic(expected = "circle2_size must be a power of two")]
    fn new_panics_on_non_power_of_two_circle2() {
        TransactionIndexer::new(16, 30);
    }
}
