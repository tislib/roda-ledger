//! In-memory transaction and account index — ADR-008.
//!
//! Three pre-allocated, fixed-size structures; zero heap allocation after
//! construction, no cold start on segment rotation:
//!
//!   `circle1`       — maps `tx_id → location in circle2` (direct-mapped cache)
//!   `circle2`       — `IndexedTxEntry` storage + per-account `prev_link` chain
//!   `account_heads` — maps `account_id → latest circle2 index` (direct-mapped)
//!
//! Both circle sizes must be powers of two so modulo reduces to a bitmask.

use std::collections::HashMap;
use storage::entities::{EntryKind, TxLinkKind, WalEntryKind};

// ── TxSlot (circle1) ──────────────────────────────────────────────────────────

/// One slot in `circle1` (transaction index). 16 bytes.
///
/// `tx_id == 0` means the slot is empty. A mismatch between the stored
/// `tx_id` and the queried `tx_id` signals eviction — fall back to disk.
#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct TxSlot {
    pub tx_id: u64,       // 8 @ 0  — 0 = empty slot
    pub offset: u32,      // 4 @ 8  — start index in circle2
    pub entry_count: u16, // 2 @ 12 — number of IndexedTxEntry records for this tx
    pub _pad: [u8; 2],    // 2 @ 14
} // 16 bytes total

// ── IndexedTxEntry (circle2) ──────────────────────────────────────────────────

/// One entry in `circle2` (entry storage + account link chain). 48 bytes.
///
/// `prev_link == 0` means no earlier entry in `circle2` for the same account.
/// A mismatch on `account_id` while following `prev_link` signals eviction —
/// the slot was overwritten by a newer entry; the chain is broken naturally.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexedTxEntry {
    pub entry_type: u8,        // 1 @ 0  — WalEntryKind::TxEntry
    pub kind: EntryKind,       // 1 @ 1  — Credit or Debit
    pub _pad: [u8; 6],         // 6 @ 2  — align tx_id to 8
    pub tx_id: u64,            // 8 @ 8
    pub account_id: u64,       // 8 @ 16
    pub amount: u64,           // 8 @ 24
    pub computed_balance: i64, // 8 @ 32 — balance after this entry
    pub prev_link: u32,        // 4 @ 40 — 1-based circle2 index of same-account previous entry;
    //          0 = no previous entry in buffer
    //          N = entry is at circle2 slot N-1
    pub _pad_end: [u8; 4], // 4 @ 44 — explicit tail pad to reach alignment boundary
} // 48 bytes total

impl Default for IndexedTxEntry {
    fn default() -> Self {
        Self {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad: [0; 6],
            tx_id: 0,
            account_id: 0,
            amount: 0,
            computed_balance: 0,
            prev_link: 0,
            _pad_end: [0; 4],
        }
    }
}

// ── IndexedTxLink ────────────────────────────────────────────────────────────

/// A stored link associated with a transaction.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexedTxLink {
    pub kind: TxLinkKind,
    pub to_tx_id: u64,
}

// ── TransactionIndexer ────────────────────────────────────────────────────────

/// In-memory transaction and account index (ADR-008).
///
/// All three backing buffers are pre-allocated in `new` and never resized.
/// Both circle sizes must be powers of two; this is enforced at construction.
pub struct TransactionIndexer {
    circle1: Vec<TxSlot>,
    circle2: Vec<IndexedTxEntry>,
    account_heads: Vec<(u64, u32)>, // (account_id, circle2_idx)
    circle1_mask: usize,
    circle2_mask: usize,
    account_heads_mask: usize,
    write_head2: usize, // monotonically increasing; masked on access
    /// Links indexed by tx_id. Bounded implicitly by transaction throughput;
    /// most transactions have zero links (common case: empty map entries).
    links: HashMap<u64, Vec<IndexedTxLink>>,
}

impl TransactionIndexer {
    /// Allocate all three buffers.
    ///
    /// All sizes must be powers of two. Panics otherwise.
    pub fn new(circle1_size: usize, circle2_size: usize, account_heads_size: usize) -> Self {
        assert!(
            circle1_size.is_power_of_two(),
            "circle1_size must be a power of two, got {circle1_size}"
        );
        assert!(
            circle2_size.is_power_of_two(),
            "circle2_size must be a power of two, got {circle2_size}"
        );
        assert!(
            account_heads_size.is_power_of_two(),
            "account_heads_size must be a power of two, got {account_heads_size}"
        );

        Self {
            circle1: vec![TxSlot::default(); circle1_size],
            circle2: vec![IndexedTxEntry::default(); circle2_size],
            account_heads: vec![(0u64, 0u32); account_heads_size],
            circle1_mask: circle1_size - 1,
            circle2_mask: circle2_size - 1,
            account_heads_mask: account_heads_size - 1,
            write_head2: 0,
            links: HashMap::new(),
        }
    }

    /// Record a transaction's starting position in `circle2`.
    ///
    /// Must be called once per `TxMetadata`, before the matching
    /// `insert_entry` calls. The slot's `entry_count` starts at 0 and
    /// is bumped by each `insert_entry` for the same tx_id — the WAL
    /// meta no longer carries a TxEntry-only count, only a combined
    /// `sub_item_count`, so the indexer counts entries as they arrive.
    pub fn insert_tx(&mut self, tx_id: u64) {
        let slot_idx = (tx_id as usize) & self.circle1_mask;
        self.circle1[slot_idx] = TxSlot {
            tx_id,
            offset: (self.write_head2 & self.circle2_mask) as u32,
            entry_count: 0,
            _pad: [0; 2],
        };
    }

    /// Append one entry to `circle2` and update the account head chain.
    ///
    /// Must be called once per `TxEntry`, in WAL order, after `insert_tx`.
    /// Automatically links the new entry into the per-account `prev_link`
    /// chain and advances `write_head2`.
    pub fn insert_entry(
        &mut self,
        tx_id: u64,
        account_id: u64,
        amount: u64,
        kind: EntryKind,
        computed_balance: i64,
    ) {
        let slot_idx = self.write_head2 & self.circle2_mask;

        let head_slot = (account_id as usize) & self.account_heads_mask;
        // prev_link is 1-based: 0 = none, N = circle2 slot N-1.
        // This avoids ambiguity between sentinel and slot index 0.
        let prev_link = if self.account_heads[head_slot].0 == account_id {
            self.account_heads[head_slot].1 + 1 // convert 0-based slot → 1-based link
        } else {
            0 // no previous entry in buffer for this account
        };

        self.circle2[slot_idx] = IndexedTxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind,
            _pad: [0; 6],
            tx_id,
            account_id,
            amount,
            computed_balance,
            prev_link,
            _pad_end: [0; 4],
        };

        self.account_heads[head_slot] = (account_id, slot_idx as u32);
        self.write_head2 += 1;

        // Bump the per-tx entry counter on circle1 so `get_transaction`
        // knows how many circle2 slots to read. Skip if the tx_id was
        // evicted (collision) — the caller will see a None on lookup.
        let tx_slot_idx = (tx_id as usize) & self.circle1_mask;
        if self.circle1[tx_slot_idx].tx_id == tx_id {
            self.circle1[tx_slot_idx].entry_count =
                self.circle1[tx_slot_idx].entry_count.saturating_add(1);
        }
    }

    /// Record a link for a transaction.
    pub fn insert_link(&mut self, tx_id: u64, kind: TxLinkKind, to_tx_id: u64) {
        self.links
            .entry(tx_id)
            .or_default()
            .push(IndexedTxLink { kind, to_tx_id });
    }

    /// Look up links for a transaction by `tx_id`.
    pub fn get_links(&self, tx_id: u64) -> Option<&Vec<IndexedTxLink>> {
        self.links.get(&tx_id)
    }

    /// Look up a transaction's entries by `tx_id`.
    ///
    /// Returns `Some(entries)` on a cache hit. Returns `None` when the
    /// `circle1` slot has been evicted or the `circle2` slots overwritten —
    /// the caller must fall back to on-disk WAL index + segment seek.
    pub fn get_transaction(&self, tx_id: u64) -> Option<Vec<IndexedTxEntry>> {
        let slot_idx = (tx_id as usize) & self.circle1_mask;
        let slot = &self.circle1[slot_idx];

        if slot.tx_id != tx_id {
            return None; // empty or evicted by a colliding tx_id
        }

        let count = slot.entry_count as usize;
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let idx = (slot.offset as usize + i) & self.circle2_mask;
            let entry = &self.circle2[idx];
            if entry.tx_id != tx_id {
                return None; // circle2 slot overwritten — evicted
            }
            entries.push(*entry);
        }
        Some(entries)
    }

    /// Traverse the `prev_link` chain for `account_id` and collect entries.
    ///
    /// Results are newest-first (reverse chronological). Stops when:
    /// - `limit` entries are collected,
    /// - `from_tx_id != 0` and `entry.tx_id < from_tx_id` (gone past boundary),
    /// - the chain ends (`prev_link == 0`), or
    /// - an evicted slot is detected (`account_id` mismatch).
    ///
    /// An empty return signals a full cache miss — caller falls back to disk.
    pub fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        limit: usize,
    ) -> Vec<IndexedTxEntry> {
        let head_slot = (account_id as usize) & self.account_heads_mask;
        if self.account_heads[head_slot].0 != account_id {
            return Vec::new(); // miss — account head overwritten by collision
        }

        let mut results = Vec::new();
        let mut current = self.account_heads[head_slot].1 as usize;

        loop {
            let entry = &self.circle2[current & self.circle2_mask];

            if entry.account_id != account_id {
                break; // slot evicted — chain broken
            }
            if from_tx_id != 0 && entry.tx_id < from_tx_id {
                break; // gone past the requested lower bound
            }

            results.push(*entry);

            if results.len() >= limit {
                break;
            }
            if entry.prev_link == 0 {
                break; // no earlier entry in buffer
            }
            current = (entry.prev_link - 1) as usize; // convert 1-based link → 0-based slot
        }

        results
    }
}

// ── compile-time size assertions ──────────────────────────────────────────────

const _: () = assert!(std::mem::size_of::<TxSlot>() == 16);
const _: () = assert!(std::mem::size_of::<IndexedTxEntry>() == 48);

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use storage::entities::EntryKind;

    fn small() -> TransactionIndexer {
        // circle1=16, circle2=64, account_heads=16 — small enough for fast tests
        TransactionIndexer::new(16, 64, 16)
    }

    // ── get_transaction ───────────────────────────────────────────────────────

    #[test]
    fn get_transaction_single_entry_hit() {
        let mut idx = small();
        idx.insert_tx(1);
        idx.insert_entry(1, 100, 500, EntryKind::Credit, 500);

        let entries = idx.get_transaction(1).expect("cache hit expected");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tx_id, 1);
        assert_eq!(entries[0].account_id, 100);
        assert_eq!(entries[0].amount, 500);
        assert_eq!(entries[0].computed_balance, 500);
        assert_eq!(entries[0].kind, EntryKind::Credit);
    }

    #[test]
    fn get_transaction_multiple_entries_hit() {
        let mut idx = small();
        idx.insert_tx(2);
        idx.insert_entry(2, 200, 1_000, EntryKind::Debit, -1_000);
        idx.insert_entry(2, 201, 1_000, EntryKind::Credit, 1_000);

        let entries = idx.get_transaction(2).expect("cache hit expected");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].account_id, 200);
        assert_eq!(entries[1].account_id, 201);
    }

    #[test]
    fn get_transaction_miss_for_unknown_tx() {
        let idx = small();
        assert!(idx.get_transaction(99).is_none());
    }

    #[test]
    fn get_transaction_miss_after_circle1_eviction() {
        let mut idx = small(); // circle1_size = 16 → mask = 15
        // tx_id 1 and tx_id 17 both map to slot 1 (1 % 16 == 17 % 16)
        idx.insert_tx(1);
        idx.insert_entry(1, 100, 100, EntryKind::Credit, 100);

        idx.insert_tx(17);
        idx.insert_entry(17, 101, 200, EntryKind::Debit, -200);

        // tx 1 was evicted from circle1 slot 1 by tx 17
        assert!(idx.get_transaction(1).is_none());
        // tx 17 is still present
        assert!(idx.get_transaction(17).is_some());
    }

    #[test]
    fn get_transaction_miss_after_circle2_eviction() {
        // circle2_size = 4 so it wraps quickly
        let mut idx = TransactionIndexer::new(16, 4, 16);

        idx.insert_tx(1);
        idx.insert_entry(1, 100, 10, EntryKind::Credit, 10);

        // fill circle2 completely — slot used by tx 1 will be overwritten
        for tx_id in 2..=4 {
            idx.insert_tx(tx_id);
            idx.insert_entry(tx_id, tx_id * 10, 1, EntryKind::Credit, 1);
        }
        // one more entry overwrites circle2 slot 0 (where tx 1 was)
        idx.insert_tx(5);
        idx.insert_entry(5, 50, 1, EntryKind::Credit, 1);

        // circle1 slot for tx 1 is still valid, but circle2 slot is overwritten
        assert!(idx.get_transaction(1).is_none());
    }

    // ── get_account_history ───────────────────────────────────────────────────

    #[test]
    fn account_history_single_entry() {
        let mut idx = small();
        idx.insert_tx(10);
        idx.insert_entry(10, 42, 750, EntryKind::Credit, 750);

        let hist = idx.get_account_history(42, 0, 10);
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].tx_id, 10);
    }

    #[test]
    fn account_history_chain_newest_first() {
        let mut idx = small();
        for tx_id in 1..=5u64 {
            idx.insert_tx(tx_id);
            idx.insert_entry(
                tx_id,
                999,
                tx_id * 100,
                EntryKind::Credit,
                (tx_id * 100) as i64,
            );
        }

        let hist = idx.get_account_history(999, 0, 10);
        assert_eq!(hist.len(), 5);
        // newest first — tx_id 5 should come first
        assert_eq!(hist[0].tx_id, 5);
        assert_eq!(hist[4].tx_id, 1);
    }

    #[test]
    fn account_history_limit_respected() {
        let mut idx = small();
        for tx_id in 1..=8u64 {
            idx.insert_tx(tx_id);
            idx.insert_entry(tx_id, 777, 1, EntryKind::Credit, 1);
        }

        let hist = idx.get_account_history(777, 0, 3);
        assert_eq!(hist.len(), 3);
    }

    #[test]
    fn account_history_miss_on_head_collision() {
        let mut idx = small(); // account_heads_size = 16 → mask = 15
        // account 1 and account 17 map to the same head slot
        idx.insert_tx(1);
        idx.insert_entry(1, 1, 100, EntryKind::Credit, 100);

        idx.insert_tx(2);
        idx.insert_entry(2, 17, 200, EntryKind::Credit, 200);

        // account 1's head has been overwritten by account 17
        let hist = idx.get_account_history(1, 0, 10);
        assert!(hist.is_empty(), "expected miss after head collision");

        // account 17 is still reachable
        let hist17 = idx.get_account_history(17, 0, 10);
        assert_eq!(hist17.len(), 1);
    }

    #[test]
    fn account_history_from_tx_id_lower_bound() {
        let mut idx = small();
        for tx_id in 1..=6u64 {
            idx.insert_tx(tx_id);
            idx.insert_entry(tx_id, 55, 1, EntryKind::Credit, 1);
        }

        // from_tx_id = 3 → collect only tx_ids >= 3 (newest-first: 6,5,4,3)
        let hist = idx.get_account_history(55, 3, 10);
        assert_eq!(hist.len(), 4);
        assert!(hist.iter().all(|e| e.tx_id >= 3));
        assert_eq!(hist[0].tx_id, 6);
        assert_eq!(hist[3].tx_id, 3);
    }

    #[test]
    fn account_history_chain_broken_on_circle2_eviction() {
        // circle2_size = 8, account_heads_size = 16
        let mut idx = TransactionIndexer::new(16, 8, 16);

        // write 5 entries for account 10 — they occupy circle2 slots 0..4
        for tx_id in 1..=5u64 {
            idx.insert_tx(tx_id);
            idx.insert_entry(tx_id, 10, 1, EntryKind::Credit, 1);
        }

        // write 7 more entries for a different account — fills slots 5,6,7 and
        // then wraps back to overwrite slots 0..3 (account 10's earlier entries),
        // but leaves slot 4 (account 10's head entry) intact so the chain
        // starts valid but breaks on the first prev_link follow.
        for tx_id in 6..=12u64 {
            idx.insert_tx(tx_id);
            idx.insert_entry(tx_id, 20, 1, EntryKind::Credit, 1);
        }

        // account 10's head (slot 4) is intact; following prev_link leads to
        // slot 3 which has been overwritten by account 20
        let hist = idx.get_account_history(10, 0, 100);
        // we get at least the most recent entry before the chain breaks
        assert!(!hist.is_empty());
        assert!(
            hist.len() < 5,
            "chain must break before all 5 entries are returned"
        );
    }

    // ── new() panics on non-power-of-two ─────────────────────────────────────

    #[test]
    #[should_panic(expected = "circle1_size must be a power of two")]
    fn new_panics_on_non_power_of_two_circle1() {
        TransactionIndexer::new(15, 16, 16);
    }

    #[test]
    #[should_panic(expected = "circle2_size must be a power of two")]
    fn new_panics_on_non_power_of_two_circle2() {
        TransactionIndexer::new(16, 30, 16);
    }

    #[test]
    #[should_panic(expected = "account_heads_size must be a power of two")]
    fn new_panics_on_non_power_of_two_account_heads() {
        TransactionIndexer::new(16, 16, 10);
    }
}
