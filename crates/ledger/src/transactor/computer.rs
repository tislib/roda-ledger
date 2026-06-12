//! `TransactorComputer` — the single-threaded transaction computation state
//! (ADR-022): account cells (balance + flags), the parent→bucket link map, and
//! the per-tx CRC/count accumulators. Drives credit/debit, open, linked_account,
//! verify, rollback, and the trailer finalize. Wrapped in `Rc<RefCell<>>` and
//! shared with `WasmRuntimeEngine`'s host imports.

use crate::balance::Balance;
use crate::pipeline::TransactorContext;
use crate::tx_ring::writer::TxRingWriter;
use crate::wait_strategy::WaitStrategy;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use storage::entities::{
    AccountFlagsUpdated, AccountLinked, AccountOpened, EntryKind, FailReason, TxEntry, TxLink,
    TxLinkKind, TxMetadata, WalEntry, WalEntryKind,
};
use storage::wal_serializer::serialize_wal_records;

// ── Account cell + flag lanes (ADR-022) ──────────────────────────────────────

/// Per-account status lane (flags lane 0) values. A zero `flags` word means
/// `NOT_EXISTENT`, so a freshly zeroed cell is an absent account.
pub const STATUS_NOT_EXISTENT: u8 = 0;
pub const STATUS_OPEN: u8 = 1;
pub const STATUS_PROGRAMMED: u8 = 2;
pub const STATUS_SYSTEM: u8 = 3;

/// `flags` is eight independent byte-lanes; `lane` selects one (0..8).
#[inline]
pub fn get_flag(flags: u64, lane: u8) -> u8 {
    (flags >> (lane as u32 * 8)) as u8
}
#[inline]
pub fn set_flag(flags: &mut u64, lane: u8, value: u8) {
    let shift = lane as u32 * 8;
    *flags = (*flags & !(0xFFu64 << shift)) | ((value as u64) << shift);
}
#[inline]
pub fn has_flag(flags: u64, lane: u8, value: u8) -> bool {
    get_flag(flags, lane) == value
}
/// Status = flags lane 0.
#[inline]
pub fn status(flags: u64) -> u8 {
    (flags & 0xFF) as u8
}
/// An account exists iff its status lane is non-zero.
#[inline]
pub fn is_existent(flags: u64) -> bool {
    (flags & 0xFF) != 0
}

/// Per-account cell: signed balance plus an 8-lane `flags` word (lane 0 =
/// status). `Default` = all-zero = `NOT_EXISTENT`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactorAccount {
    pub balance: Balance,
    pub flags: u64,
}

/// Geometric capacity growth (ADR-022): grow `cap` by `(1 + factor)` until it
/// reaches `needed`.
pub(crate) fn grow_capacity(cap: usize, factor: f64, needed: usize) -> usize {
    let mut cap = cap.max(1);
    while cap < needed {
        let next = ((cap as f64) * (1.0 + factor)).ceil() as usize;
        cap = next.max(cap + 1); // guard: factor <= 0 still makes progress
    }
    cap
}

/// Fresh account Vec sized to `initial` (≥ 1) with SYSTEM (id 0) bootstrapped
/// as an existent internal account. Grows on demand thereafter.
pub(crate) fn new_account_vec(initial: usize) -> Vec<TransactorAccount> {
    let cap = initial.max(1);
    let mut v = vec![TransactorAccount::default(); cap];
    set_flag(&mut v[0].flags, 0, STATUS_SYSTEM);
    v
}

// ── TransactorComputer ────────────────────────────────────────────────────────

/// Per-step computation state shared between the Transactor's built-in
/// operation handlers and `WasmRuntimeEngine`'s host imports. Single-threaded
/// (one borrow at a time → `RefCell` is sound). Stateful across one tx: `init`
/// pins the `tx_id`; subsequent credit/debit/begin stamp it onto emitted records.
pub struct TransactorComputer {
    pub balances: Vec<TransactorAccount>,
    pub fail_reason: FailReason,
    tx_start_index: usize,
    running_crc: u32,
    pub(crate) pending_items: usize,
    pending_tag: [u8; 8],
    pending_user_ref: u64,
    pending_timestamp: u64,
    pub tx_id: u64,
    pub tx_ring_pusher: TxRingWriter,
    wait_strategy: WaitStrategy,
    ring_retry_count: u64,
    sum: i128,
    pub(crate) next_account_id: u64,
    pub(crate) resize_factor: f64,
    /// Parent→bucket links (ADR-022 §3): `(parent_id, type_id) → child_id`.
    /// Point-resolve only (FxHash); the transactor never enumerates.
    pub(crate) links: FxHashMap<(u64, u16), u64>,
    /// `next_account_id` snapshot at tx start, for rolling back account creates.
    tx_start_next_account_id: u64,
}

impl TransactorComputer {
    pub fn new(
        initial_account_size: usize,
        tx_ring_pusher: TxRingWriter,
        wait_strategy: WaitStrategy,
    ) -> Self {
        Self {
            balances: new_account_vec(initial_account_size),
            fail_reason: FailReason::NONE,
            tx_start_index: 0,
            running_crc: 0,
            pending_items: 0,
            pending_tag: [0; 8],
            pending_user_ref: 0,
            pending_timestamp: 0,
            tx_id: 0,
            tx_ring_pusher,
            wait_strategy,
            ring_retry_count: 0,
            sum: 0,
            next_account_id: 1,
            resize_factor: 0.75,
            links: FxHashMap::default(),
            tx_start_next_account_id: 1,
        }
    }

    pub fn from_balances(
        balances: Vec<TransactorAccount>,
        next_account_id: u64,
        links: FxHashMap<(u64, u16), u64>,
        resize_factor: f64,
        tx_ring_pusher: TxRingWriter,
        wait_strategy: WaitStrategy,
    ) -> Self {
        Self {
            balances,
            fail_reason: FailReason::NONE,
            tx_start_index: 0,
            running_crc: 0,
            pending_items: 0,
            pending_tag: [0; 8],
            pending_user_ref: 0,
            pending_timestamp: 0,
            tx_id: 0,
            tx_ring_pusher,
            wait_strategy,
            ring_retry_count: 0,
            sum: 0,
            next_account_id,
            resize_factor,
            links,
            tx_start_next_account_id: next_account_id,
        }
    }

    pub fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                slot.balance = *balance;
            }
        }
    }

    /// Begin a new transaction: pin the `tx_id` and clear `fail_reason`.
    #[inline]
    pub fn init(&mut self, tx_id: u64) {
        self.tx_id = tx_id;
        self.fail_reason = FailReason::NONE;
    }

    /// Begin a transaction body: capture the header fields, mark where the first
    /// sub-item lands, and zero the CRC/count accumulators.
    #[inline]
    pub fn begin(&mut self, tag: [u8; 8], user_ref: u64, timestamp: u64) {
        self.pending_tag = tag;
        self.pending_user_ref = user_ref;
        self.pending_timestamp = timestamp;
        self.tx_start_index = self.tx_ring_pusher.cursor();
        self.running_crc = 0;
        self.pending_items = 0;
        self.sum = 0;
        self.tx_start_next_account_id = self.next_account_id;
    }

    /// Push a sub-item and fold it into the running CRC32C / follower count
    /// sealed into the trailing `TxMetadata`. EVERY follower goes through here,
    /// so the commit CRC covers all entry kinds — not just `TxEntry`.
    pub(crate) fn push_follower(&mut self, entry: WalEntry) -> usize {
        self.running_crc = crc32c::crc32c_append(self.running_crc, serialize_wal_records(&entry));
        self.pending_items += 1;
        self.tx_ring_pusher.push(entry)
    }

    // Make room by re-granting freed space WITHOUT committing, so the in-flight
    // tx stays uncommitted for verify/finalize; back off until space frees.
    // Returns false only on shutdown (ctx stopped while still waiting).
    pub(crate) fn ensure_capacity(&mut self, ctx: &TransactorContext, capacity: usize) -> bool {
        while self.tx_ring_pusher.capacity() < capacity {
            if self.tx_ring_pusher.grant() >= capacity {
                break;
            }
            if !ctx.is_running() {
                return false;
            }
            self.ring_retry_count += 1;
            self.wait_strategy.retry(self.ring_retry_count);
        }
        self.ring_retry_count = 0;
        true
    }

    #[inline]
    pub fn fail(&mut self, reason: FailReason) {
        self.fail_reason = reason;
    }

    /// Whether the current transaction has tripped a failure flag.
    #[inline]
    pub fn is_failed(&self) -> bool {
        self.fail_reason.is_failure()
    }

    /// Current status code: `0` = success, otherwise the `FailReason` value.
    #[inline]
    pub fn status(&self) -> u8 {
        self.fail_reason.as_u8()
    }

    pub fn verify(&mut self) -> FailReason {
        if self.fail_reason.is_failure() {
            return self.fail_reason;
        }

        if self.sum != 0 {
            self.fail_reason = FailReason::ZERO_SUM_VIOLATION;
        }
        self.fail_reason
    }

    pub fn rollback(&mut self) {
        let end = self.tx_ring_pusher.cursor();
        self.tx_ring_pusher
            .walk(self.tx_start_index, end, |entry| match entry {
                WalEntry::Entry(e) => {
                    if let Some(acct) = self.balances.get_mut(e.account_id as usize) {
                        match e.kind {
                            EntryKind::Credit => {
                                acct.balance = acct.balance.saturating_add(e.amount as i64);
                            }
                            EntryKind::Debit => {
                                acct.balance = acct.balance.saturating_sub(e.amount as i64);
                            }
                        }
                    }
                }
                // Reverse account-layout creates (ADR-022 §6): clear the cells
                // the open created and drop the link it added.
                WalEntry::AccountOpened(a) => {
                    let begin = a.begin_account_id as usize;
                    let stop =
                        ((a.begin_account_id + a.count as u64) as usize).min(self.balances.len());
                    for id in begin..stop {
                        self.balances[id].flags = 0;
                    }
                }
                WalEntry::AccountLinked(a) => {
                    self.links.remove(&(a.parent_id, a.type_id));
                }
                WalEntry::AccountFlagsUpdated(a) => {
                    if let Some(acct) = self.balances.get_mut(a.account_id as usize) {
                        acct.flags = a.prev_flags;
                    }
                }
                _ => {}
            });
        // Discard all sub-items and reset the accumulators + the allocator
        // high-water. `finalize_failed` then writes a lone trailing metadata.
        self.tx_ring_pusher.rollback_to(self.tx_start_index);
        self.next_account_id = self.tx_start_next_account_id;
        self.running_crc = 0;
        self.pending_items = 0;
    }

    pub fn emit_duplicate(&mut self, user_ref: u64, timestamp: u64, original_tx_id: u64) {
        let link = TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            _pad1: [0; 8],
            to_tx_id: original_tx_id,
            _pad2: [0; 16],
        };

        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::DUPLICATE,
            sub_item_count: 1,
            crc32c: 0,
            tx_id: self.tx_id,
            timestamp,
            user_ref,
            tag: *b"DUPLICAT",
        };

        // Trailer order: link folds into the running CRC via push_follower;
        // the metadata carries that CRC and is pushed raw — the same single
        // path as `finalize_committed`.
        self.running_crc = 0;
        self.pending_items = 0;
        self.push_follower(WalEntry::Link(link));
        meta.crc32c = crc32c::crc32c_append(self.running_crc, bytemuck::bytes_of(&meta));
        self.tx_ring_pusher.push(WalEntry::Metadata(meta));
        self.running_crc = 0;
        self.pending_items = 0;
    }

    /// Reset per-step state. Balances persist across steps; the per-transaction
    /// start marker and CRC/count accumulators are (re)initialized by [`init`].
    pub fn reset_step(&mut self) {
        self.fail_reason = FailReason::NONE;
    }

    // A rejected tx is a lone trailing metadata: zero sub-items, the reason, and a
    // CRC over the zeroed-crc metadata. (rollback already discarded any followers.)
    pub(crate) fn finalize_failed(&mut self, fail_reason: FailReason) {
        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason,
            sub_item_count: 0,
            crc32c: 0,
            tx_id: self.tx_id,
            timestamp: self.pending_timestamp,
            user_ref: self.pending_user_ref,
            tag: self.pending_tag,
        };
        meta.crc32c = crc32c::crc32c(bytemuck::bytes_of(&meta));
        self.tx_ring_pusher.push(WalEntry::Metadata(meta));
    }

    // Materialize the committed tx's trailing metadata from the accumulated count
    // and CRC, push it as the closing record, and reset. Returns the sub-item count.
    pub fn finalize_committed(&mut self) -> usize {
        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            fail_reason: FailReason::NONE,
            sub_item_count: self.pending_items as u16,
            crc32c: 0,
            tx_id: self.tx_id,
            timestamp: self.pending_timestamp,
            user_ref: self.pending_user_ref,
            tag: self.pending_tag,
        };
        // CRC = followers (already in running_crc) ++ the zeroed-crc metadata.
        meta.crc32c = crc32c::crc32c_append(self.running_crc, bytemuck::bytes_of(&meta));
        self.tx_ring_pusher.push(WalEntry::Metadata(meta));

        let count = self.pending_items;
        self.running_crc = 0;
        self.pending_items = 0;
        count
    }

    /// Allocate `count` (≥ 1) sequential accounts, mark them OPEN, and emit one
    /// `AccountOpened` follower. Returns the first id of the range.
    pub fn open_accounts(&mut self, count: u32, user_ref: u64) -> u64 {
        let n = count.max(1) as u64;
        let begin = self.next_account_id;
        // The u64 id space is effectively inexhaustible (u32::MAX accounts is
        // already >50GB); overflow is a bug, so panic rather than handle it.
        let end = begin.checked_add(n).expect("account id space exhausted");
        let end_usize = end as usize;
        if end_usize > self.balances.len() {
            let new_cap = grow_capacity(self.balances.len(), self.resize_factor, end_usize);
            self.balances.resize(new_cap, TransactorAccount::default());
        }
        let mut open_flags = 0u64;
        set_flag(&mut open_flags, 0, STATUS_OPEN);
        for id in begin..end {
            if let Some(acct) = self.balances.get_mut(id as usize) {
                acct.flags = open_flags;
            }
        }
        self.push_follower(WalEntry::AccountOpened(AccountOpened::new(
            begin,
            count.max(1),
            open_flags,
            user_ref,
        )));
        self.next_account_id = end;
        begin
    }
}

// ── Host-callable surface (also used by built-in ops directly) ──────────────

impl TransactorComputer {
    /// D9 guard: reject a *user-named* account that isn't OPEN (absent, SYSTEM,
    /// or PROGRAMMED). Sets `ACCOUNT_NOT_FOUND` and returns false on rejection.
    pub(crate) fn require_open(&mut self, account_id: u64) -> bool {
        let ok = self
            .balances
            .get(account_id as usize)
            .map(|a| has_flag(a.flags, 0, STATUS_OPEN))
            .unwrap_or(false);
        if !ok {
            self.fail_reason = FailReason::ACCOUNT_NOT_FOUND;
        }
        ok
    }

    /// Get-or-create the bucket linked to `parent` under `type_id` (ADR-022
    /// §6): returns the existing child, or allocates the next id as a
    /// `PROGRAMMED` account, links it, and emits `AccountOpened` +
    /// `AccountLinked` followers. Mutating — reversed by `rollback` if the tx
    /// later fails.
    pub fn linked_account(&mut self, parent: u64, type_id: u16) -> u64 {
        if let Some(&child) = self.links.get(&(parent, type_id)) {
            return child;
        }
        let child = self.next_account_id;
        let end = child.checked_add(1).expect("account id space exhausted");
        if end as usize > self.balances.len() {
            let new_cap = grow_capacity(self.balances.len(), self.resize_factor, end as usize);
            self.balances.resize(new_cap, TransactorAccount::default());
        }
        let mut flags = 0u64;
        set_flag(&mut flags, 0, STATUS_PROGRAMMED);
        self.balances[child as usize].flags = flags;
        self.next_account_id = end;
        self.links.insert((parent, type_id), child);
        self.push_follower(WalEntry::AccountOpened(AccountOpened::new(
            child, 1, flags, 0,
        )));
        self.push_follower(WalEntry::AccountLinked(AccountLinked::new(
            parent, type_id, child,
        )));
        child
    }

    #[inline]
    pub fn credit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        let idx = account_id as usize;
        if !self
            .balances
            .get(idx)
            .map(|a| is_existent(a.flags))
            .unwrap_or(false)
        {
            self.fail_reason = FailReason::ACCOUNT_NOT_FOUND;
            return;
        }
        let acct = &mut self.balances[idx];
        acct.balance = acct.balance.saturating_sub(amount as i64);
        let computed_balance = acct.balance;
        self.push_follower(WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            _pad1: [0; 8],
            account_id,
            amount,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            computed_balance,
        }));
        self.sum -= amount as i128;
    }

    #[inline]
    pub fn debit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        let idx = account_id as usize;
        if !self
            .balances
            .get(idx)
            .map(|a| is_existent(a.flags))
            .unwrap_or(false)
        {
            self.fail_reason = FailReason::ACCOUNT_NOT_FOUND;
            return;
        }
        let acct = &mut self.balances[idx];
        acct.balance = acct.balance.saturating_add(amount as i64);
        let computed_balance = acct.balance;
        self.push_follower(WalEntry::Entry(TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            _pad1: [0; 8],
            account_id,
            amount,
            kind: EntryKind::Debit,
            _pad0: [0; 6],
            computed_balance,
        }));
        self.sum += amount as i128;
    }

    #[inline]
    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.balances
            .get(account_id as usize)
            .map(|a| a.balance)
            .unwrap_or(0)
    }

    /// Read flag lane `lane` (0..8) for `account_id` (ADR-022 §6 WASM host).
    #[inline]
    pub fn get_account_flag(&self, account_id: u64, lane: u8) -> u8 {
        self.balances
            .get(account_id as usize)
            .map(|a| get_flag(a.flags, lane))
            .unwrap_or(0)
    }

    #[inline]
    pub fn has_account_flag(&self, account_id: u64, lane: u8, value: u8) -> bool {
        self.get_account_flag(account_id, lane) == value
    }

    /// Set flag lane `lane` to `value` for `account_id` and emit an
    /// `AccountFlagsUpdated` follower (ADR-022 §6). WASM may set any lane,
    /// including the status lane (0). No-op if the id is out of range or the
    /// value is unchanged (no record for a no-op).
    pub fn set_account_flag(&mut self, account_id: u64, lane: u8, value: u8) {
        let idx = account_id as usize;
        let prev = match self.balances.get(idx) {
            Some(a) => a.flags,
            None => return,
        };
        let mut new = prev;
        set_flag(&mut new, lane, value);
        if new == prev {
            return;
        }
        self.balances[idx].flags = new;
        self.push_follower(WalEntry::AccountFlagsUpdated(AccountFlagsUpdated::new(
            account_id, prev, new,
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_ring::reader::TxRingReader;
    use crate::tx_ring::ring::TxRing;
    use storage::entities::SYSTEM_ACCOUNT_ID;

    // A TransactorComputer wired to a fresh ring with `cap` slots already
    // granted, so the per-step ops can push straight into the pending window.
    fn fixture(max_accounts: usize, cap: usize) -> (TxRingReader, TransactorComputer) {
        let (mut writer, reader) = TxRing::new(cap);
        writer.reserve();
        let mut state = TransactorComputer::new(max_accounts, writer, WaitStrategy::LowLatency);
        // Unit tests credit/debit small account ids directly (not via OpenAccount);
        // mark the whole range OPEN so existence enforcement permits them.
        for id in 1..state.balances.len() {
            set_flag(&mut state.balances[id].flags, 0, STATUS_OPEN);
        }
        (reader, state)
    }

    #[test]
    fn credit_debit_update_balances_and_emit_entries() {
        let (_ring, mut s) = fixture(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 7, 0);
        s.debit(3, 100); // debit adds to the account
        s.credit(5, 100); // credit subtracts from the account
        assert_eq!(s.get_balance(3), 100);
        assert_eq!(s.get_balance(5), -100);
        assert_eq!(s.tx_ring_pusher.cursor(), 2);
    }

    #[test]
    fn verify_passes_when_credits_equal_debits() {
        let (_ring, mut s) = fixture(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 7, 0);
        s.debit(3, 100);
        s.credit(5, 100);
        assert_eq!(s.verify(), FailReason::NONE);
    }

    #[test]
    fn verify_flags_zero_sum_violation() {
        let (_ring, mut s) = fixture(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 7, 0);
        s.debit(3, 100);
        s.credit(5, 40); // debits != credits
        assert_eq!(s.verify(), FailReason::ZERO_SUM_VIOLATION);
    }

    #[test]
    fn rollback_reverts_balances_and_discards_sub_items() {
        let (_ring, mut s) = fixture(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 7, 0);
        s.debit(3, 100);
        s.credit(5, 100);
        assert_eq!(s.tx_ring_pusher.cursor(), 2);

        s.rollback();
        assert_eq!(s.get_balance(3), 0);
        assert_eq!(s.get_balance(5), 0);
        assert_eq!(s.tx_ring_pusher.cursor(), 0);
    }

    #[test]
    fn commit_publishes_records_to_a_reader() {
        let (reader, mut s) = fixture(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 7, 0);
        s.debit(3, 100);
        s.credit(5, 100);
        s.tx_ring_pusher.commit();

        assert_eq!(reader.write_index(), 2);
        match reader.get(0) {
            WalEntry::Entry(e) => {
                assert_eq!(e.amount, 100);
                assert!(matches!(e.kind, EntryKind::Debit));
            }
            _ => panic!("expected a TxEntry at slot 0"),
        }
    }

    #[test]
    fn ensure_capacity_reclaims_freed_slots() {
        let (mut writer, mut reader) = TxRing::new(4);
        writer.reserve();
        let mut s = TransactorComputer::new(8, writer, WaitStrategy::LowLatency);
        for id in 1..s.balances.len() {
            set_flag(&mut s.balances[id].flags, 0, STATUS_OPEN);
        }
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 1, 0);
        s.debit(1, 10);
        s.credit(2, 10);
        s.debit(3, 5);
        s.credit(6, 5); // fills the 4-slot grant
        assert_eq!(s.tx_ring_pusher.capacity(), 0);

        s.tx_ring_pusher.commit();
        reader.release_to(4);

        let ctx = crate::test_support::mock_transactor_ctx();
        assert!(s.ensure_capacity(&ctx, 1));
        assert!(s.tx_ring_pusher.capacity() >= 1);
        s.debit(4, 7);
        assert_eq!(s.tx_ring_pusher.cursor(), 5);
        assert_eq!(s.get_balance(4), 7);
    }

    #[test]
    fn each_tx_finalizes_at_its_own_ring_index() {
        let (mut writer, reader) = TxRing::new(64);
        writer.reserve();
        let mut s = TransactorComputer::new(16, writer, WaitStrategy::LowLatency);
        for id in 1..s.balances.len() {
            set_flag(&mut s.balances[id].flags, 0, STATUS_OPEN);
        }

        s.init(1);
        s.begin(*b"TX1\0\0\0\0\0", 0, 0);
        s.debit(1, 10);
        s.credit(2, 10);
        assert_eq!(s.tx_start_index, 0);
        assert_eq!(s.finalize_committed(), 2);

        s.tx_ring_pusher.reserve();
        s.init(2);
        s.begin(*b"TX2\0\0\0\0\0", 0, 0);
        s.debit(3, 5);
        s.credit(4, 5);
        assert_eq!(s.tx_start_index, 3);
        assert_eq!(s.finalize_committed(), 2);
        s.tx_ring_pusher.commit();

        assert_eq!(reader.write_index(), 6);
        for meta_idx in [2usize, 5usize] {
            match reader.get(meta_idx) {
                WalEntry::Metadata(m) => assert_eq!(m.sub_item_count, 2),
                _ => panic!("expected metadata at ring_index {meta_idx}"),
            }
        }
    }

    // ── OpenAccount / flags / existence / links (ADR-022) ───────────────────

    fn raw_state(max_accounts: usize, cap: usize) -> (TxRingReader, TransactorComputer) {
        let (mut writer, reader) = TxRing::new(cap);
        writer.reserve();
        (
            reader,
            TransactorComputer::new(max_accounts, writer, WaitStrategy::LowLatency),
        )
    }

    #[test]
    fn open_accounts_marks_range_open_and_emits_one_follower() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"OPENACC\0", 7, 0);
        let begin = s.open_accounts(3, 7);
        assert_eq!(begin, 1);
        assert_eq!(s.next_account_id, 4);
        assert_eq!(status(s.balances[0].flags), STATUS_SYSTEM);
        assert_eq!(status(s.balances[1].flags), STATUS_OPEN);
        assert_eq!(status(s.balances[3].flags), STATUS_OPEN);
        assert_eq!(status(s.balances[4].flags), STATUS_NOT_EXISTENT);
        assert_eq!(s.tx_ring_pusher.cursor(), 1);
    }

    #[test]
    fn open_accounts_count_zero_opens_one() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"OPENACC\0", 0, 0);
        assert_eq!(s.open_accounts(0, 0), 1);
        assert_eq!(s.next_account_id, 2);
        assert_eq!(status(s.balances[1].flags), STATUS_OPEN);
    }

    #[test]
    fn linked_account_creates_bucket_and_emits_followers() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"WASMFN\0\0", 0, 0);
        s.open_accounts(1, 0); // parent = 1
        let hold = s.linked_account(1, 7);
        assert_eq!(hold, 2, "bucket allocated as the next id");
        assert_eq!(s.next_account_id, 3);
        assert_eq!(status(s.balances[hold as usize].flags), STATUS_PROGRAMMED);
        // followers: AccountOpened(parent) + AccountOpened(bucket) + AccountLinked
        assert_eq!(s.tx_ring_pusher.cursor(), 3);
    }

    #[test]
    fn linked_account_get_returns_existing_without_allocating() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"WASMFN\0\0", 0, 0);
        let first = s.linked_account(5, 7);
        let again = s.linked_account(5, 7);
        assert_eq!(first, again, "second call resolves the same bucket");
        assert_eq!(s.next_account_id, first + 1, "no second allocation");
    }

    #[test]
    fn rollback_reverses_linked_account_create() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"WASMFN\0\0", 0, 0);
        let hold = s.linked_account(5, 7);
        assert_eq!(hold, 1);
        assert_eq!(s.next_account_id, 2);

        s.rollback();
        assert_eq!(s.next_account_id, 1);
        assert_eq!(status(s.balances[hold as usize].flags), STATUS_NOT_EXISTENT);

        s.begin(*b"WASMFN\0\0", 0, 0);
        assert_eq!(s.linked_account(5, 7), 1);
    }

    #[test]
    fn credit_debit_require_existence() {
        let (_r, mut s) = raw_state(16, 64);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 0, 0);
        s.debit(5, 100); // account 5 never opened
        assert_eq!(s.fail_reason, FailReason::ACCOUNT_NOT_FOUND);

        // SYSTEM (id 0) is existent, so internal credit/debit still works.
        s.fail_reason = FailReason::NONE;
        s.credit(SYSTEM_ACCOUNT_ID, 100);
        assert_eq!(s.fail_reason, FailReason::NONE);
    }
}
