use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::dedup::{DedupCache, DedupResult};
use storage::entities::{
    EntryKind, FailReason, SYSTEM_ACCOUNT_ID, TxEntry, TxLink, TxLinkKind, TxMetadata, WalEntry,
    WalEntryKind, WalInput,
};
use crate::pipeline::TransactorContext;
use crate::transaction::{Operation, Transaction, TransactionInput};
use crate::wasm_runtime::{WasmRuntime, WasmRuntimeEngine};
use crossbeam_skiplist::SkipMap;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hint::spin_loop;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;

// ─────────────────────────────────────────────────────────────────────────────
// TransactorState — shared inter-state buffer
// ─────────────────────────────────────────────────────────────────────────────

/// Per-step computation state shared between the Transactor's built-in
/// operation handlers and `WasmRuntimeEngine`'s host imports for
/// `Operation::Function`.
///
/// Wrapped in `Rc<RefCell<>>` and held by **both** [`TransactorRunner`]
/// (for direct mutation by `Transfer`/`Deposit`/etc.) and
/// [`WasmRuntimeEngine`] (whose `ledger.{credit,debit,get_balance}` host
/// functions call back into the same state). Single-threaded transactor
/// loop → single borrow at any time → `RefCell` is sound.
///
/// **Stateful across one transaction.** [`init`] is called once per tx
/// with the current `tx_id`; subsequent `credit` / `debit` / `meta` /
/// `emit_duplicate` calls stamp that id onto the WAL records they emit.
/// The WASM host functions read the same state without ever carrying a
/// `tx_id` across the wasmtime boundary.
///
/// Fields:
/// - `balances` — mutable account balances vector indexed by `account_id`.
/// - `entries`  — accumulating WAL entry buffer for the current step.
/// - `fail_reason` — current transaction's failure flag (`NONE` = ok).
/// - `position` — index into `entries` of the current `TxMetadata`; used
///   by `verify()` and `rollback()` to scope their iteration to the
///   entries belonging to the in-flight transaction.
/// - `tx_id` — current transaction id, set by [`init`].
pub struct TransactorState {
    pub balances: Vec<Balance>,
    pub entries: Vec<WalEntry>,
    pub fail_reason: FailReason,
    pub position: usize,
    pub tx_id: u64,
}

impl TransactorState {
    pub fn new(max_accounts: usize) -> Self {
        let mut balances = Vec::with_capacity(max_accounts);
        balances.resize(max_accounts, Balance::default());
        Self {
            balances,
            entries: Vec::with_capacity(16),
            fail_reason: FailReason::NONE,
            position: 0,
            tx_id: 0,
        }
    }

    pub fn from_balances(balances: Vec<Balance>, entries_capacity: usize) -> Self {
        Self {
            balances,
            entries: Vec::with_capacity(entries_capacity),
            fail_reason: FailReason::NONE,
            position: 0,
            tx_id: 0,
        }
    }

    pub fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                *slot = *balance;
            }
        }
    }

    /// Begin a new transaction: pin the current `tx_id` and clear the
    /// per-transaction `fail_reason`. Every subsequent [`Self::credit`] /
    /// [`Self::debit`] / [`Self::meta`] / [`Self::emit_duplicate`] call
    /// uses this id until the next `init()`.
    #[inline]
    pub fn init(&mut self, tx_id: u64) {
        self.tx_id = tx_id;
        self.fail_reason = FailReason::NONE;
    }

    #[inline]
    pub fn meta(&mut self, tag: [u8; 8], user_ref: u64, timestamp: u64) {
        self.entries.push(WalEntry::Metadata(TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 0,
            link_count: 0,
            fail_reason: FailReason::NONE,
            crc32c: 0,
            tx_id: self.tx_id,
            timestamp,
            user_ref,
            tag,
        }));
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

    /// Current status code: `0` = success, otherwise the `FailReason`
    /// numeric value. Exposed as a plain `u8` so callers that want just
    /// the opaque status (e.g. the WASM execution path) don't need to
    /// reach for `FailReason` internals.
    #[inline]
    pub fn status(&self) -> u8 {
        self.fail_reason.as_u8()
    }

    pub fn verify(&mut self) -> FailReason {
        if self.fail_reason.is_failure() {
            return self.fail_reason;
        }

        let mut sum_credits: u128 = 0;
        let mut sum_debits: u128 = 0;
        for entry in self.entries.iter().skip(self.position + 1) {
            if let WalEntry::Entry(e) = entry {
                match e.kind {
                    EntryKind::Credit => sum_credits += e.amount as u128,
                    EntryKind::Debit => sum_debits += e.amount as u128,
                }
            }
        }
        if sum_credits != sum_debits {
            self.fail_reason = FailReason::ZERO_SUM_VIOLATION;
        }
        self.fail_reason
    }

    pub fn rollback(&mut self) {
        for entry in self.entries.iter().skip(self.position + 1) {
            if let WalEntry::Entry(e) = entry
                && let Some(balance) = self.balances.get_mut(e.account_id as usize)
            {
                match e.kind {
                    EntryKind::Credit => {
                        *balance = balance.saturating_add(e.amount as i64);
                    }
                    EntryKind::Debit => {
                        *balance = balance.saturating_sub(e.amount as i64);
                    }
                }
            }
        }
        self.entries.truncate(self.position + 1);
    }

    pub fn emit_duplicate(&mut self, user_ref: u64, timestamp: u64, original_tx_id: u64) {
        let link = TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            tx_id: self.tx_id,
            to_tx_id: original_tx_id,
            _pad2: [0; 16],
        };

        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 0,
            link_count: 1,
            fail_reason: FailReason::DUPLICATE,
            crc32c: 0,
            tx_id: self.tx_id,
            timestamp,
            user_ref,
            tag: *b"DUPLICAT",
        };

        meta.crc32c = 0;
        let digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
        let digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&link));
        meta.crc32c = digest;

        self.entries.push(WalEntry::Metadata(meta));
        self.entries.push(WalEntry::Link(link));
        self.position += 2;
    }

    /// Reset per-step state. Balances persist across steps.
    pub fn reset_step(&mut self) {
        self.entries.clear();
        self.fail_reason = FailReason::NONE;
        self.position = 0;
    }
}

// ── Host-callable surface (also used by built-in ops directly) ─────────────

impl TransactorState {
    #[inline]
    pub fn credit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_sub(amount as i64);
            self.entries.push(WalEntry::Entry(TxEntry {
                entry_type: WalEntryKind::TxEntry as u8,
                tx_id: self.tx_id,
                account_id,
                amount,
                kind: EntryKind::Credit,
                _pad0: [0; 6],
                computed_balance: *balance,
            }));
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    pub fn debit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_add(amount as i64);
            self.entries.push(WalEntry::Entry(TxEntry {
                entry_type: WalEntryKind::TxEntry as u8,
                tx_id: self.tx_id,
                account_id,
                amount,
                kind: EntryKind::Debit,
                _pad0: [0; 6],
                computed_balance: *balance,
            }));
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.balances.get(account_id as usize).copied().unwrap_or(0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Transactor / TransactorRunner
// ─────────────────────────────────────────────────────────────────────────────
//
// `wasm_runtime::WasmRuntimeEngine` is hard-wired to `TransactorState`
// (no trait, no generic). Its host imports call `state.borrow_mut()
// .credit/.debit/.get_balance` directly — same methods the built-in
// operation handlers use, so the two paths cannot diverge in semantics.

pub struct Transactor {
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: Vec<Balance>,
    dedup: DedupCache,
    wasm_runtime: Arc<WasmRuntime>,
}

pub struct TransactorRunner {
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    transaction_buffer: Vec<Transaction>,
    expected_next_id: u64,
    pending: BTreeMap<u64, Transaction>,
    input_retry_count: u64,
    dedup: DedupCache,
    state: Rc<RefCell<TransactorState>>,
    wasm_engine: WasmRuntimeEngine,
}

impl Transactor {
    pub fn new(config: &LedgerConfig, wasm_runtime: Arc<WasmRuntime>) -> Self {
        let mut accounts = Vec::with_capacity(config.max_accounts);
        accounts.resize(config.max_accounts, Balance::default());
        Self {
            rejected_transactions: Arc::new(Default::default()),
            balances: accounts,
            dedup: DedupCache::new(config.storage.transaction_count_per_segment),
            wasm_runtime,
        }
    }

    pub(crate) fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                *slot = *balance;
            }
        }
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<FailReason> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| *entry.value())
    }

    pub fn get_rejected_count(&self) -> u64 {
        self.rejected_transactions.len() as u64
    }

    pub(crate) fn dedup_cache_mut(&mut self) -> &mut DedupCache {
        &mut self.dedup
    }

    pub fn start(&mut self, ctx: TransactorContext) -> std::io::Result<JoinHandle<()>> {
        let cap = ctx.input_capacity();
        let balances = std::mem::take(&mut self.balances);
        let rejected_transactions = self.rejected_transactions.clone();
        let dedup = std::mem::replace(&mut self.dedup, DedupCache::new(0));
        let wasm_runtime = self.wasm_runtime.clone();
        let expected_next_id = ctx.get_processed_index() + 1;
        // `Rc<RefCell<>>` and `WasmRuntimeEngine` are `!Send`; build them
        // inside the spawned thread so nothing non-`Send` ever crosses the
        // thread boundary.
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || {
                let state = Rc::new(RefCell::new(TransactorState::from_balances(balances, cap)));
                let wasm_engine = WasmRuntimeEngine::new(wasm_runtime, Rc::clone(&state));
                let mut runner = TransactorRunner {
                    rejected_transactions,
                    transaction_buffer: Vec::with_capacity(cap),
                    expected_next_id,
                    pending: BTreeMap::new(),
                    input_retry_count: 0,
                    dedup,
                    state,
                    wasm_engine,
                };
                runner.run(ctx);
            })
    }
}

impl TransactorRunner {
    /// Standalone constructor used by benches (no pipeline / no queues).
    pub fn new(max_accounts: usize, wasm_runtime: Arc<WasmRuntime>) -> Self {
        let state = Rc::new(RefCell::new(TransactorState::new(max_accounts)));
        let wasm_engine = WasmRuntimeEngine::new(wasm_runtime, Rc::clone(&state));
        Self {
            expected_next_id: 1,
            pending: BTreeMap::new(),
            rejected_transactions: Arc::new(Default::default()),
            transaction_buffer: Vec::with_capacity(1),
            input_retry_count: 0,
            dedup: DedupCache::new(0),
            state,
            wasm_engine,
        }
    }

    /// Process a batch of transactions directly, bypassing inbound/outbound queues.
    pub fn process_direct_batch(&mut self, txs: impl IntoIterator<Item = Transaction>) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        for tx in txs {
            self.transaction_buffer.push(tx);
        }
        self.process(timestamp);
        self.reset();
    }

    /// Process a single transaction directly, bypassing inbound/outbound queues.
    pub fn process_direct(&mut self, tx: Transaction) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.transaction_buffer.push(tx);
        self.process(timestamp);
        self.reset();
    }

    pub fn run(&mut self, ctx: TransactorContext) {
        while ctx.is_running() {
            self.run_step(&ctx);
        }
    }

    fn run_step(&mut self, ctx: &TransactorContext) {
        let inbound = ctx.input();
        let mut did_work = false;

        while let Some(txi) = inbound.pop() {
            match txi {
                TransactionInput::Single(tx) => {
                    if tx.id == self.expected_next_id {
                        self.transaction_buffer.push(tx);
                        self.expected_next_id += 1;
                        while let Some(buffered) = self.pending.remove(&self.expected_next_id) {
                            self.transaction_buffer.push(buffered);
                            self.expected_next_id += 1;
                        }
                    } else if tx.id > self.expected_next_id {
                        self.pending.insert(tx.id, tx);
                    }
                    if self.transaction_buffer.len() >= self.transaction_buffer.capacity() {
                        break;
                    }
                }
                TransactionInput::Batch(tbx) => {
                    if tbx.start_tx_id == self.expected_next_id {
                        for (i, op) in tbx.operations.into_iter().enumerate() {
                            let mut tx = Transaction::new(op);
                            tx.id = tbx.start_tx_id + i as u64;
                            self.transaction_buffer.push(tx);
                            self.expected_next_id += 1;
                            while let Some(buffered) = self.pending.remove(&self.expected_next_id) {
                                self.transaction_buffer.push(buffered);
                                self.expected_next_id += 1;
                            }
                        }
                    } else if tbx.start_tx_id > self.expected_next_id {
                        for (i, op) in tbx.operations.into_iter().enumerate() {
                            let mut tx = Transaction::new(op);
                            tx.id = tbx.start_tx_id + i as u64;
                            self.pending.insert(tx.id, tx);
                        }
                    }
                    if self.transaction_buffer.len() >= self.transaction_buffer.capacity() {
                        break;
                    }
                }
                TransactionInput::Replicated(entries) => {
                    // Drain any prior leader-path work first — its tx_ids
                    // are sequencer-assigned and would normally be earlier
                    // than replicated tx_ids only during a role transition,
                    // but flushing first preserves the invariant either way.
                    if !self.transaction_buffer.is_empty() {
                        self.flush_buffer(ctx);
                    }
                    self.apply_replicated_batch(entries, ctx);
                    did_work = true;
                }
            }
        }

        // Flush any leader-path work accumulated in the buffer.
        if !self.transaction_buffer.is_empty() {
            self.flush_buffer(ctx);
            did_work = true;
        }

        if !did_work {
            ctx.wait_strategy().retry(self.input_retry_count);
            self.input_retry_count += 1;
            return;
        }
        self.input_retry_count = 0;
    }

    /// Process the buffered leader-path transactions, push their
    /// emitted entries to the WAL stage as `WalInput::Single`, advance
    /// `compute_index`, and reset per-step state. Factored out so the
    /// `Replicated` arm of `run_step` can flush prior leader-path
    /// buffer before applying replicated entries.
    fn flush_buffer(&mut self, ctx: &TransactorContext) {
        if self.transaction_buffer.is_empty() {
            return;
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let max_tx_id = self.process(timestamp);

        // push all accumulated entries to outbound at the end of the step
        let output = ctx.output();
        let entries_len = self.state.borrow().entries.len();
        let mut i = 0;
        while i < entries_len {
            let entry = self.state.borrow().entries[i];
            let mut pending = WalInput::Single(entry);
            let mut retry_count = 0u64;
            loop {
                retry_count += 1;
                match output.push(pending) {
                    Ok(()) => break,
                    Err(returned) => pending = returned,
                }
                if retry_count.is_multiple_of(10_000) && !ctx.is_running() {
                    return;
                }
                spin_loop();
            }
            i += 1;
        }

        if max_tx_id > 0 {
            ctx.set_processed_index(max_tx_id);
        }

        self.transaction_buffer.clear();
        self.state.borrow_mut().reset_step();
    }

    /// Follower-path: mirror the effects of pre-validated WAL entries
    /// (already committed by the leader) onto this Transactor's
    /// `balances` and `dedup` state, then forward the same vec to the
    /// WAL stage as `WalInput::Multi`. NO validation, NO rollback, NO
    /// re-emission. The Transactor remains the sole writer of its
    /// internal state — this is what keeps a follower's state in sync
    /// with the WAL so a promotion to leader does not start from
    /// stale state.
    fn apply_replicated_batch(&mut self, entries: Vec<WalEntry>, ctx: &TransactorContext) {
        let mut max_tx_id: u64 = 0;

        // Apply balances under a single mut-borrow of state.
        {
            let mut state = self.state.borrow_mut();
            for entry in &entries {
                match entry {
                    WalEntry::Metadata(m) => {
                        if m.tx_id > max_tx_id {
                            max_tx_id = m.tx_id;
                        }
                    }
                    WalEntry::Entry(e) => {
                        if e.tx_id > max_tx_id {
                            max_tx_id = e.tx_id;
                        }
                        // The leader's authoritative post-update balance is
                        // recorded on the WAL record itself; just install
                        // it (idempotent and matches Recover's semantics).
                        if let Some(slot) = state.balances.get_mut(e.account_id as usize) {
                            *slot = e.computed_balance;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Update dedup outside the state borrow. Filter mirrors
        // `Recover` (skip DUPLICATE; skip user_ref==0).
        for entry in &entries {
            if let WalEntry::Metadata(m) = entry
                && m.user_ref != 0
                && m.fail_reason != FailReason::DUPLICATE
            {
                self.dedup.insert(m.user_ref, m.tx_id);
            }
        }

        // Forward the entire batch to the WAL stage as Multi.
        let output = ctx.output();
        let mut pending = WalInput::Multi(entries);
        let mut retry_count = 0u64;
        loop {
            match output.push(pending) {
                Ok(()) => break,
                Err(returned) => pending = returned,
            }
            retry_count += 1;
            if retry_count.is_multiple_of(10_000) && !ctx.is_running() {
                return;
            }
            spin_loop();
        }

        if max_tx_id > 0 {
            ctx.set_processed_index(max_tx_id);
            // Advance the sequencer's high-water mark too. Without this
            // a follower's sequencer stays at its boot value and, on
            // promotion to leader, the first client submit is assigned
            // a tx_id that collides with replicated WAL entries.
            ctx.bump_sequencer_to_at_least(max_tx_id);
            if max_tx_id >= self.expected_next_id {
                self.expected_next_id = max_tx_id + 1;
            }
        }
    }

    /// Process the buffered transactions, producing wal entries in the
    /// shared `state.entries`. Returns the maximum transaction id observed
    /// in this batch (0 if none).
    fn process(&mut self, timestamp: u64) -> u64 {
        let mut max_tx_id = 0;
        for idx in 0..self.transaction_buffer.len() {
            let tx_id = self.transaction_buffer[idx].id;
            max_tx_id = max_tx_id.max(tx_id);
            let user_ref = self.transaction_buffer[idx].operation.user_ref();

            // Every transaction starts with init() — pins the tx_id
            // onto TransactorState so subsequent credit/debit/meta calls
            // (including those made by WASM host imports) stamp it onto
            // the records they emit.
            self.state.borrow_mut().init(tx_id);

            // --- Deduplication check ---
            match self.dedup.check(user_ref, tx_id) {
                DedupResult::Duplicate(original_tx_id) => {
                    self.state
                        .borrow_mut()
                        .emit_duplicate(user_ref, timestamp, original_tx_id);
                    self.rejected_transactions
                        .insert(tx_id, FailReason::DUPLICATE);
                    continue;
                }
                DedupResult::Proceed => {}
            }

            // Clone the operation out so we don't hold a borrow on
            // self.transaction_buffer while mutating self.state below.
            let operation = self.transaction_buffer[idx].operation.clone();

            match operation {
                Operation::Deposit {
                    user_ref,
                    account,
                    amount,
                    ..
                } => {
                    let mut s = self.state.borrow_mut();
                    s.meta(*b"DEPOSIT\0", user_ref, timestamp);
                    s.debit(account, amount);
                    s.credit(SYSTEM_ACCOUNT_ID, amount);
                }
                Operation::Withdrawal {
                    user_ref,
                    account,
                    amount,
                    ..
                } => {
                    let mut s = self.state.borrow_mut();
                    s.meta(*b"WITHDRAW", user_ref, timestamp);
                    if s.get_balance(account) < amount as i64 {
                        s.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        s.credit(account, amount);
                        s.debit(SYSTEM_ACCOUNT_ID, amount);
                    }
                }
                Operation::Transfer {
                    user_ref,
                    from,
                    to,
                    amount,
                    ..
                } => {
                    let mut s = self.state.borrow_mut();
                    s.meta(*b"TRANSFER", user_ref, timestamp);
                    if from == to {
                        // no-op
                    } else if s.get_balance(from) < amount as i64 {
                        s.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        s.credit(from, amount);
                        s.debit(to, amount);
                    }
                }
                Operation::Function {
                    name,
                    params,
                    user_ref,
                } => {
                    // Locate the handler once. `caller()` lazily
                    // reconciles with the shared registry for this name
                    // only — no unrelated cache entries are touched.
                    match self.wasm_engine.caller(&name).cloned() {
                        None => {
                            // Not registered: emit a meta with zero-CRC
                            // tag and flip the fail flag. The shared
                            // verify/rollback bookkeeping below records
                            // the rejection and stamps the meta CRC.
                            let mut s = self.state.borrow_mut();
                            s.meta(build_wasm_tag(0), user_ref, timestamp);
                            s.fail(FailReason::INVALID_OPERATION);
                        }
                        Some(caller) => {
                            // Emit the tagged meta (drop the borrow
                            // before WASM runs so host imports can
                            // borrow_mut the same state).
                            self.state.borrow_mut().meta(
                                build_wasm_tag(caller.crc32c()),
                                user_ref,
                                timestamp,
                            );
                            // The caller carries its own `Rc`-shared
                            // handle to the engine's long-lived Store;
                            // `execute` borrows it internally. Host
                            // imports read TransactorState (including
                            // its tx_id) on every credit/debit.
                            let status = caller.execute(params);
                            let mut s = self.state.borrow_mut();
                            if status != 0 {
                                s.fail(FailReason::from_u8(status));
                            } else {
                                // TxMetadata.entry_count is u8 — a WASM
                                // function can call credit/debit more
                                // than 255 times. Reject those here so
                                // the meta's entry_count can losslessly
                                // encode the real count downstream.
                                let entry_count = s.entries.len() - s.position - 1;
                                if entry_count > u8::MAX as usize {
                                    s.fail(FailReason::ENTRY_LIMIT_EXCEEDED);
                                }
                            }
                        }
                    }
                }
            }

            // ── Verify + commit/rollback bookkeeping ──────────────────────
            let mut s = self.state.borrow_mut();
            let fail_reason = s.verify();
            let meta_idx = s.position;

            if fail_reason.is_failure() {
                s.rollback();

                if let Some(WalEntry::Metadata(m)) = s.entries.get_mut(meta_idx) {
                    m.fail_reason = fail_reason;
                    m.entry_count = 0;
                    m.link_count = 0;
                    m.crc32c = 0;
                    let digest = crc32c::crc32c(bytemuck::bytes_of(m));
                    m.crc32c = digest;
                }

                drop(s);
                self.rejected_transactions.insert(tx_id, fail_reason);
                self.dedup.insert(user_ref, tx_id);
                self.state.borrow_mut().position += 1;
            } else {
                let entry_count = s.entries.len() - meta_idx - 1;

                if let Some(WalEntry::Metadata(m)) = s.entries.get_mut(meta_idx) {
                    m.entry_count = entry_count as u8;
                    m.crc32c = 0;
                }
                let mut digest = if let Some(WalEntry::Metadata(m)) = s.entries.get(meta_idx) {
                    crc32c::crc32c(bytemuck::bytes_of(m))
                } else {
                    0
                };
                for i in (meta_idx + 1)..s.entries.len() {
                    if let WalEntry::Entry(e) = &s.entries[i] {
                        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(e));
                    }
                }
                if let Some(WalEntry::Metadata(m)) = s.entries.get_mut(meta_idx) {
                    m.crc32c = digest;
                }

                drop(s);
                self.dedup.insert(user_ref, tx_id);
                self.state.borrow_mut().position += 1 + entry_count;
            }
        }

        if max_tx_id > 0 {
            self.expected_next_id = self.expected_next_id.max(max_tx_id + 1);
        }

        max_tx_id
    }

    fn reset(&mut self) {
        self.transaction_buffer.clear();
        self.input_retry_count = 0;
        self.state.borrow_mut().reset_step();
    }
}

/// Build the 8-byte `TxMetadata.tag` for a WASM-driven transaction:
/// `[b'f', b'n', b'w', b'\n', crc[0], crc[1], crc[2], crc[3]]`. The
/// literal prefix lets `roda-ctl unpack` render it as `"fnw\n4a2f1c3d"`,
/// and the embedded CRC32C identifies the exact binary.
#[inline]
pub fn build_wasm_tag(crc32c: u32) -> [u8; 8] {
    let bytes = crc32c.to_le_bytes();
    [
        b'f', b'n', b'w', b'\n', bytes[0], bytes[1], bytes[2], bytes[3],
    ]
}
