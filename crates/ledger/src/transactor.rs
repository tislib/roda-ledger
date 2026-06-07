use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::dedup::{DedupCache, DedupResult};
use crate::pipeline::TransactorContext;
use crate::transaction::{Operation, Transaction, TransactionInput};
use crate::tx_ring::writer::TxRingWriter;
use crate::wait_strategy::WaitStrategy;
use crate::wasm_runtime::{WasmRuntime, WasmRuntimeEngine};
use crossbeam_skiplist::SkipMap;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;
use storage::entities::{
    EntryKind, FailReason, FunctionRegistered, SYSTEM_ACCOUNT_ID, TxEntry, TxLink, TxLinkKind,
    TxMetadata, WalEntry, WalEntryKind,
};
use storage::entries::wal_tx_term_entry;
use storage::wal_serializer::serialize_wal_records;
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
/// with the current `tx_id`; subsequent `credit` / `debit` / `begin` /
/// `emit_duplicate` calls stamp that id onto the WAL records they emit.
/// The WASM host functions read the same state without ever carrying a
/// `tx_id` across the wasmtime boundary.
///
/// Fields:
/// - `balances` — mutable account balances vector indexed by `account_id`.
/// - `fail_reason` — current transaction's failure flag (`NONE` = ok).
/// - `tx_start_index` — `ring_index` of the tx's first sub-item; `verify`/
///   `rollback`/`finalize` scope to `[tx_start_index, cursor)`.
/// - `running_crc` / `pending_items` — CRC32C and follower count folded as each
///   sub-item is pushed, then sealed into the trailing `TxMetadata` and reset.
/// - `pending_tag` / `pending_user_ref` / `pending_timestamp` — header fields
///   captured by [`Self::begin`], stamped onto the metadata at finalize.
/// - `tx_id` — current transaction id, set by [`init`].
pub struct TransactorState {
    pub balances: Vec<Balance>,
    pub fail_reason: FailReason,
    tx_start_index: usize,
    running_crc: u32,
    pending_items: usize,
    pending_tag: [u8; 8],
    pending_user_ref: u64,
    pending_timestamp: u64,
    pub tx_id: u64,
    pub tx_ring_pusher: TxRingWriter,
    wait_strategy: WaitStrategy,
    ring_retry_count: u64,
    sum: i128,
}

impl TransactorState {
    pub fn new(
        max_accounts: usize,
        tx_ring_pusher: TxRingWriter,
        wait_strategy: WaitStrategy,
    ) -> Self {
        let mut balances = Vec::with_capacity(max_accounts);
        balances.resize(max_accounts, Balance::default());
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
        }
    }

    pub fn from_balances(
        balances: Vec<Balance>,
        _entries_capacity: usize,
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
        }
    }

    pub fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                *slot = *balance;
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
    /// sub-item lands, and zero the CRC/count accumulators. Pushes nothing — the
    /// `TxMetadata` is materialized last by `finalize_committed`/`finalize_failed`.
    #[inline]
    pub fn begin(&mut self, tag: [u8; 8], user_ref: u64, timestamp: u64) {
        self.pending_tag = tag;
        self.pending_user_ref = user_ref;
        self.pending_timestamp = timestamp;
        self.tx_start_index = self.tx_ring_pusher.cursor();
        self.running_crc = 0;
        self.pending_items = 0;
        self.sum = 0;
    }

    pub fn push_entry(&mut self, entry: WalEntry) -> usize {
        self.tx_ring_pusher.push(entry)
    }

    /// Push a sub-item and fold it into the running CRC32C / follower count that
    /// gets sealed into the trailing `TxMetadata`.
    fn push_follower(&mut self, entry: WalEntry) -> usize {
        self.running_crc = crc32c::crc32c_append(self.running_crc, serialize_wal_records(&entry));
        self.pending_items += 1;
        self.push_entry(entry)
    }

    // Make room by re-granting freed space WITHOUT committing, so the in-flight
    // tx stays uncommitted for verify/finalize; back off until space frees.
    // Returns false only on shutdown (ctx stopped while still waiting).
    fn ensure_capacity(&mut self, ctx: &TransactorContext, capacity: usize) -> bool {
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

        if self.sum != 0 {
            self.fail_reason = FailReason::ZERO_SUM_VIOLATION;
        }
        self.fail_reason
    }

    pub fn rollback(&mut self) {
        let end = self.tx_ring_pusher.cursor();
        self.tx_ring_pusher.walk(self.tx_start_index, end, |entry| {
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
        });
        // No leading metadata to keep: discard all sub-items and reset the
        // accumulators. `finalize_failed` then writes a lone trailing metadata.
        self.tx_ring_pusher.rollback_to(self.tx_start_index);
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

        // Trailer order: link first, then its metadata; CRC = link ++ zeroed meta.
        let digest = crc32c::crc32c(bytemuck::bytes_of(&link));
        let digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&meta));
        meta.crc32c = digest;

        self.push_entry(WalEntry::Link(link));
        self.push_entry(WalEntry::Metadata(meta));
    }

    /// Reset per-step state. Balances persist across steps; the per-transaction
    /// start marker and CRC/count accumulators are (re)initialized by [`init`],
    /// so nothing ring-related needs resetting here.
    pub fn reset_step(&mut self) {
        self.fail_reason = FailReason::NONE;
    }

    // A rejected tx is a lone trailing metadata: zero sub-items, the reason, and a
    // CRC over the zeroed-crc metadata. (rollback already discarded any followers.)
    fn finalize_failed(&mut self, fail_reason: FailReason) {
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
        self.push_entry(WalEntry::Metadata(meta));
    }

    // Materialize the committed tx's trailing metadata from the accumulated count
    // and CRC, push it as the closing record, and reset. Returns the sub-item count.
    fn finalize_committed(&mut self) -> usize {
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
        self.push_entry(WalEntry::Metadata(meta));

        let count = self.pending_items;
        self.running_crc = 0;
        self.pending_items = 0;
        count
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
            let computed_balance = *balance;
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
            let computed_balance = *balance;
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
    ring_writer: Option<TxRingWriter>,
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
    pub fn new(
        config: &LedgerConfig,
        wasm_runtime: Arc<WasmRuntime>,
        ring_writer: TxRingWriter,
    ) -> Self {
        let mut accounts = Vec::with_capacity(config.max_accounts);
        accounts.resize(config.max_accounts, Balance::default());
        Self {
            rejected_transactions: Arc::new(Default::default()),
            balances: accounts,
            dedup: DedupCache::new(config.storage.transaction_count_per_segment),
            wasm_runtime,
            ring_writer: Some(ring_writer),
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

    /// Recovery hook — replay a `FunctionRegistered` WAL record by
    /// either reloading the binary from disk and inserting it into the
    /// runtime's registry, or unloading the handler. Mirrors the
    /// pattern of [`DedupCache::recover_entry`] but for the WASM
    /// runtime. Called from [`crate::recover::Recover::recover_until`]
    /// on every `WalEntry::FunctionRegistered` encountered during
    /// replay.
    pub(crate) fn recover_function_registered(
        &self,
        record: &FunctionRegistered,
    ) -> std::io::Result<()> {
        let name = record.name_str();
        if record.is_unregister() {
            self.wasm_runtime.recover_unregister(name)
        } else {
            self.wasm_runtime
                .recover_register(name, record.version, record.crc32c)
        }
    }

    pub fn start(&mut self, ctx: TransactorContext) -> std::io::Result<JoinHandle<()>> {
        let cap = ctx.input_capacity();
        let balances = std::mem::take(&mut self.balances);
        let rejected_transactions = self.rejected_transactions.clone();
        let dedup = std::mem::replace(&mut self.dedup, DedupCache::new(0));
        let wasm_runtime = self.wasm_runtime.clone();
        let expected_next_id = ctx.get_processed_index() + 1;
        let ring_writer = self.ring_writer.take().unwrap();
        // `Rc<RefCell<>>` and `WasmRuntimeEngine` are `!Send`; build them
        // inside the spawned thread so nothing non-`Send` ever crosses the
        // thread boundary.
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || {
                let state = Rc::new(RefCell::new(TransactorState::from_balances(
                    balances,
                    cap,
                    ring_writer,
                    ctx.wait_strategy(),
                )));
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
    pub fn new(
        max_accounts: usize,
        wasm_runtime: Arc<WasmRuntime>,
        ring_writer: TxRingWriter,
    ) -> Self {
        let state = Rc::new(RefCell::new(TransactorState::new(
            max_accounts,
            ring_writer,
            WaitStrategy::default(),
        )));
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
    pub fn process_direct_batch(
        &mut self,
        ctx: &TransactorContext,
        txs: impl IntoIterator<Item = Transaction>,
    ) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        for tx in txs {
            self.transaction_buffer.push(tx);
        }
        self.process(ctx, timestamp);
        self.reset();
    }

    /// Process a single transaction directly, bypassing inbound/outbound queues.
    pub fn process_direct(&mut self, ctx: &TransactorContext, tx: Transaction) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.transaction_buffer.push(tx);
        self.process(ctx, timestamp);
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

        let max_tx_id = self.process(ctx, timestamp);

        // Publish the whole step's records to readers in one commit. The slots
        // were granted at the head of process().
        self.state.borrow_mut().tx_ring_pusher.commit();

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

        // Apply balances under a single mut-borrow of state. tx_id is
        // now carried only on TxMetadata; follower records inherit it
        // from the most recent metadata in the stream.
        {
            let mut state = self.state.borrow_mut();
            for entry in &entries {
                match entry {
                    WalEntry::Metadata(m) if m.tx_id > max_tx_id => {
                        max_tx_id = m.tx_id;
                    }
                    WalEntry::Entry(e) => {
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

        // Mirror function-registry mutations onto our WasmRuntime so a
        // follower can serve `Operation::Function` calls against a
        // freshly-replicated handler. NOTE(ADR-015 follow-up): the
        // binary itself is NOT replicated through AppendEntries today,
        // so a follower's `recover_register` will fail with NotFound on
        // `read_function`. That's a pre-existing latent bug — flagged
        // here so a future change can ship the binary alongside the
        // record. Errors are logged and continued through; the rest of
        // the batch is still applied so balances stay in sync.
        let runtime = self.wasm_engine.runtime();
        for entry in &entries {
            if let WalEntry::FunctionRegistered(f) = entry {
                let name = f.name_str();
                let result = if f.is_unregister() {
                    runtime.recover_unregister(name)
                } else {
                    runtime.recover_register(name, f.version, f.crc32c)
                };
                if let Err(e) = result {
                    spdlog::warn!(
                        "follower: replicated FunctionRegistered({} v{}) failed to apply: {} \
                         (binary replication pending, see ADR-015)",
                        name,
                        f.version,
                        e
                    );
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

        // Forward the batch to readers via the ring. Replicated records are
        // complete (no verify/finalize), so publish each as it lands — a backed-up
        // reader can then drain and free space for the rest of the batch.
        {
            let mut s = self.state.borrow_mut();
            for entry in &entries {
                if !s.ensure_capacity(ctx, 1) {
                    return;
                }
                s.push_entry(*entry);
                s.tx_ring_pusher.commit();
            }
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
    fn process(&mut self, ctx: &TransactorContext, timestamp: u64) -> u64 {
        let mut max_tx_id = 0;
        for idx in 0..self.transaction_buffer.len() {
            let tx_id = self.transaction_buffer[idx].id;
            max_tx_id = max_tx_id.max(tx_id);
            let user_ref = self.transaction_buffer[idx].operation.user_ref();

            // Pin tx_id onto TransactorState (read by built-in ops and WASM host
            // imports), then publish the previous tx and re-grant ring space.
            self.state.borrow_mut().init(tx_id);
            self.state.borrow_mut().tx_ring_pusher.reserve();

            // --- Deduplication check ---
            match self.dedup.check(user_ref, tx_id) {
                DedupResult::Duplicate(original_tx_id) => {
                    let mut s = self.state.borrow_mut();
                    if !s.ensure_capacity(ctx, 2) {
                        return max_tx_id;
                    }
                    s.emit_duplicate(user_ref, timestamp, original_tx_id);
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
                    if !s.ensure_capacity(ctx, 3) {
                        return max_tx_id;
                    }
                    s.begin(*b"DEPOSIT\0", user_ref, timestamp);
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
                    if !s.ensure_capacity(ctx, 3) {
                        return max_tx_id;
                    }
                    s.begin(*b"WITHDRAW", user_ref, timestamp);
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
                    if !s.ensure_capacity(ctx, 3) {
                        return max_tx_id;
                    }
                    s.begin(*b"TRANSFER", user_ref, timestamp);
                    if from == to {
                        // no-op
                    } else if s.get_balance(from) < amount as i64 {
                        s.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        s.credit(from, amount);
                        s.debit(to, amount);
                    }
                }
                Operation::FunctionRegistration {
                    name,
                    binary,
                    override_existing,
                    user_ref,
                } => {
                    if !self.state.borrow_mut().ensure_capacity(ctx, 2) {
                        return max_tx_id;
                    }
                    // Record the metadata header (materialized last, as the trailer).
                    self.state
                        .borrow_mut()
                        .begin(*b"FNREG\0\0\0", user_ref, timestamp);

                    // Run the registry mutation against WasmRuntime.
                    let is_unregister = binary.is_empty();
                    let outcome = if is_unregister {
                        self.wasm_engine
                            .runtime()
                            .unregister(&name)
                            .map(|v| (v, 0u32))
                    } else {
                        self.wasm_engine
                            .runtime()
                            .register(&name, &binary, override_existing)
                    };

                    // On success push the FunctionRegistered sub-item (folded into the
                    // trailer's count+CRC); on failure flip the flag so rollback drops it.
                    match outcome {
                        Ok((version, crc)) => {
                            let record = FunctionRegistered::new(&name, version, crc);
                            self.state
                                .borrow_mut()
                                .push_follower(WalEntry::FunctionRegistered(record));
                        }
                        Err(_) => {
                            self.state.borrow_mut().fail(FailReason::INVALID_OPERATION);
                        }
                    }
                }
                Operation::NewTerm {
                    term,
                    node_id,
                    node_count,
                    node_voted,
                } => {
                    // Internal cluster op: push the TxTerm as the single sub-item;
                    // the trailing metadata (sub_item_count=1) seals it at finalize.
                    let record = wal_tx_term_entry(term, node_id, node_count, node_voted);
                    let mut s = self.state.borrow_mut();
                    if !s.ensure_capacity(ctx, 2) {
                        return max_tx_id;
                    }
                    s.begin(*b"NEWTERM\0", 0, timestamp);
                    s.push_follower(WalEntry::Term(record));
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
                            // Not registered: record the header and flip the fail
                            // flag; the bookkeeping below writes the lone fail-metadata.
                            let mut s = self.state.borrow_mut();
                            if !s.ensure_capacity(ctx, 2) {
                                return max_tx_id;
                            }
                            s.begin(build_wasm_tag(0), user_ref, timestamp);
                            s.fail(FailReason::INVALID_OPERATION);
                        }
                        Some(caller) => {
                            // Record the tagged header (drop the borrow before WASM
                            // runs so host imports can borrow_mut the same state).
                            self.state.borrow_mut().begin(
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
                            if !s.ensure_capacity(ctx, 256) {
                                return max_tx_id;
                            }
                            if status != 0 {
                                s.fail(FailReason::from_u8(status));
                            } else {
                                // sub_item_count is u16: reject WASM txs with more than
                                // u16::MAX sub-items so the trailer count fits losslessly.
                                if s.pending_items > u16::MAX as usize {
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

            if fail_reason.is_failure() {
                s.rollback();
                s.finalize_failed(fail_reason);

                drop(s);
                self.rejected_transactions.insert(tx_id, fail_reason);
                self.dedup.insert(user_ref, tx_id);
            } else {
                s.finalize_committed();

                drop(s);
                self.dedup.insert(user_ref, tx_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_ring::reader::TxRingReader;
    use crate::tx_ring::ring::TxRing;

    // A TransactorState wired to a fresh ring with `cap` slots already granted,
    // so the per-step ops can push straight into the ring's pending window. The
    // returned reader inspects what the writer (inside the state) published.
    fn fixture(max_accounts: usize, cap: usize) -> (TxRingReader, TransactorState) {
        let (mut writer, reader) = TxRing::new(cap);
        writer.reserve();
        (
            reader,
            TransactorState::new(max_accounts, writer, WaitStrategy::LowLatency),
        )
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
        // trailer layout: begin() pushes nothing; the two entries land at
        // ring_index 0 and 1 — the metadata is materialized later by finalize.
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
        s.debit(3, 100); // ring_index 0
        s.credit(5, 100); // ring_index 1
        assert_eq!(s.tx_ring_pusher.cursor(), 2);

        s.rollback();
        assert_eq!(s.get_balance(3), 0);
        assert_eq!(s.get_balance(5), 0);
        // trailer layout: there is no leading metadata, so rollback discards
        // ALL sub-items back to the transaction start.
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
        // trailer layout: the debit entry is the first record, at slot 0
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
        // A 4-slot ring: fill the grant, publish + release, then ensure_capacity
        // must reclaim the freed slots so the next op can push again.
        let (mut writer, mut reader) = TxRing::new(4);
        writer.reserve();
        let mut s = TransactorState::new(8, writer, WaitStrategy::LowLatency);
        s.init(1);
        s.begin(*b"TEST\0\0\0\0", 1, 0); // pushes nothing in the trailer layout
        s.debit(1, 10); // ring_index 0
        s.credit(2, 10); // ring_index 1
        s.debit(3, 5); // ring_index 2
        s.credit(6, 5); // ring_index 3 — fills the 4-slot grant
        assert_eq!(s.tx_ring_pusher.capacity(), 0);

        s.tx_ring_pusher.commit();
        reader.release_to(4); // reader caught up; 4 slots are free again

        // capacity() == 0, so ensure_capacity must reclaim the freed slots.
        let ctx = crate::test_support::mock_transactor_ctx();
        assert!(s.ensure_capacity(&ctx, 1));
        assert!(s.tx_ring_pusher.capacity() >= 1);
        s.debit(4, 7); // ring_index 4 reuses slot 0
        assert_eq!(s.tx_ring_pusher.cursor(), 5);
        assert_eq!(s.get_balance(4), 7);
    }

    #[test]
    fn each_tx_finalizes_at_its_own_ring_index() {
        // begin() records the transaction's start in `tx_start_index`; a
        // reserve() between transactions publishes the prior one. The trailing
        // metadata is materialized last by finalize_committed().
        let (mut writer, reader) = TxRing::new(64);
        writer.reserve();
        let mut s = TransactorState::new(16, writer, WaitStrategy::LowLatency);

        // tx1 — entries at 0,1; trailing metadata at ring_index 2.
        s.init(1);
        s.begin(*b"TX1\0\0\0\0\0", 0, 0);
        s.debit(1, 10);
        s.credit(2, 10);
        assert_eq!(s.tx_start_index, 0);
        assert_eq!(s.finalize_committed(), 2);

        // reserve() publishes tx1; tx2's entries land at 3,4 and its metadata at 5.
        s.tx_ring_pusher.reserve();
        s.init(2);
        s.begin(*b"TX2\0\0\0\0\0", 0, 0);
        s.debit(3, 5);
        s.credit(4, 5);
        assert_eq!(s.tx_start_index, 3);
        assert_eq!(s.finalize_committed(), 2);
        s.tx_ring_pusher.commit();

        assert_eq!(reader.write_index(), 6);
        // the trailing metadata of each tx sits at the end of its group
        for meta_idx in [2usize, 5usize] {
            match reader.get(meta_idx) {
                WalEntry::Metadata(m) => assert_eq!(m.sub_item_count, 2),
                _ => panic!("expected metadata at ring_index {meta_idx}"),
            }
        }
    }
}
