use crate::dedup::{DedupCache, DedupResult};
use crate::ledger::WaitStrategy;
use crate::pipeline::TransactorContext;
use crate::transactor::transaction::{Operation, Transaction, TransactionInput};
use crate::transactor::wasm_runtime::{WasmRuntime, WasmRuntimeEngine};
use crate::transactor::{Computer, TransactorAccount, grow_capacity};
use crate::tx_ring::writer::TxRingWriter;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use storage::entities::{FailReason, FunctionRegistered, SYSTEM_ACCOUNT_ID, WalEntry};
use storage::entries::wal_tx_term_entry;

pub struct Runner {
    pub(super) transaction_buffer: Vec<Transaction>,
    pub(super) expected_next_id: u64,
    pub(super) pending: BTreeMap<u64, Transaction>,
    pub(super) input_retry_count: u64,
    pub(super) dedup: DedupCache,
    pub(super) state: Rc<RefCell<Computer>>,
    pub(super) wasm_engine: WasmRuntimeEngine,
}

impl Runner {
    /// Standalone constructor used by benches (no pipeline / no queues).
    pub fn new(
        max_accounts: usize,
        wasm_runtime: Arc<WasmRuntime>,
        ring_writer: TxRingWriter,
    ) -> Self {
        let state = Rc::new(RefCell::new(Computer::new(
            max_accounts,
            ring_writer,
            WaitStrategy::default(),
        )));
        let wasm_engine = WasmRuntimeEngine::new(wasm_runtime, Rc::clone(&state));
        Self {
            expected_next_id: 1,
            pending: BTreeMap::new(),
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
                            slot.balance = e.computed_balance;
                        }
                    }
                    WalEntry::AccountOpened(a) => {
                        // Replay the open onto follower cells: grow, mark OPEN,
                        // and advance the allocator high-water.
                        let end = a.begin_account_id + a.count as u64;
                        let end_usize = end as usize;
                        if end_usize > state.balances.len() {
                            let new_cap =
                                grow_capacity(state.balances.len(), state.resize_factor, end_usize);
                            state.balances.resize(new_cap, TransactorAccount::default());
                        }
                        for id in a.begin_account_id..end {
                            if let Some(acct) = state.balances.get_mut(id as usize) {
                                acct.flags = a.flags;
                            }
                        }
                        if end > state.next_account_id {
                            state.next_account_id = end;
                        }
                    }
                    WalEntry::AccountLinked(a) => {
                        state.links.insert((a.parent_id, a.type_id), a.child_id);
                    }
                    WalEntry::AccountFlagsUpdated(a) => {
                        if let Some(acct) = state.balances.get_mut(a.account_id as usize) {
                            acct.flags = a.new_flags;
                        }
                    }
                    WalEntry::Kv(kv) => {
                        state.apply_kv(kv);
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
                s.tx_ring_pusher.push(*entry);
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

            // Pin tx_id onto TransactorComputer (read by built-in ops and WASM host
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
                    if s.require_open(account) {
                        s.debit(account, amount);
                        s.credit(SYSTEM_ACCOUNT_ID, amount);
                    }
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
                    if !s.require_open(account) {
                        // fail flag set: ACCOUNT_NOT_FOUND
                    } else if s.get_balance(account) < amount as i64 {
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
                    if !s.require_open(from) || !s.require_open(to) {
                        // fail flag set: ACCOUNT_NOT_FOUND
                    } else if from == to {
                        // no-op
                    } else if s.get_balance(from) < amount as i64 {
                        s.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        s.credit(from, amount);
                        s.debit(to, amount);
                    }
                }
                Operation::OpenAccount { count, user_ref } => {
                    let mut s = self.state.borrow_mut();
                    if !s.ensure_capacity(ctx, 2) {
                        return max_tx_id;
                    }
                    s.begin(*b"OPENACC\0", user_ref, timestamp);
                    s.open_accounts(count, user_ref);
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
                            // Run the module's optional `register` export to define its
                            // constants in this same registration tx (ADR-023 §6). A
                            // nonzero status (e.g. a prohibited-call phase violation)
                            // fails the tx → rollback.
                            if !is_unregister
                                && let Some(caller) = self.wasm_engine.caller(&name).cloned()
                            {
                                let status = caller.call_register();
                                if status != 0 {
                                    self.state.borrow_mut().fail(FailReason::from_u8(status));
                                }
                            }
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
                            // imports read TransactorComputer (including
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
