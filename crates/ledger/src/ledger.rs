use crate::balance::Balance;
pub use crate::config::{LedgerConfig, StorageConfig};
pub use crate::pipeline::CommitHandler;
use crate::pipeline::Pipeline;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryKind, QueryRequest, QueryResponse, Snapshot};
use crate::transactor;
use crate::transactor::transaction::{
    AccountHistory, OpenAccountsResult, Operation, TransactionStatus, WaitLevel,
};
pub use crate::transactor::wasm_runtime::FunctionInfo;
use crate::transactor::wasm_runtime::{WasmRuntime, validate_name};
use crate::tx_ring::ring::TxRing;
pub use crate::wait_strategy::WaitStrategy;
use crate::wal::Wal;
use spdlog::{LevelFilter, debug, info};
use std::collections::HashMap;
use std::io;
use std::io::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use storage::WalTailer;
use storage::entities::{CommittedTransaction, FailReason, WalEntry};
use storage::{Segment, Storage};

pub struct Ledger {
    sequencer: Sequencer,
    snapshot: Snapshot,
    storage: Arc<Storage>,
    pipeline: Arc<Pipeline>,
    wasm_runtime: Arc<WasmRuntime>,
    handles: Vec<JoinHandle<()>>,
    #[allow(dead_code)]
    config: LedgerConfig,
    /// See [`crate::fault::LedgerFaultInjector`]. Installed on
    /// `Storage` at construction so the WAL committer's
    /// `Syncer::sync` reaches it via the propagated hook.
    #[cfg(feature = "fault-injection")]
    fault_injector: Arc<crate::fault::LedgerFaultInjector>,
}

impl Ledger {
    pub fn new(config: LedgerConfig) -> Self {
        spdlog::default_logger().set_level_filter(LevelFilter::MoreSevereEqual(config.log_level));

        debug!("Initializing Ledger with config: {:?}", config);

        let storage = Storage::new(config.storage.clone()).unwrap();
        let storage = Arc::new(storage);

        #[cfg(feature = "fault-injection")]
        let fault_injector = {
            let injector = crate::fault::LedgerFaultInjector::new();
            storage.set_fault(Some(injector.clone()));
            injector
        };

        // SPSC tx ring: writer → transactor, reader → WAL. No Arc, not held by
        // the pipeline; the WAL reads and frees slots (on ingest) via the reader.

        let pipeline = Pipeline::new(&config);
        let wasm_runtime = Arc::new(WasmRuntime::new(storage.clone()));
        let snapshot = Snapshot::new(&config, storage.clone());

        Self {
            sequencer: Sequencer::new(pipeline.sequencer_context()),
            snapshot,
            storage,
            pipeline,
            wasm_runtime,
            handles: Vec::new(),
            config,
            #[cfg(feature = "fault-injection")]
            fault_injector,
        }
    }

    /// Test-only accessor; gated by the `fault-injection` feature.
    /// See [`crate::fault::LedgerFaultInjector`] for the supported
    /// fault axes (v1: stuck `fdatasync`).
    #[cfg(feature = "fault-injection")]
    pub fn fault_injector(&self) -> Arc<crate::fault::LedgerFaultInjector> {
        self.fault_injector.clone()
    }

    pub fn submit(&self, operation: Operation) -> u64 {
        self.sequencer.submit(operation)
    }

    pub fn submit_batch(&self, operations: Vec<Operation>) -> u64 {
        self.sequencer.submit_batch(operations)
    }

    pub fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> io::Result<(u16, u32)> {
        // Pre-validate eagerly so callers see specific `io::ErrorKind`s
        // (`InvalidInput` / `InvalidData` / `AlreadyExists`) before we
        // commit a Sequencer slot. The transactor's call to
        // `wasm_runtime.register` is the authoritative validator;
        // this is a fast-path UX for the common rejection cases.
        validate_name(name)?;
        self.wasm_runtime.validate(binary)?;
        if !override_existing && self.wasm_runtime.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("function `{}` is already registered", name),
            ));
        }
        let res = self.submit_and_wait_result(Operation::FunctionRegistration {
            name: name.to_string(),
            binary: binary.to_vec(),
            override_existing,
            user_ref: 0,
        });
        if res.is_err() {
            return Err(io::Error::other(format!(
                "function registration failed: {:?}",
                res.get_fail_reason()
            )));
        }
        let version = self.wasm_runtime.version_of(name).ok_or_else(|| {
            io::Error::other("registration applied but version missing from runtime")
        })?;
        let crc = self
            .wasm_runtime
            .crc32c_of(name)
            .ok_or_else(|| io::Error::other("registration applied but crc missing from runtime"))?;
        Ok((version, crc))
    }

    pub fn unregister_function(&self, name: &str) -> io::Result<u16> {
        validate_name(name)?;
        // Capture predecessor version BEFORE submit — after the wait the
        // handler is unloaded, so `version_of` returns None and we can't
        // read the stamped version back from the registry.
        let prev = self.wasm_runtime.version_of(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("function `{}` is not registered", name),
            )
        })?;
        let next_version = prev
            .checked_add(1)
            .ok_or_else(|| io::Error::other("function version overflow (u16 exhausted)"))?;
        let res = self.submit_and_wait_result(Operation::FunctionRegistration {
            name: name.to_string(),
            binary: Vec::new(),
            override_existing: true,
            user_ref: 0,
        });
        if res.is_err() {
            return Err(io::Error::other(format!(
                "function unregistration failed: {:?}",
                res.get_fail_reason()
            )));
        }
        Ok(next_version)
    }

    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.wasm_runtime.list()
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.snapshot.get_balance(account_id)
    }

    /// Raw 8-lane flags word for `account_id` (lane 0 = status). Used by the
    /// read-side guard (ADR-022 §8) to reject internal accounts.
    pub fn get_flags(&self, account_id: u64) -> u64 {
        self.snapshot.get_flags(account_id)
    }

    /// Per-type linked-bucket balances for `account_id` (ADR-022 §8 breakdown):
    /// `(type_id, balance)` for every bucket linked under this parent.
    pub fn linked_balances(&self, account_id: u64) -> Vec<(u16, Balance)> {
        self.snapshot.linked_balances(account_id)
    }

    /// Committed transactions that reference `account_id`, newest→oldest,
    /// scanned backward from `from_tx_id` (`0` = latest) down to `to_tx_id` (the
    /// oldest tx_id to include — the scan stops below it). Reads the durable WAL
    /// via `WalScanner`, independent of the in-memory index. The returned
    /// `scan_last_tx_id` is the oldest tx the scan reached; re-query with
    /// `from_tx_id = scan_last_tx_id` to page further into the past.
    pub fn get_account_history(
        &self,
        account_id: u64,
        from_tx_id: u64,
        to_tx_id: u64,
    ) -> AccountHistory {
        let mut transactions = Vec::new();
        // Surface every transaction so the `to_tx_id` floor is exact (a tx's id
        // lives on its metadata, which the matcher's any-entry rule can't see);
        // filter by account and stop at the floor in the handler.
        let scan_last_tx_id = self.storage.wal_scanner().scan(
            from_tx_id,
            0,
            |_| true,
            |tx| {
                if tx.meta.tx_id < to_tx_id {
                    return false;
                }
                if tx
                    .entries
                    .iter()
                    .any(|e| Self::entry_references_account(e, account_id))
                {
                    transactions.push(tx);
                }
                true
            },
        );
        AccountHistory {
            transactions,
            scan_last_tx_id,
        }
    }

    /// Whether a WAL follower references `account_id` — the account-history
    /// filter: balance entries, opens covering it, links naming it, flag updates.
    fn entry_references_account(e: &WalEntry, account_id: u64) -> bool {
        match e {
            WalEntry::Entry(te) => te.account_id == account_id,
            WalEntry::AccountOpened(a) => {
                account_id >= a.begin_account_id && account_id < a.begin_account_id + a.count as u64
            }
            WalEntry::AccountLinked(a) => a.parent_id == account_id || a.child_id == account_id,
            WalEntry::AccountFlagsUpdated(a) => a.account_id == account_id,
            _ => false,
        }
    }

    pub fn last_sealed_segment_id(&self) -> u32 {
        self.pipeline.last_sealed_id()
    }

    pub fn get_transaction_status(&self, transaction_id: u64) -> TransactionStatus {
        // Transaction id 0 is never assigned (sequencer starts at 1), and
        // ids above `last_sequenced_id` have never been issued on this node.
        if transaction_id == 0 || transaction_id > self.pipeline.last_sequenced_id() {
            return TransactionStatus::NotFound;
        }
        if self.pipeline.last_compute_id() < transaction_id {
            TransactionStatus::Pending
        } else if self.pipeline.last_commit_id() < transaction_id {
            TransactionStatus::Computed
        } else if self.pipeline.last_snapshot_id() < transaction_id {
            TransactionStatus::Committed
        } else {
            TransactionStatus::OnSnapshot
        }
    }

    pub fn last_sequenced_id(&self) -> u64 {
        self.pipeline.last_sequenced_id()
    }

    pub fn last_compute_id(&self) -> u64 {
        self.pipeline.last_compute_id()
    }

    /// Last tx id written to the WAL's page cache (buffered, pre-fsync).
    pub fn last_write_id(&self) -> u64 {
        self.pipeline.last_write_id()
    }

    pub fn last_commit_id(&self) -> u64 {
        self.pipeline.last_commit_id()
    }

    pub fn last_snapshot_id(&self) -> u64 {
        self.pipeline.last_snapshot_id()
    }

    /// Push a query into the Snapshot stage queue.
    ///
    /// The caller is responsible for creating the `QueryRequest` with a response
    /// channel and blocking on `rx.recv()`. Ledger is a thin passthrough — it
    /// wraps the request in `SnapshotMessage::Query` and handles backpressure.
    pub fn query(&self, mut request: QueryRequest) {
        let mut retry_count = 0u64;
        while self.pipeline.is_running() {
            match self.pipeline.try_push_query(request) {
                Ok(_) => return,
                Err(returned) => {
                    request = returned;
                    self.pipeline.wait_strategy().retry(retry_count);
                    retry_count += 1;
                }
            }
        }
    }

    /// Synchronous version of `query`. Blocks the current thread until the
    /// response is received from the Snapshot stage.
    pub fn query_block(&self, request: QueryRequest) -> QueryResponse {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let kind = request.kind;
        let original_respond = request.respond;

        self.query(QueryRequest {
            kind,
            respond: Box::new(move |res| {
                let res_clone = res.clone();
                original_respond(res);
                let _ = tx.send(res_clone);
            }),
        });

        rx.recv().expect("query_block: failed to receive response")
    }

    /// Read-side KV lookup (ADR-023): the resolved value for `key`, or `None`.
    /// Goes through the snapshot query channel like transaction lookups.
    pub fn get_kv(&self, key: storage::KeyPath) -> Option<storage::Value> {
        let res = self.query_block(QueryRequest {
            kind: QueryKind::GetKv { key: Box::new(key) },
            respond: Box::new(|_| {}),
        });
        match res {
            QueryResponse::Kv(v) => v,
            _ => None,
        }
    }

    /// Resolve an interned constant name to its id (ADR-023 §6), or `None`.
    pub fn get_constant(&self, name: &str) -> Option<u32> {
        let res = self.query_block(QueryRequest {
            kind: QueryKind::GetConstant {
                name: name.to_string(),
            },
            respond: Box::new(|_| {}),
        });
        match res {
            QueryResponse::Constant(id) => id,
            _ => None,
        }
    }

    pub fn get_transaction_block(&self, tx_id: u64) -> Option<CommittedTransaction> {
        let res = self.query_block(QueryRequest {
            kind: QueryKind::GetTransaction { tx_id },
            respond: Box::new(|_| {}),
        });
        match res {
            QueryResponse::Transaction(Some(tx)) => Some(tx),
            _ => None,
        }
    }

    pub fn get_transactions_block(&self, tx_ids: &[u64]) -> HashMap<u64, CommittedTransaction> {
        let res = self.query_block(QueryRequest {
            kind: QueryKind::GetTransactionBatch {
                tx_ids: tx_ids.to_vec(),
            },
            respond: Box::new(|_| {}),
        });
        match res {
            QueryResponse::TransactionBatch(txs) => {
                txs.into_iter().map(|tx| (tx.tx_id(), tx)).collect()
            }
            _ => HashMap::new(),
        }
    }

    pub fn submit_and_wait(&self, operation: Operation, level: WaitLevel) -> TransactionStatus {
        let tx_id = self.submit(operation);
        self.wait_for_transaction_level(tx_id, level);

        self.get_transaction_status(tx_id)
    }

    pub fn submit_and_wait_result(&self, operation: Operation) -> CommittedTransaction {
        let tx_id = self.submit(operation);
        self.wait_for_transaction_level(tx_id, WaitLevel::OnSnapshot);

        let transaction = self.get_transaction_block(tx_id);

        if let Some(transaction) = transaction {
            transaction
        } else {
            panic!("it should be impossible to get a None transaction from submit_and_wait_result");
        }
    }

    /// Open `count` accounts; wait until the snapshot reflects the tx, then read
    /// back the `AccountOpened` record to return the allocated id range. Account
    /// ids are allocated sequentially from 1.
    pub fn open_accounts(&self, count: u32) -> OpenAccountsResult {
        let tx_id = self.submit(Operation::OpenAccount { count, user_ref: 0 });
        self.wait_for_transaction_level(tx_id, WaitLevel::OnSnapshot);

        let transaction = self.get_transaction_block(tx_id);
        if transaction.is_none() {
            // should be impossible, but if it happens, there are serious internal bug
            panic!("open_accounts: failed to get transaction block");
        }
        let transaction = transaction.unwrap();
        if transaction.is_err() {
            return OpenAccountsResult {
                tx_id,
                fail_reason: transaction.get_fail_reason(),
                begin_account_id: 0,
                count: 0,
            };
        }

        // Read the AccountOpened record back from the committed transaction.
        let (begin_account_id, opened) = match self.query_block(QueryRequest {
            kind: QueryKind::GetTransaction { tx_id },
            respond: Box::new(|_| {}),
        }) {
            QueryResponse::Transaction(Some(result)) => result
                .entries
                .iter()
                .find_map(|e| match e {
                    WalEntry::AccountOpened(a) => Some((a.begin_account_id, a.count)),
                    _ => None,
                })
                .unwrap_or((0, 0)),
            _ => (0, 0),
        };

        OpenAccountsResult {
            tx_id,
            fail_reason: FailReason::NONE,
            begin_account_id,
            count: opened,
        }
    }

    pub fn submit_batch_and_wait(
        &self,
        ops: Vec<Operation>,
        level: WaitLevel,
    ) -> Vec<TransactionStatus> {
        let tx_ids: Vec<u64> = ops.into_iter().map(|op| self.submit(op)).collect();
        if let Some(&last) = tx_ids.last() {
            self.wait_for_transaction_level(last, level);
        }

        tx_ids
            .into_iter()
            .map(|tx_id| self.get_transaction_status(tx_id))
            .collect()
    }

    pub fn submit_batch_and_wait_result(
        &self,
        ops: Vec<Operation>,
        level: WaitLevel,
    ) -> Vec<CommittedTransaction> {
        let tx_ids: Vec<u64> = ops.into_iter().map(|op| self.submit(op)).collect();
        if let Some(&last) = tx_ids.last() {
            self.wait_for_transaction_level(last, level);
            // Results are read from the snapshot index; ensure the batch is indexed
            // (the snapshot indexes in tx order, so the last id implies the rest).
            self.wait_for_transaction_level(last, WaitLevel::OnSnapshot);
        }

        let result = self.get_transactions_block(&tx_ids);

        tx_ids
            .into_iter()
            .map(|tx_id| {
                result.get(&tx_id).cloned().expect(
                    "it should be impossible to get a None transaction from submit_batch_and_wait_result",
                )
            })
            .collect()
    }

    pub fn wait_for_transaction_level(&self, transaction_id: u64, level: WaitLevel) {
        let mut retry_count = 0u64;
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(10);

        while self.pipeline.is_running() {
            let reached = match level {
                WaitLevel::Computed => self.pipeline.last_compute_id() >= transaction_id,
                WaitLevel::Committed => self.pipeline.last_commit_id() >= transaction_id,
                WaitLevel::OnSnapshot => self.pipeline.last_snapshot_id() >= transaction_id,
            };

            if reached {
                return;
            }

            self.pipeline.wait_strategy().retry(retry_count);
            retry_count += 1;

            if start_time.elapsed() >= timeout {
                return;
            }
        }
    }

    /// Drive the cluster-commit gate consulted by the seal stage
    /// (ADR-0016 §10). The seal stage refuses to seal a segment whose
    /// last `tx_id` exceeds this value — guaranteeing sealed segments
    /// only ever contain cluster-committed transactions, so a future
    /// `start_with_recovery_until` reseed never has to unseal.
    ///
    /// The cluster supervisor wires this to:
    /// - leader: `Quorum::get()` advances (Stage 3 work).
    /// - follower: `leader_commit_tx_id` from incoming `AppendEntries`
    ///   (Stage 3 work).
    ///
    /// Plain `store` semantics — caller is responsible for advancing
    /// monotonically. Both Quorum and leader_commit are monotonically
    /// non-decreasing per Raft, so the supervisor can thread them
    /// straight through.
    ///
    /// Standalone (non-cluster) callers do not need to call this; the
    /// default `u64::MAX` leaves seal cadence unchanged.
    pub fn set_seal_watermark(&self, tx_id: u64) {
        self.pipeline.set_seal_watermark(tx_id);
    }

    /// Read the current seal watermark — observability for tests and
    /// the cluster's `Ping` / `GetStatus` surfaces.
    pub fn get_seal_watermark(&self) -> u64 {
        self.pipeline.get_seal_watermark()
    }

    // ── Cluster Mode Surface (ADR-015) ────────────────────────────────────

    /// Follower write path: hand a pre-validated batch to the
    /// **Transactor** as `TransactionInput::Replicated`. The Transactor
    /// mirrors the entries' effects onto its `balances` and `dedup`
    /// state and then forwards the entries to the WAL stage as
    /// `WalInput::Multi`. Routing through the Transactor (rather than
    /// pushing straight to WAL) keeps the follower's in-memory state
    /// in sync with the WAL — without this, a promotion to leader
    /// starts from stale state and silently double-applies user retries
    /// or mis-validates ops.
    pub fn append_wal_entries(&self, entries: Vec<WalEntry>) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let ctx = self.pipeline.ledger_context();
        let wait_strategy = ctx.wait_strategy();

        let mut pending = entries;
        let mut retry_count = 0u64;
        loop {
            if !ctx.is_running() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "ledger pipeline shut down before append_wal_entries completed",
                ));
            }
            match ctx.push_replicated_entries(pending) {
                Ok(()) => return Ok(()),
                Err(returned) => {
                    pending = returned;
                    wait_strategy.retry(retry_count);
                    retry_count = retry_count.saturating_add(1);
                }
            }
        }
    }

    /// Build a stateful `WalTailer` pre-positioned at the first
    /// `TxMetadata` with `tx_id >= from_tx_id`. After construction,
    /// `tail()` just reads from the cursor and advances.
    /// See [`WalTailer`] for cursor semantics (ADR-015).
    pub fn wal_tailer(&self, from_tx_id: u64) -> WalTailer {
        self.storage.wal_tailer(from_tx_id)
    }

    /// DIAG-flake-replication: storage's current active-segment id.
    /// Diagnostic accessor added while investigating an intermittent
    /// replication stall in `cluster_leader_replicates_to_follower`.
    pub fn last_segment_id(&self) -> u32 {
        self.storage.last_segment_id()
    }

    /// DIAG-flake-replication: on-disk size of the active `wal.bin`,
    /// or `None` if it doesn't exist. Used by the peer-task's periodic
    /// "still tailing 0 bytes" diagnostic to distinguish "writes never
    /// landed" from "writes landed but the tailer can't see them".
    pub fn active_wal_file_len(&self) -> Option<u64> {
        self.storage.active_wal_file_len()
    }

    /// DIAG-flake-replication: pointer-identity of the storage Arc.
    /// Lets the peer-task detect a ledger swap (reseed) — if the
    /// recorded pointer changes between two reads, the underlying
    /// `Storage`/WAL has been replaced out from under us.
    pub fn storage_ptr(&self) -> usize {
        Arc::as_ptr(&self.storage) as usize
    }

    pub fn wait_for_transaction(&self, transaction_id: u64) {
        self.wait_for_transaction_until(transaction_id, Duration::from_secs(100));
    }

    pub fn wait_for_transaction_until(&self, transaction_id: u64, duration: Duration) {
        let mut retry_count = 0;
        let start_time = std::time::Instant::now();

        while self.pipeline.is_running() {
            let current_snapshot_step = self.pipeline.last_snapshot_id();

            if current_snapshot_step >= transaction_id {
                break;
            }

            self.pipeline.wait_strategy().retry(retry_count);
            retry_count += 1;

            if start_time.elapsed() >= duration {
                break;
            }
        }
    }

    // wait for the last submitted transaction to pass through the pipeline
    pub fn wait_for_pass(&self) {
        let current_transaction_id = self.sequencer.last_id();

        self.wait_for_transaction(current_transaction_id);
    }

    pub fn wait_for_seal(&self) {
        if self.config.disable_seal {
            panic!("wait_for_seal is not supported in the current implementation");
        }
        self.wait_for_pass();

        let mut retry_count = 0;
        let start_seal_step_id = self.pipeline.seal_step_id();
        while self.pipeline.is_running() {
            let segment_count = self.storage.last_segment_id() - 1;
            if segment_count == self.pipeline.last_sealed_id() {
                return;
            }

            let cur_seal_step_id = self.pipeline.seal_step_id();
            debug_assert!(start_seal_step_id + 100 > cur_seal_step_id);

            retry_count += 1;
            self.pipeline.wait_strategy().retry(retry_count);
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        self.start_with_recovery_until(u64::MAX)
    }

    pub fn start_with_recovery_until(&mut self, watermark: u64) -> io::Result<()> {
        info!("Starting Ledger with recovery watermark = {}...", watermark);

        // Crash recovery first: a torn tail in wal.bin should be fixed
        // before we reason about which records cross the watermark.
        Recover::crash_recover_if_needed(&self.storage)?;

        // Physical truncation — fail fast if we cannot uphold the
        // watermark on disk. Idempotent: re-running on already-truncated
        // state is a no-op.
        self.storage.truncate_wal_above(watermark)?;

        // Pin the seal-stage gate to the recovery watermark before spawning
        // stages. Without this, the freshly spawned seal stage would observe
        // the default `u64::MAX` and seal segments whose last_tx sits above the
        // cluster-commit watermark — turning recoverable diverged tail into
        // immutable sealed history (ADR-0016 §10). The cluster supervisor
        // advances this watermark forward as new commits arrive.
        if watermark != u64::MAX {
            self.pipeline.set_seal_watermark(watermark);
        }

        let mut recover = Recover::new(&self.storage);
        let active_snapshot = recover.recover_until(watermark)?;

        // Each stage restores the pipeline index it owns from the recovered state.
        self.sequencer.recover(active_snapshot.last_tx_id);

        let (ring_writer, ring_reader) = TxRing::new(self.config.ring_size);

        // Start Transactor
        self.handles.push(
            transactor::Transactor {
                ctx: self.pipeline.transactor_context(),
                active_snapshot: &active_snapshot,
                config: &self.config,
                wasm_runtime: self.wasm_runtime.clone(),
                ring_writer,
            }
            .start()?,
        );

        // Start WAL
        self.handles.extend(
            Wal {
                ctx: self.pipeline.wal_context(),
                active_snapshot: &active_snapshot,
                storage: self.storage.clone(),
                ring_reader,
            }
            .start()
            .map_err(|e| Error::new(e.kind(), format!("failed to start wal: {}", e)))?,
        );
        self.handles.push(
            self.snapshot
                .start(self.pipeline.snapshot_context(), &active_snapshot)
                .map_err(|e| Error::new(e.kind(), format!("failed to start snapshot: {}", e)))?,
        );
        if !self.config.disable_seal {
            self.handles.push(
                Seal {
                    ctx: self.pipeline.seal_context(),
                    active_snapshot: &active_snapshot,
                    storage: self.storage.clone(),
                    config: &self.config,
                }
                .start()
                .map_err(|e| Error::new(e.kind(), format!("failed to start seal: {}", e)))?,
            );
        }

        debug!(
            "Ledger started successfully via start_with_recovery_until(watermark={}).",
            watermark
        );
        Ok(())
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        debug!("Shutting down Ledger...");
        self.pipeline.shutdown();
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }

        // Signal clean shutdown so the next start can distinguish a crash.
        if let Err(e) = Segment::create_wal_stop(&self.storage.config().data_dir) {
            spdlog::error!("failed to create wal.stop marker: {}", e);
        }
    }
}
