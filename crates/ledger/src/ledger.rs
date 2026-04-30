use crate::balance::Balance;
pub use crate::config::{LedgerConfig, StorageConfig};
pub use crate::pipeline::CommitHandler;
use crate::pipeline::Pipeline;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryRequest, QueryResponse, Snapshot, SnapshotMessage};
use crate::transaction::{Operation, SubmitResult, TransactionStatus, WaitLevel};
use crate::transactor::Transactor;
use crate::wal::Wal;
pub use crate::wait_strategy::WaitStrategy;
pub use crate::wasm_runtime::FunctionInfo;
use crate::wasm_runtime::{WasmRegistry, WasmRuntime};
use spdlog::{LevelFilter, info};
use std::io;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use storage::WalTailer;
use storage::entities::WalEntry;
use storage::{Segment, Storage};

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    seal: Seal,
    storage: Arc<Storage>,
    pipeline: Arc<Pipeline>,
    wasm_runtime: Arc<WasmRuntime>,
    /// Thin facade over the full register / unregister / list lifecycle.
    wasm_registry: WasmRegistry,
    handles: Vec<JoinHandle<()>>,
    #[allow(dead_code)]
    config: LedgerConfig,
}

impl Ledger {
    pub fn new(config: LedgerConfig) -> Self {
        spdlog::default_logger().set_level_filter(LevelFilter::MoreSevereEqual(config.log_level));

        info!("Initializing Ledger with config: {:?}", config);

        let storage = Storage::new(config.storage.clone()).unwrap();
        let storage = Arc::new(storage);

        let pipeline = Pipeline::new(&config);
        let wasm_runtime = Arc::new(WasmRuntime::new());
        let snapshot = Snapshot::new(&config, wasm_runtime.clone(), storage.clone());
        let seal = Seal::new(&config, storage.clone());
        let transactor = Transactor::new(&config, wasm_runtime.clone());
        let wasm_registry = WasmRegistry::new(
            wasm_runtime.clone(),
            storage.clone(),
            pipeline.ledger_context(),
            config.wait_strategy,
        );

        Self {
            sequencer: Sequencer::new(pipeline.sequencer_context()),
            transactor,
            wal: Wal::new(storage.clone()),
            snapshot,
            seal,
            storage,
            pipeline,
            wasm_runtime,
            wasm_registry,
            handles: Vec::new(),
            config,
        }
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
        self.wasm_registry.register(name, binary, override_existing)
    }

    pub fn unregister_function(&self, name: &str) -> io::Result<u16> {
        self.wasm_registry.unregister(name)
    }

    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.wasm_registry.list()
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.snapshot.get_balance(account_id)
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
        } else if let Some(reason) = self.transactor.transaction_rejection_reason(transaction_id) {
            TransactionStatus::Error(reason)
        } else if self.pipeline.last_commit_id() < transaction_id {
            TransactionStatus::Computed
        } else if self.pipeline.last_snapshot_id() < transaction_id {
            TransactionStatus::Committed
        } else {
            TransactionStatus::OnSnapshot
        }
    }

    pub fn last_compute_id(&self) -> u64 {
        self.pipeline.last_compute_id()
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
    pub fn query(&self, request: QueryRequest) {
        let mut msg = SnapshotMessage::Query(request);
        let mut retry_count = 0u64;
        while self.pipeline.is_running() {
            match self.pipeline.try_push_query(msg) {
                Ok(_) => return,
                Err(returned) => {
                    msg = returned;
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

    pub fn submit_and_wait(&self, operation: Operation, level: WaitLevel) -> SubmitResult {
        let tx_id = self.submit(operation);
        self.wait_for_transaction_level(tx_id, level);
        self.build_submit_result(tx_id)
    }

    pub fn submit_batch_and_wait(
        &self,
        ops: Vec<Operation>,
        level: WaitLevel,
    ) -> Vec<SubmitResult> {
        let tx_ids: Vec<u64> = ops.into_iter().map(|op| self.submit(op)).collect();
        if let Some(&last) = tx_ids.last() {
            self.wait_for_transaction_level(last, level);
        }
        tx_ids
            .into_iter()
            .map(|tx_id| self.build_submit_result(tx_id))
            .collect()
    }

    fn build_submit_result(&self, tx_id: u64) -> SubmitResult {
        let status = self.get_transaction_status(tx_id);
        if status.is_err() {
            SubmitResult {
                tx_id,
                fail_reason: status.error_reason(),
            }
        } else {
            SubmitResult {
                tx_id,
                fail_reason: storage::entities::FailReason::NONE,
            }
        }
    }

    pub fn wait_for_transaction_level(&self, transaction_id: u64, level: WaitLevel) {
        let mut retry_count = 0u64;
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(10);

        while self.pipeline.is_running() {
            // On error, return immediately regardless of wait level
            if self.pipeline.last_compute_id() >= transaction_id
                && self
                    .transactor
                    .transaction_rejection_reason(transaction_id)
                    .is_some()
            {
                return;
            }

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

    pub fn get_rejected_count(&self) -> u64 {
        self.transactor.get_rejected_count()
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

    /// Build a stateful `WalTailer` bound to this ledger's storage.
    /// See [`WalTailer`] for cursor semantics (ADR-015).
    pub fn wal_tailer(&self) -> WalTailer {
        self.storage.wal_tailer()
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
        while self.pipeline.is_running() {
            let segment_count = self.storage.last_segment_id() - 1;
            if segment_count == self.pipeline.last_sealed_id() {
                return;
            }

            retry_count += 1;
            self.pipeline.wait_strategy().retry(retry_count);
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        info!("Starting Ledger stages...");

        // Crash recovery: must run BEFORE normal recovery.
        Recover::crash_recover_if_needed(&self.storage).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed crash recovery during start: {}", e),
            )
        })?;

        self.recover(u64::MAX).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to recover ledger during start: {}", e),
            )
        })?;
        self.handles.push(
            self.transactor
                .start(self.pipeline.transactor_context())
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start transactor: {}", e))
                })?,
        );
        self.handles.extend(
            self.wal.start(self.pipeline.wal_context()).map_err(|e| {
                std::io::Error::new(e.kind(), format!("failed to start wal: {}", e))
            })?,
        );
        self.handles.push(
            self.snapshot
                .start(self.pipeline.snapshot_context())
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start snapshot: {}", e))
                })?,
        );
        if !self.config.disable_seal {
            self.handles.push(
                self.seal.start(self.pipeline.seal_context()).map_err(|e| {
                    io::Error::new(e.kind(), format!("failed to start seal: {}", e))
                })?,
            );
        }

        info!("Ledger started successfully.");

        Ok(())
    }

    fn recover(&mut self, watermark: u64) -> std::io::Result<()> {
        let mut recover = Recover::new(
            &mut self.transactor,
            &mut self.snapshot,
            &mut self.seal,
            &self.pipeline,
            &self.storage,
            &self.wasm_runtime,
        );

        recover.recover_until(watermark).map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to recover ledger state: {}", e))
        })
    }

    /// Boot variant of [`Self::start`] used by the cluster's
    /// recovery-mode reseed path (ADR-0016 §9 / §10). Truncates any
    /// WAL content above `watermark`, replays snapshot + WAL up to and
    /// including `watermark`, then spawns the pipeline stages — matching
    /// `start()`'s behaviour from that point onward.
    ///
    /// Intended use is **boot-time only**, on a freshly constructed
    /// `Ledger` whose stages have not been started yet. The cluster
    /// supervisor invokes this exactly once when it detects log
    /// divergence on an incoming `AppendEntries`: it drops the live
    /// `Arc<Ledger>` and reconstructs through this path with
    /// `watermark = leader_commit_tx_id` from the rejecting request.
    /// No leader code path ever calls this.
    ///
    /// Sequence:
    /// 1. ADR-006 crash-recovery on the active `wal.bin`.
    /// 2. Physical truncation: delete every segment fully past
    ///    `watermark`, byte-truncate the segment that straddles it,
    ///    discard stale snapshots, recompute `last_segment_id`.
    /// 3. Replay snapshot + WAL bounded by `watermark`. Pipeline
    ///    indices are clamped to `min(replayed_last_tx, watermark)`.
    /// 4. Spawn transactor/wal/snapshot/(seal) — same as `start()`.
    ///
    /// `watermark = u64::MAX` is equivalent to `start()` (no
    /// truncation, no snapshot filter, no clamp).
    pub fn start_with_recovery_until(&mut self, watermark: u64) -> std::io::Result<()> {
        info!("Starting Ledger with recovery watermark = {}...", watermark);

        // Crash recovery first: a torn tail in wal.bin should be fixed
        // before we reason about which records cross the watermark.
        Recover::crash_recover_if_needed(&self.storage).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed crash recovery during start_with_recovery_until: {}",
                    e
                ),
            )
        })?;

        // Physical truncation — fail fast if we cannot uphold the
        // watermark on disk. Idempotent: re-running on already-truncated
        // state is a no-op.
        self.storage.truncate_wal_above(watermark).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed truncate_wal_above({}) during start: {}",
                    watermark, e
                ),
            )
        })?;

        // Pin the seal-stage gate to the recovery watermark BEFORE
        // recovery's pre-seal pass and before spawning stages.
        // Without this, both the recovery pre-seal and the freshly
        // spawned seal stage would observe the default `u64::MAX` and
        // seal segments whose last_tx sits above the cluster-commit
        // watermark — turning recoverable diverged tail into
        // immutable sealed history (ADR-0016 §10). The cluster
        // supervisor will advance this watermark forward as new
        // commits arrive.
        if watermark != u64::MAX {
            self.pipeline.set_seal_watermark(watermark);
        }

        self.recover(watermark).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed bounded recovery (watermark={}) during start: {}",
                    watermark, e
                ),
            )
        })?;

        self.handles.push(
            self.transactor
                .start(self.pipeline.transactor_context())
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start transactor: {}", e))
                })?,
        );
        self.handles.extend(
            self.wal.start(self.pipeline.wal_context()).map_err(|e| {
                std::io::Error::new(e.kind(), format!("failed to start wal: {}", e))
            })?,
        );
        self.handles.push(
            self.snapshot
                .start(self.pipeline.snapshot_context())
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start snapshot: {}", e))
                })?,
        );
        if !self.config.disable_seal {
            self.handles
                .push(self.seal.start(self.pipeline.seal_context()).map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start seal: {}", e))
                })?);
        }

        info!(
            "Ledger started successfully via start_with_recovery_until(watermark={}).",
            watermark
        );
        Ok(())
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        info!("Shutting down Ledger...");
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
