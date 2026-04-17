use crate::balance::Balance;
pub use crate::config::{LedgerConfig, StorageConfig};
use crate::entities::{FunctionRegistered, WalEntry};
use crate::pipeline::{LedgerContext, Pipeline};
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryRequest, QueryResponse, Snapshot, SnapshotMessage};
use crate::storage::{Segment, Storage, functions as function_storage};
use crate::transaction::{Operation, SubmitResult, TransactionStatus, WaitLevel};
use crate::transactor::Transactor;
pub use crate::wait_strategy::WaitStrategy;
use crate::wal::Wal;
use crate::wasm_runtime::{self, WasmRuntime};
use spdlog::{LevelFilter, info};
use std::io;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Metadata returned by [`Ledger::list_functions`] for every currently
/// registered WASM function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionInfo {
    pub name: String,
    pub version: u16,
    pub crc32c: u32,
}

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    seal: Seal,
    storage: Arc<Storage>,
    pipeline: Arc<Pipeline>,
    wasm_runtime: Arc<WasmRuntime>,
    /// Handle to push non-transactional WAL entries (e.g.
    /// `FunctionRegistered`) directly onto the WAL input queue.
    ledger_ctx: LedgerContext,
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
        let ledger_ctx = pipeline.ledger_context();

        Self {
            sequencer: Sequencer::new(pipeline.sequencer_context()),
            transactor,
            wal: Wal::new(storage.clone()),
            snapshot,
            seal,
            storage,
            pipeline,
            wasm_runtime,
            ledger_ctx,
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

    // ─── WASM Function Registry API (ADR-014) ──────────────────────────────

    /// Register a WASM function under `name` at the next version.
    ///
    /// Steps:
    /// 1. Validate the binary against the ABI (`execute(i64×8)->i32`).
    /// 2. If `override_existing == false` and the name is already loaded,
    ///    return [`io::ErrorKind::AlreadyExists`].
    /// 3. Compute next version and write the binary atomically to
    ///    `{data_dir}/functions/{name}_v{N}.wasm`.
    /// 4. Push `WalEntry::FunctionRegistered` onto `wal_input`.
    /// 5. **Block** until the Snapshot stage commits the record and the
    ///    `WasmRuntime` reflects the new handler (polling
    ///    `wasm_runtime.contains(name)` with the pipeline's wait strategy).
    ///
    /// Returns `(version, crc32c)`.
    pub fn register_function(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> io::Result<(u16, u32)> {
        // 1. Validate (ABI + name).
        wasm_runtime::validate_name(name)?;
        self.wasm_runtime.validate(binary)?;

        // 2. Uniqueness check against the live registry (source of truth
        //    for "currently active").
        if !override_existing && self.wasm_runtime.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("function `{}` is already registered", name),
            ));
        }

        // 3. Pick next version. The WAL + WasmRuntime carry the
        //    authoritative version counter; the on-disk `functions/`
        //    directory is just reference data.
        let next_version = self.next_function_version(name)?;
        function_storage::write_function(&self.storage, name, next_version, binary)?;
        let crc = crc32c::crc32c(binary);

        // 4. Push WAL record (non-transactional; bypasses Transactor).
        let record = FunctionRegistered::new(name, next_version, crc);
        self.push_wal_entry_blocking(WalEntry::FunctionRegistered(record));

        // 5. Wait for the Snapshot stage to commit it (WasmRuntime
        //    update_seq bump guarantees the handler is loaded and
        //    visible to every WasmRuntimeEngine's next refresh).
        self.wait_until_function_loaded(name, crc);

        Ok((next_version, crc))
    }

    /// Unregister the currently-loaded function under `name`. Writes a
    /// 0-byte `{name}_v{N+1}.wasm` on disk and a WAL record with
    /// `crc32c = 0`. Blocks until the Snapshot stage commits it and the
    /// handler is gone from the live `WasmRuntime`.
    ///
    /// Returns the version number stamped on the unregister record.
    /// Errors with [`io::ErrorKind::NotFound`] if `name` is not currently
    /// registered.
    pub fn unregister_function(&self, name: &str) -> io::Result<u16> {
        wasm_runtime::validate_name(name)?;

        if !self.wasm_runtime.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("function `{}` is not registered", name),
            ));
        }

        let next_version = self.next_function_version(name)?;
        // Empty file under the next version — audit-trail preserved.
        function_storage::write_function(&self.storage, name, next_version, &[])?;
        let record = FunctionRegistered::new(name, next_version, 0);
        self.push_wal_entry_blocking(WalEntry::FunctionRegistered(record));

        self.wait_until_function_unloaded(name);
        Ok(next_version)
    }

    /// List every currently-loaded function with its version + CRC32C.
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.wasm_runtime
            .handlers_snapshot()
            .into_iter()
            .map(|(name, version, crc32c)| FunctionInfo {
                name,
                version,
                crc32c,
            })
            .collect()
    }

    // ─── internal helpers for the registry API ─────────────────────────────

    /// Next monotonic version number for `name`. The authoritative
    /// counter is the in-memory registry (installed by the Snapshot
    /// stage from committed `FunctionRegistered` WAL records); the
    /// on-disk `functions/` directory is reference data only.
    fn next_function_version(&self, name: &str) -> io::Result<u16> {
        let prev = self.wasm_runtime.version_of(name).unwrap_or(0);
        prev.checked_add(1)
            .ok_or_else(|| io::Error::other("function version overflow (u16 exhausted)"))
    }

    /// Push a WAL entry with backpressure. Uses the pipeline's
    /// configured wait strategy when the queue is full.
    fn push_wal_entry_blocking(&self, mut entry: WalEntry) {
        let mut retry_count = 0u64;
        while self.ledger_ctx.is_running() {
            match self.ledger_ctx.push_wal_entry(entry) {
                Ok(()) => return,
                Err(returned) => {
                    entry = returned;
                    self.ledger_ctx.wait_strategy().retry(retry_count);
                    retry_count += 1;
                }
            }
        }
    }

    fn wait_until_function_loaded(&self, name: &str, expected_crc: u32) {
        let mut retry_count = 0u64;
        while self.pipeline.is_running() {
            if self.wasm_runtime.crc32c_of(name) == Some(expected_crc) {
                return;
            }
            self.pipeline.wait_strategy().retry(retry_count);
            retry_count += 1;
        }
    }

    fn wait_until_function_unloaded(&self, name: &str) {
        let mut retry_count = 0u64;
        while self.pipeline.is_running() {
            if !self.wasm_runtime.contains(name) {
                return;
            }
            self.pipeline.wait_strategy().retry(retry_count);
            retry_count += 1;
        }
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.snapshot.get_balance(account_id)
    }

    pub fn last_sealed_segment_id(&self) -> u32 {
        self.pipeline.last_sealed_id()
    }

    pub fn get_transaction_status(&self, transaction_id: u64) -> TransactionStatus {
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
                fail_reason: crate::entities::FailReason::NONE,
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

        self.recover().map_err(|e| {
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
            self.handles
                .push(self.seal.start(self.pipeline.seal_context()).map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start seal: {}", e))
                })?);
        }

        info!("Ledger started successfully.");

        Ok(())
    }

    fn recover(&mut self) -> std::io::Result<()> {
        let mut recover = Recover::new(
            &mut self.transactor,
            &mut self.snapshot,
            &mut self.seal,
            &self.pipeline,
            &self.storage,
            &self.wasm_runtime,
        );

        recover.recover().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to recover ledger state: {}", e))
        })
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
