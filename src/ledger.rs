use crate::balance::Balance;
pub use crate::config::{LedgerConfig, StorageConfig};
use crate::pipeline::Pipeline;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryRequest, QueryResponse, Snapshot, SnapshotMessage};
use crate::storage::{Segment, Storage};
use crate::transaction::{Operation, SubmitResult, TransactionStatus, WaitLevel};
use crate::transactor::Transactor;
pub use crate::wait_strategy::WaitStrategy;
use crate::wal::Wal;
use crate::wasm_runtime::WasmRuntime;
use spdlog::{LevelFilter, info};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    seal: Seal,
    storage: Arc<Storage>,
    pipeline: Arc<Pipeline>,
    #[allow(dead_code)]
    wasm_runtime: Arc<WasmRuntime>,
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
        let snapshot = Snapshot::new(&config);
        let seal = Seal::new(&config, storage.clone());
        let wasm_runtime = Arc::new(WasmRuntime::new());
        let transactor = Transactor::new(&config, wasm_runtime.clone());

        Self {
            sequencer: Sequencer::new(pipeline.sequencer_context()),
            transactor,
            wal: Wal::new(storage.clone()),
            snapshot,
            seal,
            storage,
            pipeline,
            wasm_runtime,
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
