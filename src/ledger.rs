use crate::balance::Balance;
use crate::pipeline::Pipeline;
pub use crate::pipeline_mode::PipelineMode;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryRequest, QueryResponse, Snapshot, SnapshotMessage};
use crate::storage::{Storage, StorageConfig};
use crate::transaction::{Operation, SubmitResult, Transaction, TransactionStatus, WaitLevel};
use crate::transactor::Transactor;
use crate::wal::Wal;
use spdlog::{Level, LevelFilter, info};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct LedgerConfig {
    pub max_accounts: usize,
    pub queue_size: usize,
    pub storage: StorageConfig,
    pub pipeline_mode: PipelineMode,
    pub log_level: Level,
    pub seal_check_internal: Duration,
    pub index_circle1_size: usize,
    pub index_circle2_size: usize,
    pub dedup_enabled: bool,
    pub dedup_window_ms: u64,
    pub disable_seal: bool,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            max_accounts: 1_000_000,
            queue_size: 1024,
            storage: StorageConfig::default(),
            pipeline_mode: PipelineMode::Balanced,
            log_level: Level::Info,
            seal_check_internal: Duration::from_secs(1),
            index_circle1_size: 1 << 20, // 1M slots
            index_circle2_size: 1 << 21, // 2M entries
            dedup_enabled: true,
            dedup_window_ms: 10_000, // 10 seconds
            disable_seal: false,
        }
    }
}

impl LedgerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn temp() -> Self {
        let mut dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        dir.push(format!("temp_{}", rand));

        Self {
            storage: StorageConfig {
                data_dir: dir.to_string_lossy().to_string(),
                temporary: true,
                snapshot_frequency: 2,
                wal_segment_size_mb: 2048,
            },
            log_level: Level::Critical,
            seal_check_internal: Duration::from_millis(10),
            ..Default::default()
        }
    }

    pub fn bench() -> Self {
        let mut dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        dir.push(format!("temp_{}", rand));

        Self {
            storage: StorageConfig {
                data_dir: dir.to_string_lossy().to_string(),
                temporary: true,
                snapshot_frequency: u32::MAX,
                wal_segment_size_mb: 2048,
            },
            log_level: Level::Critical,
            seal_check_internal: Duration::from_mins(10),
            disable_seal: true, // disable seal to avoid unnecessary disk IO to finish the benchmark process faster
            ..Default::default()
        }
    }
}

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    seal: Seal,
    storage: Arc<Storage>,
    pipeline: Arc<Pipeline>,
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

        let pipeline = Pipeline::new(config.queue_size, 1024 * 128);

        let snapshot = Snapshot::new(
            config.max_accounts,
            config.pipeline_mode,
            config.index_circle1_size,
            config.index_circle2_size,
        );

        let seal = Seal::new(config.max_accounts, storage.clone(), config.seal_check_internal);

        Self {
            sequencer: Sequencer::new(pipeline.sequencer_context()),
            transactor: Transactor::new(
                config.max_accounts,
                config.pipeline_mode,
                config.dedup_enabled,
                config.dedup_window_ms,
            ),
            wal: Wal::new(storage.clone(), config.pipeline_mode),
            snapshot,
            seal,
            storage,
            pipeline,
            handles: Vec::new(),
            config: config.clone(),
        }
    }

    pub fn submit(&self, operation: Operation) -> u64 {
        let transaction = Transaction::new(operation);
        self.sequencer.submit(transaction)
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.snapshot.get_balance(account_id)
    }

    pub fn last_sealed_segment_id(&self) -> u32 {
        self.pipeline.last_sealed_id()
    }

    pub fn get_transaction_status(&self, transaction_id: u64) -> TransactionStatus {
        if self.pipeline.last_computed_id() < transaction_id {
            TransactionStatus::Pending
        } else if let Some(reason) = self.transactor.transaction_rejection_reason(transaction_id) {
            TransactionStatus::Error(reason)
        } else if self.pipeline.last_committed_id() < transaction_id {
            TransactionStatus::Computed
        } else if self.pipeline.last_snapshot_id() < transaction_id {
            TransactionStatus::Committed
        } else {
            TransactionStatus::OnSnapshot
        }
    }

    pub fn last_computed_id(&self) -> u64 {
        self.pipeline.last_computed_id()
    }

    pub fn last_committed_id(&self) -> u64 {
        self.pipeline.last_committed_id()
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
                    self.config.pipeline_mode.wait_strategy(retry_count);
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
            if self.pipeline.last_computed_id() >= transaction_id
                && self
                    .transactor
                    .transaction_rejection_reason(transaction_id)
                    .is_some()
            {
                return;
            }

            let reached = match level {
                WaitLevel::Processed => self.pipeline.last_computed_id() >= transaction_id,
                WaitLevel::Committed => self.pipeline.last_committed_id() >= transaction_id,
                WaitLevel::Snapshotted => self.pipeline.last_snapshot_id() >= transaction_id,
            };

            if reached {
                return;
            }

            self.config.pipeline_mode.wait_strategy(retry_count);
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
        self.wait_for_transaction_until(transaction_id, Duration::from_secs(10));
    }

    pub fn wait_for_transaction_until(&self, transaction_id: u64, duration: Duration) {
        let mut retry_count = 0;
        let start_time = std::time::Instant::now();

        while self.pipeline.is_running() {
            let current_snapshot_step = self.pipeline.last_snapshot_id();

            if current_snapshot_step >= transaction_id {
                break;
            }

            self.config.pipeline_mode.wait_strategy(retry_count);
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
            self.config.pipeline_mode.wait_strategy(retry_count);
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        info!("Starting Ledger stages...");
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
        self.handles
            .push(self.wal.start(self.pipeline.wal_context()).map_err(|e| {
                std::io::Error::new(e.kind(), format!("failed to start wal: {}", e))
            })?);
        self.handles.push(
            self.snapshot
                .start(self.pipeline.snapshot_context())
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("failed to start snapshot: {}", e))
                })?,
        );
        if !self.config.disable_seal {
            self.handles.push(
                self.seal
                    .start(self.pipeline.seal_context())
                    .map_err(|e| {
                        std::io::Error::new(e.kind(), format!("failed to start seal: {}", e))
                    })?,
            );
        }

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
    }
}
