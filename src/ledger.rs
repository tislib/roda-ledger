use crate::balance::Balance;
use crate::entities::WalEntry;
pub use crate::pipeline_mode::PipelineMode;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::{QueryKind, QueryRequest, QueryResponse, Snapshot, SnapshotMessage};
use crate::storage::{Storage, StorageConfig};
use crate::transaction::{Operation, Transaction, TransactionStatus};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use spdlog::{Level, LevelFilter, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
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
        }
    }
}

impl LedgerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn temp() -> Self {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        dir.push(format!("roda-ledger-{}", nanos));
        // Directory creation is handled by Storage::new()

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
}

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    seal: Seal,
    storage: Arc<Storage>,
    running: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    query_queue: Arc<ArrayQueue<SnapshotMessage>>,
    #[allow(dead_code)]
    config: LedgerConfig,
}

impl Ledger {
    pub fn new(config: LedgerConfig) -> Self {
        spdlog::default_logger().set_level_filter(LevelFilter::MoreSevereEqual(config.log_level));

        info!("Initializing Ledger with config: {:?}", config);

        let storage = Storage::new(config.storage.clone()).unwrap();
        let storage = Arc::new(storage);

        let sequencer_transactor_queue = Arc::new(ArrayQueue::new(config.queue_size));
        let transactor_wal_queue: Arc<ArrayQueue<WalEntry>> = Arc::new(ArrayQueue::new(1024 * 128));
        let wal_snapshot_queue: Arc<ArrayQueue<SnapshotMessage>> =
            Arc::new(ArrayQueue::new(config.queue_size));
        let running = Arc::new(AtomicBool::new(true));

        let snapshot = Snapshot::new(
            wal_snapshot_queue.clone(),
            config.max_accounts,
            running.clone(),
            config.pipeline_mode,
            config.index_circle1_size,
            config.index_circle2_size,
        );

        let seal = Seal::new(
            config.max_accounts,
            storage.clone(),
            running.clone(),
            config.seal_check_internal,
        );

        Self {
            config: config.clone(),
            sequencer: Sequencer::new(sequencer_transactor_queue.clone()),
            transactor: Transactor::new(
                sequencer_transactor_queue,
                transactor_wal_queue.clone(),
                running.clone(),
                config.max_accounts,
                config.pipeline_mode,
                config.dedup_enabled,
                config.dedup_window_ms,
            ),
            wal: Wal::new(
                transactor_wal_queue,
                wal_snapshot_queue.clone(),
                storage.clone(),
                running.clone(),
                config.pipeline_mode,
            ),
            snapshot,
            seal,
            storage,
            query_queue: wal_snapshot_queue,
            running,
            handles: Vec::new(),
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
        self.seal.last_sealed_segment_id()
    }

    pub fn get_transaction_status(&self, transaction_id: u64) -> TransactionStatus {
        if self.transactor.last_processed_transaction_id() < transaction_id {
            TransactionStatus::Pending
        } else if let Some(reason) = self.transactor.transaction_rejection_reason(transaction_id) {
            TransactionStatus::Error(reason)
        } else if self.wal.last_processed_transaction_id() < transaction_id {
            TransactionStatus::Computed
        } else if self.snapshot.last_processed_transaction_id() < transaction_id {
            TransactionStatus::Committed
        } else {
            TransactionStatus::OnSnapshot
        }
    }

    pub fn last_computed_id(&self) -> u64 {
        self.transactor.last_processed_transaction_id()
    }

    pub fn last_committed_id(&self) -> u64 {
        self.wal.last_processed_transaction_id()
    }

    pub fn last_snapshot_id(&self) -> u64 {
        self.snapshot.last_processed_transaction_id()
    }

    /// Push a query into the Snapshot stage queue.
    ///
    /// The caller is responsible for creating the `QueryRequest` with a response
    /// channel and blocking on `rx.recv()`. Ledger is a thin passthrough — it
    /// wraps the request in `SnapshotMessage::Query` and handles backpressure.
    pub fn query(&self, request: QueryRequest) {
        let mut msg = SnapshotMessage::Query(request);
        let mut retry_count = 0u64;
        loop {
            match self.query_queue.push(msg) {
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

    pub fn get_rejected_count(&self) -> u64 {
        self.transactor.get_rejected_count()
    }

    pub fn wait_for_transaction(&self, transaction_id: u64) {
        self.wait_for_transaction_until(transaction_id, Duration::from_secs(10));
    }

    pub fn wait_for_transaction_until(&self, transaction_id: u64, duration: Duration) {
        let mut retry_count = 0;
        let start_time = std::time::Instant::now();

        loop {
            let current_snapshot_step = self.snapshot.last_processed_transaction_id();

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
        self.wait_for_pass();

        let mut retry_count = 0;
        loop {
            let segment_count = self.storage.last_segment_id() - 1;
            if segment_count == self.seal.last_sealed_segment_id() {
                return;
            }

            retry_count += 1;
            self.config.pipeline_mode.wait_strategy(retry_count);
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        info!("Starting Ledger stages...");
        self.recover().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to recover ledger during start: {}", e))
        })?;
        self.handles.push(self.transactor.start().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to start transactor: {}", e))
        })?);
        self.handles.push(self.wal.start().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to start wal: {}", e))
        })?);
        self.handles.push(self.snapshot.start().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to start snapshot: {}", e))
        })?);
        self.handles.push(self.seal.start().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to start seal: {}", e))
        })?);

        Ok(())
    }

    fn recover(&mut self) -> std::io::Result<()> {
        let mut recover = Recover::new(
            &mut self.transactor,
            &mut self.snapshot,
            &mut self.seal,
            &self.sequencer,
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
        self.running.store(false, Ordering::Relaxed);
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }
    }
}
