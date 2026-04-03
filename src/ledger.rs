use crate::balance::Balance;
use crate::entities::WalEntry;
pub use crate::pipeline_mode::PipelineMode;
use crate::recover::Recover;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::storage::{Storage, StorageConfig};
use crate::transaction::{Operation, Transaction, TransactionStatus};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use spdlog::{Level, LevelFilter, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::thread::{JoinHandle, sleep, yield_now};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct LedgerConfig {
    pub max_accounts: usize,
    pub queue_size: usize,
    pub storage: StorageConfig,
    pub pipeline_mode: PipelineMode,
    pub log_level: Level,
    pub seal_check_internal: Duration,
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
                ..Default::default()
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
        let wal_snapshot_queue: Arc<ArrayQueue<WalEntry>> =
            Arc::new(ArrayQueue::new(config.queue_size));
        let running = Arc::new(AtomicBool::new(true));

        let snapshot = Snapshot::new(
            wal_snapshot_queue.clone(),
            config.max_accounts,
            storage.clone(),
            running.clone(),
            config.pipeline_mode,
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
            ),
            wal: Wal::new(
                transactor_wal_queue,
                wal_snapshot_queue,
                storage.clone(),
                running.clone(),
                config.pipeline_mode,
            ),
            snapshot,
            seal,
            storage,
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
                return;
            }

            self.config.pipeline_mode.wait_strategy(retry_count);
            retry_count += 1;

            if start_time.elapsed() >= duration {
                break;
            }
        }
    }

    pub fn wait_for_seal(&self) {
        let mut retry_count = 0;
        loop {
            let segment_count = self.storage.last_segment_id();
            if segment_count.is_err() {
                error!(
                    "Failed to get last segment id: {}",
                    segment_count.err().unwrap()
                );
                sleep(Duration::from_secs(1));
                continue;
            }
            let segment_count = segment_count.unwrap();
            if segment_count == self.seal.last_sealed_segment_id() {
                println!("All segments sealed: {}", segment_count);
                return;
            }

            retry_count += 1;
            self.config.pipeline_mode.wait_strategy(retry_count);
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        info!("Starting Ledger stages...");
        self.recover()?;
        self.handles.push(self.transactor.start()?);
        self.handles.push(self.wal.start()?);
        self.handles.push(self.snapshot.start()?);
        self.handles.push(self.seal.start()?);
        // wait for all threads to start
        sleep(Duration::from_millis(100));

        Ok(())
    }

    fn recover(&mut self) -> std::io::Result<()> {
        let mut recover = Recover::new(
            &mut self.transactor,
            &self.wal,
            &self.snapshot,
            &mut self.seal,
            &self.sequencer,
            &self.storage,
        );

        recover.recover()
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
