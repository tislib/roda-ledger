use crate::balance::Balance;
use crate::entities::WalEntry;
pub use crate::pipeline_mode::PipelineMode;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::transaction::{Operation, Transaction, TransactionStatus};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use spdlog::{Level, LevelFilter, debug, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, sleep, yield_now};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct LedgerConfig {
    pub max_accounts: usize,
    pub queue_size: usize,
    pub location: Option<String>,
    pub in_memory: bool,
    pub temporary: bool, // data will be deleted after the ledger is dropped
    pub snapshot_interval: std::time::Duration,
    pub pipeline_mode: PipelineMode,
    pub log_level: Level,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            max_accounts: 1_000_000,
            queue_size: 2048,
            location: None,
            in_memory: false,
            temporary: false,
            snapshot_interval: Duration::from_secs(600),
            pipeline_mode: PipelineMode::Balanced,
            log_level: Level::Info,
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
        let _ = std::fs::create_dir_all(&dir);

        Self {
            location: Some(dir.to_string_lossy().to_string()),
            temporary: true,
            log_level: Level::Critical,
            ..Default::default()
        }
    }
}

pub struct Ledger {
    sequencer: Sequencer,
    transactor: Transactor,
    wal: Wal,
    snapshot: Snapshot,
    running: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    config: LedgerConfig,
}

impl Ledger {
    pub fn new(config: LedgerConfig) -> Self {
        spdlog::default_logger().set_level_filter(LevelFilter::MoreSevereEqual(config.log_level));

        info!("Initializing Ledger with config: {:?}", config);

        let sequencer_transactor_queue = Arc::new(ArrayQueue::new(config.queue_size));
        let transactor_wal_queue: Arc<ArrayQueue<WalEntry>> =
            Arc::new(ArrayQueue::new(config.queue_size));
        let wal_snapshot_queue: Arc<ArrayQueue<WalEntry>> =
            Arc::new(ArrayQueue::new(config.queue_size));
        let running = Arc::new(AtomicBool::new(true));

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
                wal_snapshot_queue.clone(),
                config.location.as_deref(),
                config.in_memory,
                running.clone(),
                config.pipeline_mode,
            ),
            snapshot: Snapshot::new(
                wal_snapshot_queue,
                config.location.as_deref(),
                config.in_memory,
                config.snapshot_interval,
                running.clone(),
                config.max_accounts,
                config.pipeline_mode,
            ),
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
        let mut prev_snapshot_step = self.snapshot.last_processed_transaction_id();

        let mut no_movement_count = 1;

        loop {
            let current_snapshot_step = self.snapshot.last_processed_transaction_id();
            let current_wal_step = self.wal.last_processed_transaction_id();
            let current_transactor_step = self.transactor.last_processed_transaction_id();

            if current_transactor_step >= transaction_id
                && current_wal_step >= transaction_id
                && current_snapshot_step >= transaction_id
            {
                return;
            }
            yield_now();

            // Break if no movement
            if prev_snapshot_step == current_snapshot_step {
                no_movement_count += 1;
            } else {
                no_movement_count = 0;
            }
            if no_movement_count > 1_000_000 {
                return;
            }

            prev_snapshot_step = current_snapshot_step;
        }
    }

    pub fn start(&mut self) {
        info!("Starting Ledger stages...");
        self.replay();
        self.handles.push(self.transactor.start());
        self.handles.push(self.wal.start());
        self.handles.extend(self.snapshot.start());
        // wait for all threads to start
        sleep(Duration::from_millis(100));
    }

    fn replay(&mut self) {
        info!("Starting WAL replay...");
        // 1. Restore snapshot from disk
        let _ = self.snapshot.restore();

        // 2. Update transactor balances from snapshot
        let balances = self.snapshot.get_all_balances();
        self.transactor.load_balances(balances);

        // 3. Replay WAL records
        let last_snapshot_tx_id = self.snapshot.last_processed_transaction_id();
        let last_wal_pos = self.snapshot.last_wal_position();

        // NOTE: In the entries-based model, we read WalEntry, not Transaction.
        // Replaying transactions is no longer needed since we have entries.
        let start_pos = last_wal_pos; // Simplification

        let records = self.wal.get_records(start_pos);
        let mut last_replayed_id = last_snapshot_tx_id;

        for entry in records {
            let tx_id = entry.tx_id();
            if tx_id > last_snapshot_tx_id {
                debug!("Replaying transaction {}", tx_id);
                self.transactor.apply_wal_entry(entry);
                self.snapshot.reprocess_transaction(entry);
                last_replayed_id = self.transactor.last_processed_transaction_id();
            }
        }

        // Synchronize last_processed_transaction_id
        self.wal.set_last_processed_transaction_id(last_replayed_id);
        self.snapshot
            .set_last_processed_transaction_id(last_replayed_id);

        // Set sequencer's next ID
        self.sequencer.set_next_id(last_replayed_id + 1);
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        info!("Shutting down Ledger...");
        self.running.store(false, Ordering::Relaxed);
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }

        if self.config.temporary
            && let Some(loc) = self.config.location.clone()
        {
            let _ = std::fs::remove_dir_all(loc);
        }
    }
}
