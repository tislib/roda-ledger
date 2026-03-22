use crate::balance::BalanceDataType;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::transaction::{Transaction, TransactionDataType, TransactionStatus};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, yield_now};

#[derive(Clone, Debug)]
pub struct LedgerConfig {
    pub queue_size: usize,
    pub location: Option<String>,
    pub in_memory: bool,
    pub snapshot_interval: std::time::Duration,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            queue_size: 1024,
            location: None,
            in_memory: false,
            snapshot_interval: std::time::Duration::from_secs(600),
        }
    }
}

pub struct Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    sequencer: Sequencer<Data, BalanceData>,
    transactor: Transactor<Data, BalanceData>,
    wal: Wal<Data, BalanceData>,
    snapshot: Snapshot<Data, BalanceData>,
    running: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl<Data, BalanceData> Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(config: LedgerConfig) -> Self {
        let sequencer_transactor_queue = Arc::new(ArrayQueue::new(config.queue_size));
        let transactor_wal_queue = Arc::new(ArrayQueue::new(config.queue_size));
        let wal_snapshot_queue = Arc::new(ArrayQueue::new(config.queue_size));
        let running = Arc::new(AtomicBool::new(true));

        Self {
            sequencer: Sequencer::new(sequencer_transactor_queue.clone()),
            transactor: Transactor::new(
                sequencer_transactor_queue,
                transactor_wal_queue.clone(),
                running.clone(),
            ),
            wal: Wal::new(
                transactor_wal_queue,
                wal_snapshot_queue.clone(),
                config.location.as_deref(),
                config.in_memory,
                running.clone(),
            ),
            snapshot: Snapshot::new(
                wal_snapshot_queue,
                config.location.as_deref(),
                config.in_memory,
                config.snapshot_interval,
                running.clone(),
            ),
            running,
            handles: Vec::new(),
        }
    }

    pub fn submit(&self, transaction: Transaction<Data, BalanceData>) -> u64 {
        self.sequencer.submit(transaction)
    }

    pub fn get_balance(&self, account_id: u64) -> BalanceData {
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

    pub fn wait_for_transaction(&mut self, transaction_id: u64) {
        let mut prev_snapshot_step = self.snapshot.last_processed_transaction_id();
        let mut prev_rejected_count = self.transactor.rejected_count() as u64;

        let mut no_movement_count = 1;

        loop {
            let current_snapshot_step = self.snapshot.last_processed_transaction_id();
            let current_wal_step = self.wal.last_processed_transaction_id();
            let current_transactor_step = self.transactor.last_processed_transaction_id();

            let rejected_count = self.transactor.rejected_count() as u64;

            let mut target_step = transaction_id;
            if rejected_count >= transaction_id {
                return;
            }
            target_step -= rejected_count;

            if current_transactor_step >= target_step
                && current_wal_step >= target_step
                && current_snapshot_step >= target_step
            {
                return;
            }
            yield_now();

            // Break if no movement
            if prev_snapshot_step == current_snapshot_step && prev_rejected_count == rejected_count
            {
                no_movement_count += 1;
            } else {
                no_movement_count = 0;
            }
            if no_movement_count > 10_000 {
                return;
            }

            prev_rejected_count = rejected_count;
            prev_snapshot_step = current_snapshot_step;
        }
    }

    pub fn start(&mut self) {
        self.replay();
        self.handles.push(self.transactor.start());
        self.handles.push(self.wal.start());
        self.handles.extend(self.snapshot.start());
    }

    fn replay(&mut self) {
        // 1. Restore snapshot from disk
        let _ = self.snapshot.restore();

        // 2. Update transactor balances from snapshot
        let balances = self.snapshot.get_all_balances();
        self.transactor.load_balances(balances);

        // 3. Replay WAL records
        let last_snapshot_tx_id = self.snapshot.last_processed_transaction_id();
        let last_wal_pos = self.snapshot.last_wal_position();

        let tx_size = std::mem::size_of::<Transaction<Data, BalanceData>>() as u64;
        let start_pos = if last_snapshot_tx_id == 0 {
            0
        } else {
            last_wal_pos + tx_size
        };

        let records = self.wal.get_records(start_pos);
        let mut last_replayed_id = last_snapshot_tx_id;

        for tx in records {
            if tx.id > last_snapshot_tx_id {
                self.transactor.reprocess_transaction(tx);
                self.snapshot.reprocess_transaction(tx);
                last_replayed_id = tx.id;
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

impl<Data, BalanceData> Drop for Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }
    }
}
