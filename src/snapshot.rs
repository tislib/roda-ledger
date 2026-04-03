use crate::balance::Balance;
use crate::entities::WalEntry;
use crate::pipeline_mode::PipelineMode;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Snapshot {
    inbound: Arc<ArrayQueue<WalEntry>>,
    balances: Arc<Vec<AtomicI64>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

struct SnapshotRunner {
    inbound: Arc<ArrayQueue<WalEntry>>,
    balances: Arc<Vec<AtomicI64>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

impl Snapshot {
    pub fn new(
        inbound: Arc<ArrayQueue<WalEntry>>,
        account_count: usize,
        running: Arc<AtomicBool>,
        pipeline_mode: PipelineMode,
    ) -> Self {
        let balances: Arc<Vec<AtomicI64>> =
            Arc::new((0..account_count).map(|_| AtomicI64::new(0)).collect());

        Self {
            inbound,
            balances,
            last_processed_transaction_id: Arc::new(Default::default()),
            running,
            pipeline_mode,
        }
    }

    pub fn start(&self) -> std::io::Result<JoinHandle<()>> {
        let runner = SnapshotRunner {
            inbound: self.inbound.clone(),
            balances: self.balances.clone(),
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            pending_entries: 0,
            running: self.running.clone(),
            pipeline_mode: self.pipeline_mode,
        };
        std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut r = runner;
                r.run();
            })
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id.load(Ordering::Acquire)
    }

    pub(crate) fn store_last_processed_id(&self, id: u64) {
        self.last_processed_transaction_id
            .store(id, Ordering::Release);
    }

    pub(crate) fn recover_balance(&self, account_id: usize, computed_balance: i64) {
        self.balances[account_id].store(computed_balance, Ordering::Release);
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.balances[account_id as usize].load(Ordering::Acquire)
    }
}

impl SnapshotRunner {
    pub fn run(&mut self) {
        let mut retry_count = 0;
        let mut last_processed_tx_id = 0;
        while self.running.load(Ordering::Relaxed) {
            if let Some(wal_entry) = self.inbound.pop() {
                retry_count = 0;

                match wal_entry {
                    WalEntry::Metadata(m) => {
                        self.pending_entries = m.entry_count;
                        last_processed_tx_id = m.tx_id;
                    }
                    WalEntry::Entry(e) => {
                        self.balances[e.account_id as usize]
                            .store(e.computed_balance, Ordering::Release);
                        self.pending_entries -= 1;
                    }
                    WalEntry::SegmentSealed(_) => {}
                    WalEntry::SegmentHeader(_) => {}
                }
                if last_processed_tx_id > 0 && self.pending_entries == 0 {
                    self.last_processed_transaction_id
                        .store(last_processed_tx_id, Ordering::Release);
                }
            } else {
                self.pipeline_mode.wait_strategy(retry_count);
                retry_count += 1;
            }
        }
    }
}
