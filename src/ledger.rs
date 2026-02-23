use crate::balance::BalanceDataType;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::transaction::{Transaction, TransactionDataType};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::thread::yield_now;

pub struct Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    sequencer: Sequencer<Data, BalanceData>,
    transactor: Transactor<Data, BalanceData>,
    wal: Wal<Data, BalanceData>,
    snapshot: Snapshot<Data, BalanceData>,
}

impl<Data, BalanceData> Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(capacity: usize, ledger_folder: Option<&str>) -> Self {
        let sequencer_transactor_queue = Arc::new(ArrayQueue::new(capacity));
        let transactor_wal_queue = Arc::new(ArrayQueue::new(capacity));
        let wal_snapshot_queue = Arc::new(ArrayQueue::new(capacity));
        Self {
            sequencer: Sequencer::new(sequencer_transactor_queue.clone()),
            transactor: Transactor::new(sequencer_transactor_queue, transactor_wal_queue.clone()),
            wal: Wal::new(
                transactor_wal_queue,
                wal_snapshot_queue.clone(),
                ledger_folder,
            ),
            snapshot: Snapshot::new(wal_snapshot_queue),
        }
    }

    pub fn submit(&self, transaction: Transaction<Data, BalanceData>) -> u64 {
        self.sequencer.submit(transaction)
    }

    pub(crate) fn get_balance(&self, account_id: u64) -> BalanceData {
        self.snapshot.get_balance(account_id)
    }

    pub(crate) fn tick(&mut self, tick_count: u64) {
        for _ in 0..1_000_000 {
            let rejected_count = self.transactor.rejected_count();

            let mut target_step = tick_count;
            if rejected_count > tick_count {
                return;
            }
            target_step -= rejected_count;

            if self.transactor.step() >= target_step
                && self.wal.step() >= target_step
                && self.snapshot.step() >= target_step
            {
                return;
            }
            yield_now();
        }
    }

    pub fn start(&mut self) {
        self.transactor.start();
        self.wal.start();
        self.snapshot.start();
    }
}
