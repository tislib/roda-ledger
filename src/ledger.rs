use crate::balance::BalanceDataType;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::transaction::{Transaction, TransactionDataType};
use crate::transactor::Transactor;
use crate::wal::Wal;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub struct Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    sequencer: Sequencer<Data, BalanceData>,
    transactor: Transactor<Data, BalanceData>,
    wal: Wal<Data, BalanceData>,
    snapshot: Snapshot<Data, BalanceData>,
    thread_join_handles: Vec<std::thread::JoinHandle<()>>,
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
            thread_join_handles: vec![],
        }
    }

    pub fn submit(&self, transaction: Transaction<Data, BalanceData>) -> u64 {
        self.sequencer.submit(transaction)
    }

    pub(crate) fn get_balance(&self, account_id: u64) -> BalanceData {
        self.snapshot.get_balance(account_id)
    }

    pub fn get_balance_at(
        &self,
        account_id: u64,
        transaction_id: u64,
    ) -> Result<BalanceData, String> {
        self.snapshot.get_balance_at(account_id, transaction_id)
    }

    pub(crate) fn tick(&mut self, tick_count: i32) {
        for _ in 0..tick_count {
            self.transactor.tick();
            self.wal.tick();
            self.snapshot.tick();
        }
    }

    pub fn start(&mut self) {
        self.transactor.start();
        self.wal.start();
        self.snapshot.start();
    }
}

#[cfg(test)]
mod tests {

    use crate::balance::BalanceDataType;
    use crate::transaction::{TransactionDataType, TransactionExecutionContext};
    use bytemuck::{Pod, Zeroable};

    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable, Default)]
    struct MockBalance(u64);
    impl BalanceDataType for MockBalance {}

    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
    struct MockTransactionData(u64);
    impl TransactionDataType for MockTransactionData {
        type BalanceData = MockBalance;
        fn process(
            &self,
            ctx: &mut impl TransactionExecutionContext<Self::BalanceData>,
        ) -> Result<(), String> {
            let balance = ctx.get_balance(1);
            ctx.update_balance(1, MockBalance(balance.0 + self.0));
            Ok(())
        }
    }

    use crate::ledger::Ledger;
    use crate::transaction::Transaction;

    #[test]
    fn test_ledger_point_in_time() {
        let mut ledger = Ledger::<MockTransactionData, MockBalance>::new(10, None);
        ledger.start();

        // Submit transaction with ID 1 (assigned by sequencer)
        ledger.submit(Transaction::new(MockTransactionData(100)));
        ledger.tick(1);

        // Submit transaction with ID 2
        ledger.submit(Transaction::new(MockTransactionData(50)));
        ledger.tick(1);

        // Final balance should be 150
        assert_eq!(ledger.get_balance(1).0, 150);

        // Balance at transaction ID 1 should be 100
        assert_eq!(ledger.get_balance_at(1, 1).unwrap().0, 100);

        // Balance at transaction ID 2 should be 150
        assert_eq!(ledger.get_balance_at(1, 2).unwrap().0, 150);

        // Balance before transaction ID 1 should fail
        assert!(ledger.get_balance_at(1, 0).is_err());
    }
}
