use crate::balance::BalanceDataType;
use crate::sequencer::Sequencer;
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crate::transactor::Transactor;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub struct Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    sequencer: Sequencer<Data, BalanceData>,
    transactor: Transactor<Data, BalanceData>,
}

impl<Data, BalanceData> Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub(crate) fn get_balance(&self, p0: u64) -> Result<BalanceData, String> {
        todo!()
    }
}

impl<Data, BalanceData> Ledger<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(capacity: usize) -> Self {
        let sequencer_transactor_queue = Arc::new(ArrayQueue::new(capacity));
        let transactor_wal_queue = Arc::new(ArrayQueue::new(capacity));
        Self {
            sequencer: Sequencer::new(sequencer_transactor_queue.clone()),
            transactor: Transactor::new(sequencer_transactor_queue, transactor_wal_queue.clone()),
        }
    }

    pub fn submit(&self, transaction: Transaction<Data, BalanceData>) {
        self.sequencer.submit(transaction);
    }
}

#[cfg(test)]
mod tests {

    use crate::balance::BalanceDataType;
    use crate::transaction::{TransactionDataType, TransactionExecutionContext};

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct MockBalance(u64);
    impl BalanceDataType for MockBalance {}

    struct MockTransactionData(u64);
    impl TransactionDataType for MockTransactionData {
        type BalanceData = MockBalance;
        fn process(
            &self,
            ctx: &impl TransactionExecutionContext<Self::BalanceData>,
        ) -> Result<(), String> {
            let balance = ctx.get_balance(1).unwrap_or(MockBalance(0));
            ctx.update_balance(1, MockBalance(balance.0 + self.0))?;
            Ok(())
        }
    }
}
