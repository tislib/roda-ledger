use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam_queue::ArrayQueue;
use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType};

pub struct Sequencer<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    next_id: AtomicU64,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
}

impl<Data, BalanceData> Sequencer<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            outbound,
        }
    }

    pub fn submit(&self, mut transaction: Transaction<Data, BalanceData>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        transaction.id = id;

        while let Err(t) = self.outbound.push(transaction) {
            transaction = t;
            // busy loop
        }
    }
}