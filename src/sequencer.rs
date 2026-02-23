use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType};
use crossbeam_queue::ArrayQueue;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

    #[inline(always)]
    pub fn submit(&self, mut transaction: Transaction<Data, BalanceData>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        transaction.id = id;

        while let Err(t) = self.outbound.push(transaction) {
            transaction = t;
            spin_loop()
        }

        transaction.id
    }
}
