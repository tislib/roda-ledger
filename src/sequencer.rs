use crate::transaction::{Transaction, TransactionDataType};
use crossbeam_queue::ArrayQueue;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Sequencer<Data: TransactionDataType> {
    next_id: AtomicU64,
    outbound: Arc<ArrayQueue<Transaction<Data>>>,
}

impl<Data: TransactionDataType> Sequencer<Data> {
    pub fn new(outbound: Arc<ArrayQueue<Transaction<Data>>>) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            outbound,
        }
    }

    #[inline(always)]
    pub fn submit(&self, mut transaction: Transaction<Data>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        transaction.id = id;

        while let Err(t) = self.outbound.push(transaction) {
            transaction = t;
            spin_loop()
        }

        transaction.id
    }

    pub(crate) fn set_next_id(&self, next_id: u64) {
        self.next_id.store(next_id, Ordering::Relaxed);
    }
}
