use crate::transaction::Transaction;
use crossbeam_queue::ArrayQueue;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::yield_now;

pub struct Sequencer {
    next_id: AtomicU64,
    outbound: Arc<ArrayQueue<Transaction>>,
}

impl Sequencer {
    pub fn new(outbound: Arc<ArrayQueue<Transaction>>) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            outbound,
        }
    }

    #[inline(always)]
    pub fn submit(&self, mut transaction: Transaction) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Acquire);
        transaction.id = id;

        let mut retry_count = 0;
        while let Err(t) = self.outbound.push(transaction) {
            transaction = t;
            spin_loop();
            retry_count += 1;
            if retry_count % 10_000 == 0 {
                yield_now();
            }
        }

        id
    }

    pub(crate) fn set_next_id(&self, next_id: u64) {
        self.next_id.store(next_id, Ordering::Release);
    }

    pub(crate) fn last_id(&self) -> u64 {
        self.next_id.load(Ordering::Acquire) - 1
    }
}
