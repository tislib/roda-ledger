use crate::ledger::Ledger;
use crate::testing::stress::workload::WorkloadClient;
use crate::transaction::Transaction;
use crate::wallet::transaction::WalletTransaction;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

pub struct DirectWorkloadClient {
    ledger: Arc<Ledger<WalletTransaction>>,
    step: AtomicU64,
}

impl DirectWorkloadClient {
    pub fn new(ledger: Arc<Ledger<WalletTransaction>>) -> Self {
        Self {
            ledger,
            step: AtomicU64::new(0),
        }
    }
}

impl WorkloadClient for DirectWorkloadClient {
    fn submit(&self, tx: Transaction<WalletTransaction>) {
        self.ledger.submit(tx);
        self.step.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self
            .step
            .load(std::sync::atomic::Ordering::Relaxed)
            .is_multiple_of(10000)
            && self.ledger.get_rejected_count() > 0
        {
            panic!(
                "Ledger rejected transactions: {}",
                self.ledger.get_rejected_count()
            );
        }
    }
}
