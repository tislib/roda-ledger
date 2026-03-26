use crate::ledger::Ledger;
use crate::testing::stress::workload::{AccountSelector, RunConfig, Workload, WorkloadClient};
use crate::transaction::Transaction;
use crate::balance::Balance;
use crate::wallet::transaction::WalletTransaction;
use std::sync::Arc;

pub struct DirectWorkloadClient {
    ledger: Arc<Ledger<WalletTransaction>>,
}

impl DirectWorkloadClient {
    pub fn new(ledger: Arc<Ledger<WalletTransaction>>) -> Self {
        Self { ledger }
    }
}

impl WorkloadClient for DirectWorkloadClient {
    fn submit(&self, tx: Transaction<WalletTransaction>) {
        self.ledger.submit(tx);
    }
}

impl<C> Workload<C>
where
    C: WorkloadClient,
{
    pub fn deposit(
        &mut self,
        selector: AccountSelector,
        amount: u64,
        config: RunConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let all_accounts = self.all_accounts.clone();
        self.run(config, move |idx| {
            let account_id = selector.select(idx, &all_accounts);
            WalletTransaction::deposit(account_id, amount)
        })
    }

    pub fn transfer(
        &mut self,
        from_selector: AccountSelector,
        to_selector: AccountSelector,
        amount: u64,
        config: RunConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let all_accounts = self.all_accounts.clone();
        self.run(config, move |idx| {
            let from = from_selector.select(idx, &all_accounts);
            let to = to_selector.select(idx + 1, &all_accounts);
            WalletTransaction::transfer(from, to, amount)
        })
    }
}
