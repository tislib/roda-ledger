use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct MixedWorkloadScenario {
    pub duration: Duration,
    pub accounts: u64,
}

impl MixedWorkloadScenario {
    pub fn new(duration: Duration, accounts: u64) -> Self {
        Self { duration, accounts }
    }
}

impl Scenario for MixedWorkloadScenario {
    fn name(&self) -> String {
        "mixed_workload".to_string()
    }

    fn duration(&self) -> Duration {
        self.duration
    }

    fn execute(
        &self,
        client: DirectWorkloadClient,
        metrics: Arc<WorkloadMetrics>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let accounts: Vec<u64> = (0..self.accounts).collect();
        workload = workload.with_accounts(accounts.clone());

        let duration = self.duration;
        let accounts_ref = accounts;

        let workload_handle = std::thread::spawn(move || {
            let config = RunConfig {
                limit: Limit::Duration(duration),
                power: Power::Full,
            };

            let _ = workload.run(config, |idx| {
                let from_idx = (idx % accounts_ref.len() as u64) as usize;
                let to_idx = ((idx + 1) % accounts_ref.len() as u64) as usize;

                if idx % 10 < 7 {
                    // 70% deposits
                    WalletTransaction::deposit(accounts_ref[from_idx], 100)
                } else if idx % 10 < 9 {
                    // 20% transfers
                    WalletTransaction::transfer(accounts_ref[from_idx], accounts_ref[to_idx], 50)
                } else {
                    // 10% withdrawals
                    WalletTransaction::withdraw(accounts_ref[from_idx], 10)
                }
            });
        });

        Ok(workload_handle)
    }
}
