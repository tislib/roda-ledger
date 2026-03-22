use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct HotAccountContentionScenario {
    pub duration: Duration,
    pub accounts: u64,
}

impl HotAccountContentionScenario {
    pub fn new(duration: Duration, accounts: u64) -> Self {
        Self { duration, accounts }
    }
}

impl Scenario for HotAccountContentionScenario {
    fn name(&self) -> String {
        "hot_account_contention".to_string()
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
                let hot_account_idx = 0;
                let other_account_idx = (1 + (idx % (accounts_ref.len() as u64 - 1))) as usize;

                if idx % 2 == 0 {
                    // All transfers TO the hot account
                    WalletTransaction::transfer(
                        accounts_ref[other_account_idx],
                        accounts_ref[hot_account_idx],
                        1,
                    )
                } else {
                    // All transfers FROM the hot account
                    WalletTransaction::transfer(
                        accounts_ref[hot_account_idx],
                        accounts_ref[other_account_idx],
                        1,
                    )
                }
            });
        });

        Ok(workload_handle)
    }
}
