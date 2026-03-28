use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct AccountScaleScenario {
    pub duration: Duration,
    pub max_accounts: u64,
}

impl AccountScaleScenario {
    pub fn new(duration: Duration, max_accounts: u64) -> Self {
        Self {
            duration,
            max_accounts,
        }
    }
}

impl Scenario for AccountScaleScenario {
    fn name(&self) -> String {
        "account_scale".to_string()
    }

    fn duration(&self) -> Duration {
        self.duration
    }

    fn max_accounts(&self) -> u64 {
        self.max_accounts
    }

    fn execute(
        &self,
        client: DirectWorkloadClient,
        metrics: Arc<WorkloadMetrics>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let max_accounts = self.max_accounts;
        let duration = self.duration;

        let workload_handle = std::thread::spawn(move || {
            let config = RunConfig {
                limit: Limit::Duration(duration),
                power: Power::Full,
            };

            let _ = workload.run(config, |_| {
                let account_id = rand::random::<u64>() % max_accounts;
                WalletTransaction::deposit(account_id, 100)
            });
        });

        Ok(workload_handle)
    }
}
