use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::transaction::Operation;
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

    fn max_accounts(&self) -> u64 {
        self.accounts
    }

    fn execute(
        &self,
        client: DirectWorkloadClient,
        metrics: Arc<WorkloadMetrics>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let accounts_size = self.accounts;
        let duration = self.duration;

        let workload_handle = std::thread::spawn(move || {
            let config = RunConfig {
                limit: Limit::Duration(duration),
                power: Power::Full,
            };

            let _ = workload.run(config, |_| {
                let account_id = rand::random::<u64>() % accounts_size;
                Operation::Deposit {
                    account: account_id,
                    amount: 1,
                    user_ref: 0,
                }
            });
        });

        Ok(workload_handle)
    }
}
