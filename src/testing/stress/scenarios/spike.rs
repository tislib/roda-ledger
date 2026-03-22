use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct SpikeScenario {
    pub duration: Duration,
    pub accounts: u64,
}

impl SpikeScenario {
    pub fn new(duration: Duration, accounts: u64) -> Self {
        Self { duration, accounts }
    }
}

impl Scenario for SpikeScenario {
    fn name(&self) -> String {
        "spike".to_string()
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

        let config = RunConfig {
            limit: Limit::Duration(self.duration),
            power: Power::Full,
        };

        let workload_handle = std::thread::spawn(move || {
            let _ = workload.run(config, move |idx| {
                WalletTransaction::deposit(accounts[idx as usize % accounts.len()], 100)
            });
        });

        Ok(workload_handle)
    }
}
