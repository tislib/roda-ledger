use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct SustainLoadScenario {
    pub duration: Duration,
    pub rate: u64,
    pub accounts: u64,
}

impl SustainLoadScenario {
    pub fn new(duration: Duration, rate: u64, accounts: u64) -> Self {
        Self {
            duration,
            rate,
            accounts,
        }
    }
}

impl Scenario for SustainLoadScenario {
    fn name(&self) -> String {
        "sustain_load".to_string()
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

        let config = RunConfig {
            limit: Limit::Duration(self.duration),
            power: Power::Rate(self.rate),
        };

        // Run the workload in a separate thread
        let workload_handle = std::thread::spawn(move || {
            let _ = workload.run(config, move |_| {
                let account_id = rand::random::<u64>() % accounts_size;
                WalletTransaction::deposit(account_id, 100)
            });
        });

        Ok(workload_handle)
    }
}
