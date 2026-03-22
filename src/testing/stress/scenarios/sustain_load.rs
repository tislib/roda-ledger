use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::Workload;
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

    fn execute(&self, client: DirectWorkloadClient, metrics: Arc<WorkloadMetrics>) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let accounts: Vec<u64> = (0..self.accounts).collect();
        workload = workload.with_accounts(accounts.clone());

        let scenario_duration = self.duration;
        let rate = self.rate;

        // Run workload in a separate thread
        let workload_handle = std::thread::spawn(move || {
            let _ = workload.sustain_load(
                move |idx| WalletTransaction::deposit(accounts[idx as usize % accounts.len()], 100),
                scenario_duration,
                rate,
            );
        });

        Ok(workload_handle)
    }
}
