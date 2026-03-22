use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct SpikeRecoveryScenario {
    pub duration: Duration,
    pub accounts: u64,
}

impl SpikeRecoveryScenario {
    pub fn new(duration: Duration, accounts: u64) -> Self {
        Self {
            duration,
            accounts,
        }
    }
}

impl Scenario for SpikeRecoveryScenario {
    fn name(&self) -> String {
        "spike_recovery".to_string()
    }

    fn duration(&self) -> Duration {
        self.duration
    }

    fn execute(&self, client: DirectWorkloadClient, metrics: Arc<WorkloadMetrics>) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let accounts: Vec<u64> = (0..self.accounts).collect();
        workload = workload.with_accounts(accounts.clone());

        let duration = self.duration;
        let accounts_ref = accounts;

        let workload_handle = std::thread::spawn(move || {
            let half_duration = duration / 2;
            
            // Spike phase: Full power
            let spike_config = RunConfig {
                limit: Limit::Duration(half_duration),
                power: Power::Full,
            };
            
            let (res, count) = workload.run_step(
                spike_config,
                |idx| WalletTransaction::deposit(accounts_ref[idx as usize % accounts_ref.len()], 100),
                0,
            ).unwrap();
            
            if res.is_err() { return; }

            // Recovery phase: Moderate rate
            let recovery_config = RunConfig {
                limit: Limit::Duration(half_duration),
                power: Power::Rate(10_000), // 10k TPS
            };

            let _ = workload.run_step(
                recovery_config,
                |idx| WalletTransaction::deposit(accounts_ref[idx as usize % accounts_ref.len()], 100),
                count,
            );
        });

        Ok(workload_handle)
    }
}
