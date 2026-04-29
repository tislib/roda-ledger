use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::transaction::Operation;
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
        Self { duration, accounts }
    }
}

impl Scenario for SpikeRecoveryScenario {
    fn name(&self) -> String {
        "spike_recovery".to_string()
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
            let half_duration = duration / 2;

            // Spike phase: Full power
            let spike_config = RunConfig {
                limit: Limit::Duration(half_duration),
                power: Power::Full,
            };

            let (res, count) = workload
                .run_step(
                    spike_config,
                    |_| {
                        let account_id = rand::random::<u64>() % accounts_size;
                        Operation::Deposit {
                            account: account_id,
                            amount: 100,
                            user_ref: 0,
                        }
                    },
                    0,
                )
                .unwrap();

            if res.is_err() {
                return;
            }

            // Recovery phase: Moderate rate
            let recovery_config = RunConfig {
                limit: Limit::Duration(half_duration),
                power: Power::Rate(10_000), // 10k TPS
            };

            let _ = workload.run_step(
                recovery_config,
                |_| {
                    let account_id = rand::random::<u64>() % accounts_size;
                    Operation::Deposit {
                        account: account_id,
                        amount: 100,
                        user_ref: 0,
                    }
                },
                count,
            );
        });

        Ok(workload_handle)
    }
}
