use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::transaction::Operation;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct LoadRampScenario {
    pub duration: Duration,
    pub start_rate: u64,
    pub end_rate: u64,
    pub accounts: u64,
}

impl LoadRampScenario {
    pub fn new(duration: Duration, start_rate: u64, end_rate: u64, accounts: u64) -> Self {
        Self {
            duration,
            start_rate,
            end_rate,
            accounts,
        }
    }
}

impl Scenario for LoadRampScenario {
    fn name(&self) -> String {
        "load_ramp".to_string()
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
        let start_rate = self.start_rate;
        let end_rate = self.end_rate;

        // Run workload in a separate thread
        let workload_handle = std::thread::spawn(move || {
            let start_time = Instant::now();
            let interval = Duration::from_millis(100); // Update rate every 100ms
            let total_ticks = (duration.as_secs_f64() / interval.as_secs_f64()).ceil() as u64;

            let mut total_count = 0;

            for i in 0..total_ticks {
                if start_time.elapsed() >= duration {
                    break;
                }

                let progress = i as f64 / total_ticks as f64;
                let current_rate =
                    (start_rate as f64 + (end_rate as f64 - start_rate as f64) * progress) as u64;

                let config = RunConfig {
                    limit: Limit::Duration(interval),
                    power: Power::Rate(current_rate.max(1)),
                };

                let (res, count) = workload
                    .run_step(
                        config,
                        |_| {
                            let account_id = rand::random::<u64>() % accounts_size;
                            Operation::Deposit {
                                account: account_id,
                                amount: 100,
                                user_ref: 0,
                            }
                        },
                        total_count,
                    )
                    .unwrap();

                total_count += count;
                if res.is_err() {
                    break;
                }
            }
        });

        Ok(workload_handle)
    }
}
