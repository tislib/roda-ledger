use crate::testing::reporting::WorkloadMetrics;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct PeakScenario {
    pub duration: Duration,
    pub peak_rate: u64,
    pub accounts: u64,
}

impl PeakScenario {
    pub fn new(duration: Duration, peak_rate: u64, accounts: u64) -> Self {
        Self {
            duration,
            peak_rate,
            accounts,
        }
    }
}

impl Scenario for PeakScenario {
    fn name(&self) -> String {
        "peak".to_string()
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
        let peak_rate = self.peak_rate;

        let workload_handle = std::thread::spawn(move || {
            let start_time = Instant::now();
            let interval = Duration::from_millis(100); // Update rate every 100ms
            let total_ticks = duration.as_secs_f64() / interval.as_secs_f64();

            let mut total_count = 0;

            for i in 0..(total_ticks as u64) {
                if start_time.elapsed() >= duration {
                    break;
                }

                // Linear ramp-up and ramp-down
                let progress = i as f64 / total_ticks;
                let current_rate = if progress < 0.5 {
                    (progress * 2.0 * peak_rate as f64) as u64
                } else {
                    ((1.0 - progress) * 2.0 * peak_rate as f64) as u64
                };

                let config = RunConfig {
                    limit: Limit::Duration(interval),
                    power: Power::Rate(current_rate.max(1)),
                };

                let accounts_ref = &accounts;
                let (res, count) = workload
                    .run_step(
                        config,
                        move |idx| {
                            WalletTransaction::deposit(
                                accounts_ref[idx as usize % accounts_ref.len()],
                                100,
                            )
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
