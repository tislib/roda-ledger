use crate::testing::reporting::WorkloadMetrics;
use crate::transaction::Operation;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type DynError = Box<dyn Error>;
pub type StepOutcome = Result<(), DynError>;
pub type RunStepResult = Result<(StepOutcome, u64), DynError>;

pub enum Limit {
    Count(u64),
    Duration(Duration),
}

pub enum Power {
    Rate(u64), // Transactions per second
    Full,
}

pub struct RunConfig {
    pub limit: Limit,
    pub power: Power,
}

pub trait WorkloadClient: Send + Sync {
    fn submit(&self, operation: Operation);
}

/// Workload is designed to execute transactions according to the given configuration.
pub struct Workload<C>
where
    C: WorkloadClient,
{
    pub client: C,
    pub metrics: Option<Arc<WorkloadMetrics>>,
}

impl<C> Workload<C>
where
    C: WorkloadClient,
{
    pub fn new(client: C) -> Self {
        Self {
            client,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<WorkloadMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Runs a workload with the given configuration and operation generator.
    pub fn run<F>(
        &mut self,
        config: RunConfig,
        operation_gen: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(u64) -> Operation + Send,
    {
        self.run_step(config, operation_gen, 0).map(|_| ())
    }

    /// Runs a single step of a workload, allowing for offset management in sequences.
    pub fn run_step<F>(
        &mut self,
        config: RunConfig,
        mut operation_gen: F,
        offset: u64,
    ) -> RunStepResult
    where
        F: FnMut(u64) -> Operation + Send,
    {
        let start_time = Instant::now();
        let mut count = 0;

        match config.power {
            Power::Full => loop {
                if let Limit::Count(limit_count) = config.limit
                    && count >= limit_count
                {
                    break;
                }
                if let Limit::Duration(limit_duration) = config.limit
                    && start_time.elapsed() >= limit_duration
                {
                    break;
                }

                let operation = operation_gen(offset + count);

                let start = Instant::now();
                self.client.submit(operation);
                let elapsed = start.elapsed();

                if let Some(metrics) = &self.metrics {
                    metrics.record(elapsed);
                }

                count += 1;
            },
            Power::Rate(rate) => {
                let interval = Duration::from_secs_f64(1.0 / rate as f64);
                let mut next_tick = start_time;

                loop {
                    if let Limit::Count(limit_count) = config.limit
                        && count >= limit_count
                    {
                        break;
                    }
                    if let Limit::Duration(limit_duration) = config.limit
                        && start_time.elapsed() >= limit_duration
                    {
                        break;
                    }

                    let operation = operation_gen(offset + count);

                    let start = Instant::now();
                    self.client.submit(operation);
                    let elapsed = start.elapsed();

                    if let Some(metrics) = &self.metrics {
                        metrics.record(elapsed);
                    }

                    count += 1;

                    next_tick += interval;
                    let now = Instant::now();
                    if next_tick > now {
                        std::thread::sleep(next_tick - now);
                    }
                }
            }
        }

        Ok((Ok(()), count))
    }
}
