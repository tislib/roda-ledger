use crate::testing::reporting::WorkloadMetrics;
use crate::transaction::Transaction;
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type DynError = Box<dyn Error>;
pub type StepOutcome = Result<(), DynError>;
pub type RunStepResult = Result<(StepOutcome, u64), DynError>;

pub enum AccountSelector {
    Single(u64),
    Multi(Vec<u64>),
    Range { start: u64, end: u64 },
    All,
}

impl AccountSelector {
    pub fn get_accounts(&self, all_accounts: &[u64]) -> Vec<u64> {
        match self {
            AccountSelector::Single(id) => vec![*id],
            AccountSelector::Multi(ids) => ids.clone(),
            AccountSelector::Range { start, end } => (*start..=*end).collect(),
            AccountSelector::All => all_accounts.to_vec(),
        }
    }

    /// Selects an account in a deterministic way using the given index for round-robin selection.
    /// This ensures that the workload is reproducible and avoids randomness-related issues.
    pub fn select(&self, index: u64, all_accounts: &[u64]) -> u64 {
        match self {
            AccountSelector::Single(id) => *id,
            AccountSelector::Multi(ids) => {
                if ids.is_empty() {
                    panic!("No accounts available in Multi selector");
                }
                ids[(index as usize) % ids.len()]
            }
            AccountSelector::Range { start, end } => {
                let range = end - start + 1;
                start + (index % range)
            }
            AccountSelector::All => {
                if all_accounts.is_empty() {
                    panic!("No accounts available for selection in All selector");
                }
                all_accounts[(index as usize) % all_accounts.len()]
            }
        }
    }
}

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
    fn submit(&self, tx: Transaction<WalletTransaction>);
}

/// Workload is designed to be deterministic and reproducible. It uses round-robin
/// account selection instead of random selection to ensure that tests are stable.
pub struct Workload<C>
where
    C: WorkloadClient,
{
    pub client: C,
    pub all_accounts: Vec<u64>,
    pub metrics: Option<Arc<WorkloadMetrics>>,
}

impl<C> Workload<C>
where
    C: WorkloadClient,
{
    pub fn new(client: C) -> Self {
        Self {
            client,
            all_accounts: Vec::new(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<WorkloadMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_accounts(mut self, accounts: Vec<u64>) -> Self {
        self.all_accounts = accounts;
        self
    }

    /// Runs a workload with the given configuration and operation generator.
    pub fn run<F>(
        &mut self,
        config: RunConfig,
        operation_gen: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(u64) -> WalletTransaction + Send,
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
        F: FnMut(u64) -> WalletTransaction + Send,
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

                let tx_data = operation_gen(offset + count);
                let tx = Transaction::new(tx_data);

                let start = Instant::now();
                self.client.submit(tx);
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

                    let tx_data = operation_gen(offset + count);
                    let tx = Transaction::new(tx_data);

                    let start = Instant::now();
                    self.client.submit(tx);
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
