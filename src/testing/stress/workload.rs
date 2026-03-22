use crate::transaction::Transaction;
use crate::wallet::balance::WalletBalance;
use crate::wallet::transaction::WalletTransaction;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    fn submit(&self, tx: Transaction<WalletTransaction, WalletBalance>);
}

/// Workload is designed to be deterministic and reproducible. It uses round-robin
/// account selection instead of random selection to ensure that tests are stable.
pub struct Workload<C>
where
    C: WorkloadClient,
{
    pub client: C,
    pub all_accounts: Vec<u64>,
}

impl<C> Workload<C>
where
    C: WorkloadClient,
{
    pub fn new(client: C) -> Self {
        Self {
            client,
            all_accounts: Vec::new(),
        }
    }

    pub fn with_accounts(mut self, accounts: Vec<u64>) -> Self {
        self.all_accounts = accounts;
        self
    }

    pub fn sustain_load<F>(
        &mut self,
        operation: F,
        duration: Duration,
        rate: u64,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(u64) -> WalletTransaction + Send + 'static,
    {
        let config = RunConfig {
            limit: Limit::Duration(duration),
            power: Power::Rate(rate),
        };
        self.run_internal(config, operation)
    }

    pub fn peak<F>(
        &mut self,
        operation: F,
        duration: Duration,
        peak_rate: u64,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(u64) -> WalletTransaction + Send + Sync + 'static,
    {
        let start_time = Instant::now();
        let interval = Duration::from_millis(100); // Update rate every 100ms
        let total_ticks = duration.as_secs_f64() / interval.as_secs_f64();

        let operation = Arc::new(operation);
        let mut total_count = 0;

        for i in 0..(total_ticks as u64) {
            let elapsed = start_time.elapsed();
            if elapsed >= duration {
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

            let op = Arc::clone(&operation);
            let offset = total_count;
            let (res, count) = self.run_internal_with_offset(config, move |idx| op(idx), offset)?;
            total_count += count;
            if res.is_err() {
                return res.map(|_| ());
            }
        }

        Ok(())
    }

    pub fn spike<F>(
        &mut self,
        operation: F,
        duration: Duration,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(u64) -> WalletTransaction + Send + 'static,
    {
        let config = RunConfig {
            limit: Limit::Duration(duration),
            power: Power::Full,
        };
        self.run_internal(config, operation)
    }

    pub fn run_internal<F>(
        &mut self,
        config: RunConfig,
        operation_gen: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(u64) -> WalletTransaction + Send + 'static,
    {
        self.run_internal_with_offset(config, operation_gen, 0).map(|_| ())
    }

    pub fn run_internal_with_offset<F>(
        &mut self,
        config: RunConfig,
        operation_gen: F,
        offset: u64,
    ) -> Result<(Result<(), Box<dyn std::error::Error>>, u64), Box<dyn std::error::Error>>
    where
        F: Fn(u64) -> WalletTransaction + Send + 'static,
    {
        let start_time = Instant::now();
        let mut count = 0;

        match config.power {
            Power::Full => {
                loop {
                    if let Limit::Count(limit_count) = config.limit {
                        if count >= limit_count {
                            break;
                        }
                    }
                    if let Limit::Duration(limit_duration) = config.limit {
                        if start_time.elapsed() >= limit_duration {
                            break;
                        }
                    }

                    let tx_data = operation_gen(offset + count);
                    let tx = Transaction::new(tx_data);
                    self.client.submit(tx);
                    count += 1;
                }
            }
            Power::Rate(rate) => {
                let interval = Duration::from_secs_f64(1.0 / rate as f64);
                let mut next_tick = start_time;

                loop {
                    if let Limit::Count(limit_count) = config.limit {
                        if count >= limit_count {
                            break;
                        }
                    }
                    if let Limit::Duration(limit_duration) = config.limit {
                        if start_time.elapsed() >= limit_duration {
                            break;
                        }
                    }

                    let tx_data = operation_gen(offset + count);
                    let tx = Transaction::new(tx_data);
                    self.client.submit(tx);
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
