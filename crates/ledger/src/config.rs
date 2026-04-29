//! Centralized configuration for the ledger and its storage backend.
//!
//! All config structs live here so that individual stages can take a borrowed
//! `&LedgerConfig` and pull out whatever they need, instead of receiving a
//! long list of primitive arguments from the caller.

use crate::wait_strategy::WaitStrategy;
use serde::Deserialize;
use spdlog::Level;
use std::time::Duration;
pub use storage::StorageConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct LedgerConfig {
    pub max_accounts: usize,
    pub wait_strategy: WaitStrategy,
    pub storage: StorageConfig,

    // ---- Fields not exposed via config.toml (internal tuning / runtime) ----
    #[serde(skip)]
    pub queue_size: usize,
    #[serde(skip, default = "default_log_level")]
    pub log_level: Level,
    #[serde(skip, default = "default_seal_check_internal")]
    pub seal_check_internal: Duration,
    #[serde(skip)]
    pub disable_seal: bool,
}

fn default_log_level() -> Level {
    Level::Info
}
fn default_seal_check_internal() -> Duration {
    Duration::from_secs(1)
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            max_accounts: 1_000_000,
            queue_size: 1 << 14,
            storage: StorageConfig::default(),
            wait_strategy: WaitStrategy::Balanced,
            log_level: default_log_level(),
            seal_check_internal: default_seal_check_internal(),
            disable_seal: false,
        }
    }
}

impl LedgerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index circle 1 size: covers the active segment (one active window).
    /// Rounded up to the next power of two as required by `TransactionIndexer`.
    pub fn index_circle1_size(&self) -> usize {
        (self.storage.transaction_count_per_segment as usize).next_power_of_two()
    }

    /// Index circle 2 size: covers active + previous segment (matches dedup window).
    /// Rounded up to the next power of two as required by `TransactionIndexer`.
    pub fn index_circle2_size(&self) -> usize {
        (self.storage.transaction_count_per_segment as usize * 2).next_power_of_two()
    }

    pub fn temp() -> Self {
        let mut dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        dir.push(format!("temp_{}", rand));

        Self {
            storage: StorageConfig {
                data_dir: dir.to_string_lossy().to_string(),
                temporary: true,
                snapshot_frequency: 2,
                transaction_count_per_segment: 10_000_000,
            },
            log_level: Level::Critical,
            seal_check_internal: Duration::from_millis(10),
            ..Default::default()
        }
    }

    pub fn bench() -> Self {
        let mut dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        dir.push(format!("temp_{}", rand));

        Self {
            storage: StorageConfig {
                data_dir: dir.to_string_lossy().to_string(),
                temporary: true,
                snapshot_frequency: u32::MAX,
                transaction_count_per_segment: 10_000_000,
            },
            log_level: Level::Critical,
            seal_check_internal: Duration::from_mins(10),
            // disable seal to avoid unnecessary disk IO during benchmarks
            disable_seal: true,
            ..Default::default()
        }
    }
}
