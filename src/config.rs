//! Centralized configuration for the ledger and its storage backend.
//!
//! All config structs live here so that individual stages can take a borrowed
//! `&LedgerConfig` and pull out whatever they need, instead of receiving a
//! long list of primitive arguments from the caller.

use crate::wait_strategy::WaitStrategy;
use serde::Deserialize;
use spdlog::Level;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    /// Whether the data directory is a temp dir that should be removed on drop.
    /// Not part of config.toml — only set programmatically (e.g. `LedgerConfig::temp`).
    #[serde(skip)]
    pub temporary: bool,
    pub wal_segment_size_mb: u64,
    /// How often (in sealed segments) to write a snapshot. 0 = disabled.
    pub snapshot_frequency: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data/".to_string(),
            temporary: false,
            wal_segment_size_mb: 64,
            snapshot_frequency: 4,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct LedgerConfig {
    pub max_accounts: usize,
    pub wait_strategy: WaitStrategy,
    pub dedup_enabled: bool,
    pub dedup_window_ms: u64,
    pub storage: StorageConfig,

    // ---- Fields not exposed via config.toml (internal tuning / runtime) ----
    #[serde(skip)]
    pub queue_size: usize,
    #[serde(skip, default = "default_log_level")]
    pub log_level: Level,
    #[serde(skip, default = "default_seal_check_internal")]
    pub seal_check_internal: Duration,
    #[serde(skip, default = "default_index_circle1_size")]
    pub index_circle1_size: usize,
    #[serde(skip, default = "default_index_circle2_size")]
    pub index_circle2_size: usize,
    #[serde(skip)]
    pub disable_seal: bool,
}

fn default_log_level() -> Level {
    Level::Info
}
fn default_seal_check_internal() -> Duration {
    Duration::from_secs(1)
}
fn default_index_circle1_size() -> usize {
    1 << 20
}
fn default_index_circle2_size() -> usize {
    1 << 21
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            max_accounts: 1_000_000,
            queue_size: 1024,
            storage: StorageConfig::default(),
            wait_strategy: WaitStrategy::Balanced,
            log_level: default_log_level(),
            seal_check_internal: default_seal_check_internal(),
            index_circle1_size: default_index_circle1_size(),
            index_circle2_size: default_index_circle2_size(),
            dedup_enabled: true,
            dedup_window_ms: 10_000,
            disable_seal: false,
        }
    }
}

impl LedgerConfig {
    pub fn new() -> Self {
        Self::default()
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
                wal_segment_size_mb: 2048,
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
                wal_segment_size_mb: 2048,
            },
            log_level: Level::Critical,
            seal_check_internal: Duration::from_mins(10),
            // disable seal to avoid unnecessary disk IO during benchmarks
            disable_seal: true,
            ..Default::default()
        }
    }
}
