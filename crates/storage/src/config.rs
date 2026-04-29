use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    /// Whether the data directory is a temp dir that should be removed on drop.
    /// Not part of config.toml — only set programmatically (e.g. `LedgerConfig::temp`).
    #[serde(skip)]
    pub temporary: bool,
    pub transaction_count_per_segment: u64,
    /// How often (in sealed segments) to write a snapshot. 0 = disabled.
    pub snapshot_frequency: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data/".to_string(),
            temporary: false,
            transaction_count_per_segment: 10_000_000,
            snapshot_frequency: 4,
        }
    }
}