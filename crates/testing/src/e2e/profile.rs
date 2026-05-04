//! Server profile loading from `profiles.toml`.
//!
//! A profile is the single source of truth for node count and all server/ledger
//! configuration. Backends read the profile — no hardcoded config values in
//! backend or context code.
//!
//! Runtime-specific values (`host`, `port`, `data_dir`) are declared in the
//! profile as defaults. Backends override them at startup (e.g. free port,
//! temp directory).

use cluster::config::ServerSection;
use ledger::config::{LedgerConfig, StorageConfig};
use ledger::wait_strategy::WaitStrategy;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Raw deserialization target matching profiles.toml layout.
#[derive(Debug, Deserialize)]
struct ProfileDef {
    nodes: usize,
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    ledger: LedgerConfig,
}

/// A server profile — node count + full server and ledger configuration.
#[derive(Debug, Clone)]
pub struct Profile {
    pub name: String,
    pub nodes: usize,
    pub server: ServerSection,
    pub ledger: LedgerConfig,
}

fn load_profiles() -> HashMap<String, Profile> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Path::new(manifest_dir).join("src/e2e/profiles.toml");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let raw: HashMap<String, ProfileDef> =
        toml::from_str(&contents).expect("failed to parse profiles.toml");

    raw.into_iter()
        .map(|(name, def)| {
            let profile = Profile {
                name: name.clone(),
                nodes: def.nodes,
                server: def.server,
                ledger: def.ledger,
            };
            (name, profile)
        })
        .collect()
}

/// Load a named profile from `profiles.toml`. Panics if not found.
pub fn profile(name: &str) -> Profile {
    let profiles = load_profiles();
    profiles
        .get(name)
        .unwrap_or_else(|| panic!("unknown profile: {name}"))
        .clone()
}

impl Profile {
    /// Build a `LedgerConfig` for one node, overriding `data_dir` with a
    /// fresh temp directory. The caller owns the temp dir lifecycle.
    pub fn ledger_config_with_temp_dir(&self) -> LedgerConfig {
        let mut dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        dir.push(format!("temp_{}", rand));

        let mut config = self.ledger.clone();
        config.storage = StorageConfig {
            data_dir: dir.to_string_lossy().to_string(),
            temporary: true,
            transaction_count_per_segment: self.ledger.storage.transaction_count_per_segment,
            snapshot_frequency: self.ledger.storage.snapshot_frequency,
        };
        config
    }

    /// Render a `config.toml` string for the Process backend, with the
    /// given runtime overrides for host, port, and data_dir.
    pub fn render_config_toml(&self, host: &str, port: u16, data_dir: &str) -> String {
        format!(
            r#"[server]
host = "{host}"
port = {port}
max_connections = {max_conn}
max_message_size_bytes = {max_msg}

[ledger]
max_accounts = {max_accounts}
wait_strategy = "{wait_strategy}"

[ledger.storage]
data_dir = "{data_dir}"
transaction_count_per_segment = {tx_per_seg}
snapshot_frequency = {snap_freq}
"#,
            max_conn = self.server.max_connections,
            max_msg = self.server.max_message_size_bytes,
            max_accounts = self.ledger.max_accounts,
            wait_strategy = match self.ledger.wait_strategy {
                WaitStrategy::LowLatency => "low_latency",
                WaitStrategy::Balanced => "balanced",
                WaitStrategy::LowCpu => "low_cpu",
                WaitStrategy::Custom { .. } => "balanced",
            },
            tx_per_seg = self.ledger.storage.transaction_count_per_segment,
            snap_freq = self.ledger.storage.snapshot_frequency,
        )
    }
}
