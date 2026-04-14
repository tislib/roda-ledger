//! Configuration for the gRPC server binary.
//!
//! `ServerConfig` is the root object deserialized from `config.toml`: it owns a
//! `LedgerConfig` for the embedded ledger and a `GrpcServerSection` describing
//! how the gRPC transport should bind and accept connections.

use crate::config::LedgerConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub server: GrpcServerSection,
    pub ledger: LedgerConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct GrpcServerSection {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub max_message_size_bytes: usize,
}

impl Default for GrpcServerSection {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50051,
            max_connections: 1000,
            max_message_size_bytes: 4 * 1024 * 1024, // 4MB
        }
    }
}

impl GrpcServerSection {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

#[derive(Debug)]
pub enum ServerConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ServerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "failed to read config file: {}", e),
            Self::Parse(e) => write!(f, "failed to parse config toml: {}", e),
        }
    }
}

impl std::error::Error for ServerConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Parse(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for ServerConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<toml::de::Error> for ServerConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::Parse(e)
    }
}

impl ServerConfig {
    /// Load a `ServerConfig` from a TOML file on disk.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ServerConfigError> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(s: &str) -> Result<Self, ServerConfigError> {
        Ok(toml::from_str(s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_strategy::WaitStrategy;

    #[test]
    fn parses_reference_config() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 50051
            max_connections = 1000
            max_message_size_bytes = 4194304

            [ledger]
            max_accounts = 1000000
            wait_strategy = "balanced"

            [ledger.storage]
            data_dir = "/data"
            transaction_count_per_segment = 10000000
            snapshot_frequency = 2
        "#;

        let cfg = ServerConfig::from_toml_str(toml).expect("parse");
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 50051);
        assert_eq!(cfg.server.max_connections, 1000);
        assert_eq!(cfg.server.max_message_size_bytes, 4 * 1024 * 1024);

        assert_eq!(cfg.ledger.max_accounts, 1_000_000);
        assert_eq!(cfg.ledger.wait_strategy, WaitStrategy::Balanced);

        assert_eq!(cfg.ledger.storage.data_dir, "/data");
        assert_eq!(cfg.ledger.storage.transaction_count_per_segment, 10_000_000);
        assert_eq!(cfg.ledger.storage.snapshot_frequency, 2);

        // Derived index sizes based on transaction_count_per_segment (rounded to next power of two).
        assert_eq!(cfg.ledger.index_circle1_size(), 16_777_216); // next_power_of_two(10M)
        assert_eq!(cfg.ledger.index_circle2_size(), 33_554_432); // next_power_of_two(20M)
        assert!(!cfg.ledger.disable_seal);
    }

    #[test]
    fn defaults_when_empty() {
        let cfg = ServerConfig::from_toml_str("").expect("parse");
        assert_eq!(cfg.server.port, 50051);
        assert_eq!(cfg.ledger.max_accounts, 1_000_000);
    }

    #[test]
    fn parses_shipped_config_file() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
        ServerConfig::from_file(&path).expect("shipped config.toml should parse");
    }
}
