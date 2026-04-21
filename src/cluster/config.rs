//! Static cluster configuration for ADR-015. Leader and peers are fixed;
//! dynamic membership and elections are deferred to ADR-016.

use crate::config::LedgerConfig;
use crate::grpc::GrpcServerSection;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    Leader,
    Follower,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    /// gRPC endpoint of the peer's Node service (e.g. `http://127.0.0.1:50061`).
    pub node_addr: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct NodeServerSection {
    pub host: String,
    pub port: u16,
}

impl Default for NodeServerSection {
    fn default() -> Self {
        Self { host: "0.0.0.0".to_string(), port: 50061 }
    }
}

impl NodeServerSection {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub mode: ClusterMode,
    pub node_id: u64,
    pub term: u64,
    pub peers: Vec<PeerConfig>,
    pub server: GrpcServerSection,
    pub node: NodeServerSection,
    pub ledger: LedgerConfig,
    /// How often the replication thread ticks when idle (ms).
    pub replication_poll_ms: u64,
    /// Max WAL bytes per `AppendEntries` RPC.
    pub append_entries_max_bytes: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            mode: ClusterMode::Leader,
            node_id: 1,
            term: 1,
            peers: Vec::new(),
            server: GrpcServerSection::default(),
            node: NodeServerSection::default(),
            ledger: LedgerConfig::default(),
            replication_poll_ms: 5,
            append_entries_max_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum ClusterConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ClusterConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "failed to read cluster config: {}", e),
            Self::Parse(e) => write!(f, "failed to parse cluster config toml: {}", e),
        }
    }
}

impl std::error::Error for ClusterConfigError {}

impl From<std::io::Error> for ClusterConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<toml::de::Error> for ClusterConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::Parse(e)
    }
}

impl ClusterConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ClusterConfigError> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(s: &str) -> Result<Self, ClusterConfigError> {
        Ok(toml::from_str(s)?)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.mode, ClusterMode::Leader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_cluster_config() {
        let toml = r#"
            mode = "leader"
            node_id = 1
            term = 1
            replication_poll_ms = 5
            append_entries_max_bytes = 4194304

            [[peers]]
            id = 2
            node_addr = "http://127.0.0.1:50062"

            [server]
            host = "0.0.0.0"
            port = 50051

            [node]
            host = "0.0.0.0"
            port = 50061

            [ledger]
            max_accounts = 100000

            [ledger.storage]
            data_dir = "/tmp/leader"
        "#;
        let cfg = ClusterConfig::from_toml_str(toml).unwrap();
        assert!(cfg.is_leader());
        assert_eq!(cfg.peers.len(), 1);
        assert_eq!(cfg.peers[0].id, 2);
        assert_eq!(cfg.node.port, 50061);
    }
}
