//! Root config for the `cluster` binary (ADR-015).
//!
//! Layers over the existing single-node `ServerConfig`:
//!   - `server` + `ledger`: inherited from `crate::grpc::ServerConfig`.
//!   - `cluster`: new section describing node identity, role, peers,
//!     and the Node-proto listen address.

use crate::config::LedgerConfig;
use crate::grpc::GrpcServerSection;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// Top-level config for a clustered deployment.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ClusterServerConfig {
    /// Client-facing gRPC transport (same `[server]` section used by the
    /// single-node `roda-ledger` binary — port 50051 by default).
    pub server: GrpcServerSection,
    /// Embedded ledger.
    pub ledger: LedgerConfig,
    /// Peer-to-peer replication transport + membership.
    pub cluster: ClusterConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    #[default]
    Leader,
    Follower,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Role of this node.
    pub mode: NodeMode,
    /// Unique id inside the cluster.
    pub node_id: u64,
    /// Bind host for the Node-proto service.
    pub node_host: String,
    /// Bind port for the Node-proto service (separate from client API).
    pub node_port: u16,
    /// Static peer list (ADR-015 — no elections). Addresses of the other
    /// nodes in this cluster. Empty on a single-node deployment.
    pub peers: Vec<PeerConfig>,
    /// Persisted term from the `raft_state` sidecar. Under ADR-015 this
    /// is always 1; ADR-016 will overwrite on leader change.
    pub current_term: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            mode: NodeMode::Leader,
            node_id: 1,
            node_host: "0.0.0.0".to_string(),
            node_port: 50052,
            peers: Vec::new(),
            current_term: 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerConfig {
    pub node_id: u64,
    pub address: String,
}

impl ClusterConfig {
    /// Resolve the Node-proto bind socket from host + port.
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.node_host, self.node_port).parse()
    }

    /// True iff this node is a follower under the static-leader model.
    pub fn is_follower(&self) -> bool {
        self.mode == NodeMode::Follower
    }
}

impl ClusterServerConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ClusterConfigError> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(s: &str) -> Result<Self, ClusterConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Apply cluster-derived overrides onto the embedded `LedgerConfig`.
    /// In particular: flip `replication_mode = true` for followers.
    pub fn finalize(mut self) -> Self {
        if self.cluster.is_follower() {
            self.ledger.replication_mode = true;
        }
        self
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
            Self::Parse(e) => write!(f, "failed to parse cluster toml: {}", e),
        }
    }
}

impl std::error::Error for ClusterConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Parse(e) => Some(e),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_leader_config() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 50051

            [ledger]
            max_accounts = 1000

            [cluster]
            mode = "leader"
            node_id = 1
            node_host = "0.0.0.0"
            node_port = 50052
            current_term = 1

            [[cluster.peers]]
            node_id = 2
            address = "10.0.0.2:50052"
        "#;
        let cfg = ClusterServerConfig::from_toml_str(toml).expect("parse").finalize();
        assert_eq!(cfg.cluster.mode, NodeMode::Leader);
        assert_eq!(cfg.cluster.peers.len(), 1);
        assert!(!cfg.ledger.replication_mode);
    }

    #[test]
    fn follower_flips_replication_mode() {
        let toml = r#"
            [cluster]
            mode = "follower"
            node_id = 2
            node_host = "0.0.0.0"
            node_port = 50052
            current_term = 1
        "#;
        let cfg = ClusterServerConfig::from_toml_str(toml).expect("parse").finalize();
        assert!(cfg.cluster.is_follower());
        assert!(cfg.ledger.replication_mode);
    }

    #[test]
    fn socket_addr_composes() {
        let c = ClusterConfig {
            node_host: "127.0.0.1".to_string(),
            node_port: 50052,
            ..Default::default()
        };
        let addr = c.socket_addr().unwrap();
        assert_eq!(addr.port(), 50052);
    }
}
