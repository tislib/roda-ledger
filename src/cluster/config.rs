use crate::config::LedgerConfig;
use crate::grpc::GrpcServerSection;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ClusterServerConfig {
    pub server: GrpcServerSection,
    pub ledger: LedgerConfig,
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
    pub mode: NodeMode,
    pub node_id: u64,
    pub node_host: String,
    pub node_port: u16,
    pub peers: Vec<PeerConfig>,
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
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.node_host, self.node_port).parse()
    }

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

    /// Apply cluster-derived overrides onto the embedded ledger config.
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
