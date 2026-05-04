//! Cluster configuration shape (ADR-0016 §1).
//!
//! Two deployment modes share a single `Config`:
//!
//! 1. **Standalone** — no `[cluster]` section at all. Boots the
//!    client-facing Ledger gRPC server only; no Node service, no
//!    replication, no peers. Useful for embedded / single-process
//!    deployments and for tests that don't exercise replication.
//!
//! ```toml
//! [server]
//! host = "0.0.0.0"
//! port = 50051
//!
//! [ledger]
//! # ... standard LedgerConfig ...
//! ```
//!
//! 2. **Clustered** — explicit `[cluster]` block. Identity and peer
//!    membership live under `[cluster]`; the top-level `[server]` is
//!    still strictly the client-facing Ledger gRPC. The peer-facing
//!    Node gRPC service is configured via `[cluster.node]`.
//!
//! ```toml
//! [server]                        # client-facing Ledger gRPC
//! host = "0.0.0.0"
//! port = 50051
//!
//! [cluster.node]                  # this node's identity + peer-facing gRPC
//! node_id = 1
//! host    = "0.0.0.0"
//! port    = 50061
//!
//! [[cluster.peers]]               # full membership, including self
//! peer_id = 1
//! host    = "http://127.0.0.1:50061"
//!
//! [[cluster.peers]]
//! peer_id = 2
//! host    = "http://127.0.0.1:50062"
//!
//! [ledger]
//! # ... standard LedgerConfig ...
//! ```
//!
//! When `[cluster]` is present, the `cluster.peers` list **includes
//! every node, this one included**. Each node's config file is
//! therefore identical except for its own `[cluster.node]`. At
//! startup the node validates that `cluster.node.node_id` appears in
//! `cluster.peers`, and derives its "others" set by filtering itself
//! out. A "single-node cluster" (one peer = self) is distinct from
//! "standalone" (no cluster at all): the former still bumps term on
//! restart and exposes the Node gRPC service.
//!
//! Role (Leader / Follower / Initializing / Candidate) is **not** a
//! configuration value — it is a runtime property driven by the
//! role state machine (Stages 3b/4 work).

use ledger::config::LedgerConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

// ── Client-facing gRPC server section ───────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerSection {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub max_message_size_bytes: usize,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50051,
            max_connections: 1000,
            max_message_size_bytes: 4 * 1024 * 1024,
        }
    }
}

impl ServerSection {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

// ── [cluster.node] : this node's identity + peer-facing gRPC bind ──────────

/// Identity (`node_id`) and peer-facing Node gRPC bind for **this**
/// process. Lives under `[cluster.node]` so the file shape mirrors
/// what other peers see in `[[cluster.peers]]`.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ClusterNodeSection {
    pub node_id: u64,
    pub host: String,
    pub port: u16,
}

impl Default for ClusterNodeSection {
    fn default() -> Self {
        Self {
            node_id: 1,
            host: "0.0.0.0".to_string(),
            port: 50061,
        }
    }
}

impl ClusterNodeSection {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

// ── [[cluster.peers]] : full cluster membership (incl. self) ───────────────

/// One entry in the symmetric `cluster.peers` list. `host` is the gRPC
/// endpoint of that peer's Node service, e.g.
/// `http://127.0.0.1:50061` (the URL **does** include the scheme so
/// tonic can connect directly).
#[derive(Clone, Debug, Deserialize)]
pub struct PeerConfig {
    pub peer_id: u64,
    pub host: String,
}

// ── [cluster] : the umbrella section grouping node + peers ─────────────────

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ClusterSection {
    pub node: ClusterNodeSection,
    pub peers: Vec<PeerConfig>,
    /// How often the per-peer replication thread ticks when its
    /// `WalTailer` returns no new bytes (idle heartbeat cadence).
    pub replication_poll_ms: u64,
    /// Max WAL bytes shipped per `AppendEntries` RPC.
    pub append_entries_max_bytes: usize,
}

impl Default for ClusterSection {
    fn default() -> Self {
        // Used only when the user explicitly wrote a partial `[cluster]`
        // block (e.g. `[cluster.node] node_id = 1`) without listing
        // peers. Synthesise a single-peer (self) list so validation
        // succeeds — same shape as a standalone-with-cluster boot.
        let node = ClusterNodeSection::default();
        let peers = vec![PeerConfig {
            peer_id: node.node_id,
            host: format!("http://{}:{}", node.host, node.port),
        }];
        Self {
            node,
            peers,
            replication_poll_ms: 5,
            append_entries_max_bytes: 4 * 1024 * 1024,
        }
    }
}

// ── Top-level Config ───────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerSection,
    /// Optional cluster block. `None` means **standalone**: no Node
    /// gRPC, no replication, no peers — just the writable client
    /// Ledger gRPC. `Some(_)` means **clustered**: the role state
    /// machine, term bumping, and peer replication all engage. Even
    /// a single-node-cluster (one self-peer) takes this path.
    ///
    /// Replication-shaped knobs (`replication_poll_ms`,
    /// `append_entries_max_bytes`) live inside [`ClusterSection`] —
    /// they're meaningless in standalone mode.
    pub cluster: Option<ClusterSection>,
    pub ledger: LedgerConfig,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    /// Config validation failed (e.g. self not present in
    /// `cluster.peers`, duplicate `peer_id`).
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "failed to read cluster config: {}", e),
            Self::Parse(e) => write!(f, "failed to parse cluster config toml: {}", e),
            Self::Invalid(msg) => write!(f, "invalid cluster config: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::Parse(e)
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Sanity-check the cluster shape. Run automatically on every
    /// `from_*` constructor; callers who build a `Config` manually
    /// (tests, `load_cluster`) can opt in with this method.
    ///
    /// In **standalone** mode (`cluster.is_none()`) there is nothing
    /// cluster-shaped to validate.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let Some(cluster) = self.cluster.as_ref() else {
            return Ok(());
        };
        if cluster.node.node_id == 0 {
            return Err(ConfigError::Invalid(
                "cluster.node.node_id must be non-zero".into(),
            ));
        }
        if cluster.peers.is_empty() {
            return Err(ConfigError::Invalid(
                "cluster.peers must contain at least one entry (this node)".into(),
            ));
        }
        // Self must appear in peers.
        let self_id = cluster.node.node_id;
        if !cluster.peers.iter().any(|p| p.peer_id == self_id) {
            return Err(ConfigError::Invalid(format!(
                "cluster.peers does not contain self (node_id={})",
                self_id
            )));
        }
        // Duplicate peer_ids are a deployment bug.
        let mut ids: Vec<u64> = cluster.peers.iter().map(|p| p.peer_id).collect();
        ids.sort_unstable();
        for w in ids.windows(2) {
            if w[0] == w[1] {
                return Err(ConfigError::Invalid(format!(
                    "cluster.peers contains duplicate peer_id={}",
                    w[0]
                )));
            }
        }
        Ok(())
    }

    /// True when `[cluster]` is present in config — i.e. the role
    /// state machine, Node gRPC, term bumping, and replication
    /// machinery should all engage. False = standalone single-process
    /// deployment with only the client-facing Ledger gRPC.
    #[inline]
    pub fn is_clustered(&self) -> bool {
        self.cluster.is_some()
    }

    /// This node's `node_id`, hoisted from `cluster.node.node_id`.
    /// Returns `0` when standalone (no cluster identity).
    #[inline]
    pub fn node_id(&self) -> u64 {
        self.cluster.as_ref().map(|c| c.node.node_id).unwrap_or(0)
    }

    /// Iterator over peers excluding self. Empty when standalone or
    /// when the cluster has only this node. Used by leader-side
    /// replication fan-out.
    pub fn other_peers(&self) -> impl Iterator<Item = &PeerConfig> {
        let self_id = self.node_id();
        self.cluster
            .as_ref()
            .into_iter()
            .flat_map(|c| c.peers.iter())
            .filter(move |p| p.peer_id != self_id)
    }

    /// Total cluster size (this node + others). Returns `1` for
    /// standalone — the lone process is its own one-node "cluster"
    /// from a quorum-sizing perspective, even though no Quorum
    /// actually runs.
    #[inline]
    pub fn cluster_size(&self) -> usize {
        self.cluster.as_ref().map(|c| c.peers.len()).unwrap_or(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_cluster_config() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 50051

            [cluster]
            replication_poll_ms = 5
            append_entries_max_bytes = 4194304

            [cluster.node]
            node_id = 1
            host = "0.0.0.0"
            port = 50061

            [[cluster.peers]]
            peer_id = 1
            host = "http://127.0.0.1:50061"

            [[cluster.peers]]
            peer_id = 2
            host = "http://127.0.0.1:50062"

            [ledger]
            max_accounts = 100000

            [ledger.storage]
            data_dir = "/tmp/leader"
        "#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(cfg.is_clustered());
        assert_eq!(cfg.node_id(), 1);
        assert_eq!(cfg.cluster_size(), 2);
        let cluster = cfg.cluster.as_ref().unwrap();
        assert_eq!(cluster.peers[1].peer_id, 2);
        assert_eq!(cluster.node.port, 50061);
        assert_eq!(cluster.replication_poll_ms, 5);
        assert_eq!(cluster.append_entries_max_bytes, 4_194_304);
        // Self filtered out for fan-out.
        let others: Vec<u64> = cfg.other_peers().map(|p| p.peer_id).collect();
        assert_eq!(others, vec![2]);
    }

    #[test]
    fn default_config_is_standalone() {
        let cfg = Config::default();
        assert!(!cfg.is_clustered());
        assert_eq!(cfg.node_id(), 0);
        assert_eq!(cfg.cluster_size(), 1);
        assert_eq!(cfg.other_peers().count(), 0);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn config_with_only_server_block_is_standalone() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 50051

            [ledger]
            max_accounts = 100000

            [ledger.storage]
            data_dir = "/tmp/standalone"
        "#;
        let cfg = Config::from_toml_str(toml).unwrap();
        assert!(!cfg.is_clustered());
        assert!(cfg.cluster.is_none());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_self_missing_from_peers() {
        let toml = r#"
            [cluster.node]
            node_id = 1

            [[cluster.peers]]
            peer_id = 2
            host = "http://x:50061"

            [ledger.storage]
            data_dir = "/tmp/x"
        "#;
        let err = Config::from_toml_str(toml).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("does not contain self"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn validate_rejects_duplicate_peer_id() {
        let toml = r#"
            [cluster.node]
            node_id = 1

            [[cluster.peers]]
            peer_id = 1
            host = "http://a"

            [[cluster.peers]]
            peer_id = 1
            host = "http://b"

            [ledger.storage]
            data_dir = "/tmp/x"
        "#;
        let err = Config::from_toml_str(toml).unwrap_err();
        assert!(format!("{}", err).contains("duplicate"));
    }

    #[test]
    fn validate_rejects_zero_node_id() {
        let toml = r#"
            [cluster.node]
            node_id = 0

            [[cluster.peers]]
            peer_id = 0
            host = "http://x"

            [ledger.storage]
            data_dir = "/tmp/x"
        "#;
        let err = Config::from_toml_str(toml).unwrap_err();
        assert!(format!("{}", err).contains("node_id"));
    }
}
