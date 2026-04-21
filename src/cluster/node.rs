//! `Cluster` — boots both the client-facing Ledger gRPC server and the
//! peer-facing Node gRPC server; on the leader it also drives the
//! replication fan-out.

use crate::cluster::config::{ClusterConfig, ClusterMode};
use crate::cluster::proto::NodeRole;
use crate::cluster::replication::Replication;
use crate::cluster::server::{NodeHandler, NodeServerRuntime};
use crate::grpc::GrpcServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct Cluster {
    config: ClusterConfig,
    ledger: Arc<Ledger>,
}

impl Cluster {
    /// Build (and start) the embedded Ledger. The gRPC servers and
    /// replication thread are launched by [`Cluster::run`].
    pub fn new(config: ClusterConfig) -> std::io::Result<Self> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start()?;
        Ok(Self { config, ledger: Arc::new(ledger) })
    }

    pub fn ledger(&self) -> Arc<Ledger> {
        self.ledger.clone()
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Boot both servers and (on leaders) the replication fan-out. Returns
    /// a `ClusterHandles` that owns every spawned task. Drop / `shutdown`
    /// to stop.
    pub async fn run(&self) -> Result<ClusterHandles, Box<dyn std::error::Error + Send + Sync>> {
        let client_addr = self.config.server.socket_addr()?;
        let node_addr = self.config.node.socket_addr()?;

        let role = match self.config.mode {
            ClusterMode::Leader => NodeRole::Leader,
            ClusterMode::Follower => NodeRole::Follower,
        };

        // Client-facing Ledger server: read-only on followers.
        let client_server = if self.config.mode == ClusterMode::Leader {
            GrpcServer::new(self.ledger.clone(), client_addr)
        } else {
            GrpcServer::new_read_only(self.ledger.clone(), client_addr)
        };
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                spdlog::error!("ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node server: always on, both sides.
        let node_handler = NodeHandler::new(
            self.ledger.clone(),
            self.config.node_id,
            self.config.term,
            role,
        );
        // Size server decode/encode limits to cover the largest chunk the
        // leader might ship plus protobuf framing overhead (doubled +4 KiB).
        let node_max_bytes = self.config.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                spdlog::error!("node gRPC server exited: {}", e);
            }
        });

        // Replication: leader only.
        let mut repl_handles: Vec<JoinHandle<()>> = Vec::new();
        if self.config.mode == ClusterMode::Leader && !self.config.peers.is_empty() {
            let repl = Replication::new(
                self.ledger.clone(),
                self.config.peers.clone(),
                self.config.node_id,
                self.config.term,
                self.config.append_entries_max_bytes,
                Duration::from_millis(self.config.replication_poll_ms.max(1)),
            );
            repl_handles = repl.spawn();
            info!(
                "cluster: leader node_id={} replicating to {} peer(s)",
                self.config.node_id,
                self.config.peers.len()
            );
        }

        Ok(ClusterHandles {
            client_handle,
            node_handle,
            repl_handles,
        })
    }
}

pub struct ClusterHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
    pub repl_handles: Vec<JoinHandle<()>>,
}

impl ClusterHandles {
    /// Abort every background task. Does NOT wait for graceful shutdown —
    /// sufficient for tests; production paths can wire a real shutdown
    /// channel via `NodeServerRuntime::run_with_shutdown`.
    pub fn abort(&self) {
        self.client_handle.abort();
        self.node_handle.abort();
        for h in &self.repl_handles {
            h.abort();
        }
    }
}
