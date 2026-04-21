//! `Cluster` — boots both the client-facing Ledger gRPC server and the
//! peer-facing Node gRPC server; on the leader it also drives the
//! replication fan-out.

use crate::cluster::config::{ClusterConfig, ClusterMode};
use crate::cluster::peer_manager::PeerManager;
use crate::cluster::peer_replication::ReplicationParams;
use crate::cluster::proto::NodeRole;
use crate::cluster::server::{NodeHandler, NodeServerRuntime};
use crate::grpc::GrpcServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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

        // Replication: leader only. `PeerManager::spawn` is the only
        // top-level `tokio::spawn` — it owns one supervisor task that
        // spawns one child sub-task per peer.
        let mut peer_manager_handle: Option<JoinHandle<()>> = None;
        let mut peer_manager_running: Option<Arc<AtomicBool>> = None;
        if self.config.mode == ClusterMode::Leader && !self.config.peers.is_empty() {
            let params = ReplicationParams::new(
                self.config.node_id,
                self.config.term,
                self.config.append_entries_max_bytes,
                Duration::from_millis(self.config.replication_poll_ms.max(1)),
            );
            let manager = PeerManager::new(
                self.ledger.clone(),
                self.config.peers.clone(),
                params,
            );
            peer_manager_running = Some(manager.running());
            peer_manager_handle = Some(manager.spawn());
            info!(
                "cluster: leader node_id={} replicating to {} peer(s)",
                self.config.node_id,
                self.config.peers.len()
            );
        }

        Ok(ClusterHandles {
            client_handle,
            node_handle,
            peer_manager_handle,
            peer_manager_running,
        })
    }
}

pub struct ClusterHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
    /// Supervisor handle for the peer-replication subtree (leader only).
    pub peer_manager_handle: Option<JoinHandle<()>>,
    /// Shutdown flag owned by the peer manager; flipping it drains every
    /// peer subtask without aborting.
    pub peer_manager_running: Option<Arc<AtomicBool>>,
}

impl ClusterHandles {
    /// Abort every background task. Does NOT wait for graceful shutdown —
    /// sufficient for tests; production paths can wire a real shutdown
    /// channel via `NodeServerRuntime::run_with_shutdown`.
    pub fn abort(&self) {
        self.client_handle.abort();
        self.node_handle.abort();
        if let Some(run) = &self.peer_manager_running {
            run.store(false, std::sync::atomic::Ordering::Release);
        }
        if let Some(h) = &self.peer_manager_handle {
            h.abort();
        }
    }
}
