//! `Leader` — role-specific bring-up for the leader side of the cluster.
//!
//! Owns everything the leader role needs: the client-facing Ledger gRPC
//! server (writable), the peer-facing Node gRPC server (role = Leader),
//! and direct supervision of one [`PeerReplication`] sub-task per peer.
//! The top-level `tokio::spawn` for peer supervision lives here —
//! `Leader::run` spawns one child per peer.

use crate::cluster::GrpcServer;
use crate::cluster::Quorum;
use crate::cluster::config::ClusterConfig;
use crate::cluster::node_server::{NodeHandler, NodeServerRuntime};
use crate::cluster::peer_replication::{PeerReplication, ReplicationParams};
use crate::cluster::proto::node::NodeRole;
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a leader node. Construct, then `run()`.
pub struct Leader {
    config: ClusterConfig,
    ledger: Arc<Ledger>,
}

impl Leader {
    pub fn new(config: ClusterConfig, ledger: Arc<Ledger>) -> Self {
        Self { config, ledger }
    }

    /// Spawn every task the leader role needs. Returns a `LeaderHandles`
    /// carrying the spawned handles plus shared shutdown/observability
    /// state (`running`, `majority`, `peer_handles`).
    pub async fn run(&self) -> Result<LeaderHandles, Box<dyn std::error::Error + Send + Sync>> {
        let client_addr = self.config.server.socket_addr()?;
        let node_addr = self.config.node.socket_addr()?;

        // Client-facing Ledger server — full read/write on the leader.
        // `with_term` stamps the current cluster term on every submit reply.
        let client_server =
            GrpcServer::new(self.ledger.clone(), client_addr).with_term(self.config.term);
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("leader ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node server. The leader advertises NodeRole::Leader
        // so followers that misconfigure themselves as peer-targets of
        // this node get `RejectNotFollower` rather than silent success.
        let node_handler = NodeHandler::new(
            self.ledger.clone(),
            self.config.node_id,
            self.config.term,
            NodeRole::Leader,
        );
        let node_max_bytes = self.config.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("leader node gRPC server exited: {}", e);
            }
        });

        // Replication: one child task per peer, all sharing one `Quorum`
        // and one cooperative shutdown flag. Replaces the former
        // `PeerManager` supervisor — the leader is the supervisor now.
        let running = Arc::new(AtomicBool::new(true));
        let quorum = Arc::new(Quorum::new(self.config.peers.len()));
        let mut peer_handles: Vec<JoinHandle<()>> = Vec::with_capacity(self.config.peers.len());

        if self.config.peers.is_empty() {
            info!(
                "leader: node_id={} has no peers; no replication tasks started",
                self.config.node_id
            );
        } else {
            let params = ReplicationParams::new(
                self.config.node_id,
                self.config.term,
                self.config.append_entries_max_bytes,
                Duration::from_millis(self.config.replication_poll_ms.max(1)),
            );
            for (idx, peer) in self.config.peers.iter().enumerate() {
                let replicator = PeerReplication::new(
                    peer.clone(),
                    idx as u32,
                    self.ledger.clone(),
                    params.clone(),
                    running.clone(),
                    quorum.clone(),
                );
                peer_handles.push(tokio::spawn(async move { replicator.run().await }));
            }
            info!(
                "leader: node_id={} replicating to {} peer(s)",
                self.config.node_id,
                self.config.peers.len()
            );
        }

        Ok(LeaderHandles {
            client_handle,
            node_handle,
            peer_handles,
            running,
            quorum,
        })
    }
}

/// Handles + shared state produced by a successful `Leader::run`.
pub struct LeaderHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
    /// One handle per peer replication task. Positional with `config.peers`.
    pub peer_handles: Vec<JoinHandle<()>>,
    /// Cooperative shutdown flag shared with every peer sub-task. Flip
    /// to drain peer tasks without aborting.
    pub running: Arc<AtomicBool>,
    /// Cluster-wide majority tracker. Callers read `quorum.get()` for
    /// the latest majority-committed index.
    pub quorum: Arc<Quorum>,
}

impl LeaderHandles {
    /// Stop every leader-role task. Tests use this; production paths can
    /// wire a real shutdown channel via `NodeServerRuntime::run_with_shutdown`.
    pub fn abort(&self) {
        self.client_handle.abort();
        self.node_handle.abort();
        self.running.store(false, Ordering::Release);
        for h in &self.peer_handles {
            h.abort();
        }
    }
}
