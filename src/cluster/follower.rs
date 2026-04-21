//! `Follower` — role-specific bring-up for the follower side of the cluster.
//!
//! Owns the client-facing Ledger gRPC server in **read-only** mode and the
//! peer-facing Node gRPC server with `NodeRole::Follower`. No replication
//! fan-out runs on this side; incoming `AppendEntries` are applied to the
//! local ledger via `NodeHandler`.

use crate::cluster::config::ClusterConfig;
use crate::cluster::proto::NodeRole;
use crate::cluster::server::{NodeHandler, NodeServerRuntime};
use crate::grpc::GrpcServer;
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a follower node. Construct, then `run()`.
pub struct Follower {
    config: ClusterConfig,
    ledger: Arc<Ledger>,
}

impl Follower {
    pub fn new(config: ClusterConfig, ledger: Arc<Ledger>) -> Self {
        Self { config, ledger }
    }

    /// Spawn both gRPC servers and return their handles.
    pub async fn run(&self) -> Result<FollowerHandles, Box<dyn std::error::Error + Send + Sync>> {
        let client_addr = self.config.server.socket_addr()?;
        let node_addr = self.config.node.socket_addr()?;

        // Client-facing Ledger server — read-only on followers. Every
        // `submit_*` / `register_function` RPC returns FAILED_PRECONDITION.
        let client_server = GrpcServer::new_read_only(self.ledger.clone(), client_addr);
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("follower ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node server — accepts `AppendEntries` from the leader.
        let node_handler = NodeHandler::new(
            self.ledger.clone(),
            self.config.node_id,
            self.config.term,
            NodeRole::Follower,
        );
        let node_max_bytes = self.config.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("follower node gRPC server exited: {}", e);
            }
        });

        info!("follower: node_id={} up", self.config.node_id);
        Ok(FollowerHandles {
            client_handle,
            node_handle,
        })
    }
}

/// Handles produced by a successful `Follower::run`.
pub struct FollowerHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
}

impl FollowerHandles {
    pub fn abort(&self) {
        self.client_handle.abort();
        self.node_handle.abort();
    }
}
