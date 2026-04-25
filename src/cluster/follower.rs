//! `Follower` — role-specific bring-up for the follower side of the cluster.
//!
//! Owns the client-facing Ledger gRPC server in **read-only** mode and the
//! peer-facing Node gRPC server with `NodeRole::Follower`. No replication
//! fan-out runs on this side; incoming `AppendEntries` are applied to the
//! local ledger via `NodeHandler`, which also durably `observe()`s the
//! leader's term on every accepted batch.

use crate::cluster::config::Config;
use crate::cluster::node_handler::NodeHandler;
use crate::cluster::proto::node::NodeRole;
use crate::cluster::server::{NodeServerRuntime, Server};
use crate::cluster::{ClusterCommitIndex, Term};
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a follower node. Construct, then `run()`.
pub struct Follower {
    config: Config,
    ledger: Arc<Ledger>,
    term: Arc<Term>,
}

impl Follower {
    pub fn new(config: Config, ledger: Arc<Ledger>, term: Arc<Term>) -> Self {
        Self {
            config,
            ledger,
            term,
        }
    }

    /// Spawn both gRPC servers and return their handles.
    pub async fn run(&self) -> Result<FollowerHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("Follower::run requires a clustered config");
        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;

        // Shared watermark — written by NodeHandler on every successful
        // AppendEntries, read by LedgerHandler for ClusterCommit waits
        // and `GetPipelineIndex`.
        let cluster_commit_index = ClusterCommitIndex::new();

        // Client-facing Ledger server — read-only on followers.
        // Attaches the shared Arc<Term> so the stub that returns
        // FAILED_PRECONDITION still surfaces the current term in any
        // error-carrying field, and the query RPCs can resolve per-tx
        // term via Term::get_term_at_tx (hot ring / cold scan).
        let client_server = Server::new_read_only(
            self.ledger.clone(),
            client_addr,
            self.term.clone(),
            cluster_commit_index.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("follower ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node server — accepts `AppendEntries` from the leader.
        // The handler's Arc<Term> lets it `observe()` the incoming term
        // and durably advance the follower's own term log in lock-step
        // with replication.
        let node_handler = NodeHandler::new(
            self.ledger.clone(),
            self.config.node_id(),
            self.term.clone(),
            NodeRole::Follower,
            Some(cluster_commit_index.clone()),
        );
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("follower node gRPC server exited: {}", e);
            }
        });

        info!("follower: node_id={} up", self.config.node_id());
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
