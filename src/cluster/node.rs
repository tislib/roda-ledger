//! `Cluster` — thin dispatcher.
//!
//! Three deployment shapes:
//!
//! - **Standalone** — `config.cluster.is_none()`. Spawns only the
//!   client-facing Ledger gRPC server in writable mode. No Node gRPC,
//!   no replication, no peers. The Ledger still bumps term on
//!   `start()`, so its `term.log` advances on every restart in
//!   lock-step with the future cluster path.
//! - **Clustered, seed leader** — `is_seed_leader()` is true. Routes
//!   into [`Leader::run`].
//! - **Clustered, follower** — otherwise. Routes into [`Follower::run`].
//!
//! The Stage-3a leader-detection heuristic (lowest `peer_id`) is a
//! placeholder until Stage 4 lands real elections.

use crate::cluster::config::Config;
use crate::cluster::follower::{Follower, FollowerHandles};
use crate::cluster::leader::{Leader, LeaderHandles};
use crate::cluster::server::Server;
use crate::cluster::{ClusterCommitIndex, Quorum, Term};
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::task::JoinHandle;

pub struct ClusterNode {
    config: Config,
    ledger: Arc<Ledger>,
    /// Always opened, even in standalone mode (so the term log
    /// advances on every restart per ADR-0016 §11). Cluster code
    /// paths share it across handlers via `Arc::clone`.
    term: Arc<Term>,
}

impl ClusterNode {
    /// Build (and start) the embedded Ledger, and open the durable
    /// term log under `ledger.storage.data_dir`. The gRPC server(s)
    /// are launched by [`ClusterNode::run`].
    pub fn new(config: Config) -> std::io::Result<Self> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start()?;
        let term = Arc::new(Term::open_in_dir(&config.ledger.storage.data_dir)?);
        // Bump the term once on every successful start, recording the
        // current `last_commit_id` as the new term's start_tx_id —
        // mirrors what an election does in Stage 4. This holds for
        // both standalone and clustered deployments so on-disk
        // term.log shape is identical across modes.
        let start_tx = ledger.last_commit_id();
        let new_term = term.new_term(start_tx)?;
        info!(
            "cluster::new: bumped term to {} at start_tx_id={} (clustered={})",
            new_term,
            start_tx,
            config.is_clustered()
        );
        Ok(Self {
            config,
            ledger: Arc::new(ledger),
            term,
        })
    }

    pub fn ledger(&self) -> Arc<Ledger> {
        self.ledger.clone()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Stage-3a placeholder for the role state machine. Returns
    /// `true` if **this** node should bring up as Leader given the
    /// current `cluster.peers` membership. Lowest `peer_id` wins.
    ///
    /// Caller must guarantee `config.is_clustered()` — standalone
    /// configs never call this; they take the standalone branch in
    /// [`Self::run`] instead.
    fn is_seed_leader(&self) -> bool {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("is_seed_leader called on a standalone config");
        let self_id = self.config.node_id();
        let min_id = cluster.peers.iter().map(|p| p.peer_id).min().unwrap_or(self_id);
        self_id == min_id
    }

    /// Dispatch to the standalone or role-specific bring-up. Returns
    /// a unified `Handles` so generic callers (tests, `load_cluster`)
    /// don't need to branch.
    pub async fn run(&self) -> Result<Handles, Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.is_clustered() {
            // Standalone: just the writable client-facing Ledger gRPC.
            let client_addr = self.config.server.socket_addr()?;
            // ClusterCommitIndex isn't meaningful here, but the
            // Server constructor takes one. Hand it a fresh empty
            // index — no one will advance it.
            let cluster_commit_index = ClusterCommitIndex::new();
            let server = Server::new(
                self.ledger.clone(),
                client_addr,
                self.term.clone(),
                cluster_commit_index,
            );
            let client_handle = tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("standalone ledger gRPC server exited: {}", e);
                }
            });
            info!("standalone: client-facing Ledger gRPC up");
            return Ok(Handles::Standalone(StandaloneHandles { client_handle }));
        }

        if self.is_seed_leader() {
            let leader =
                Leader::new(self.config.clone(), self.ledger.clone(), self.term.clone());
            let handles = leader.run().await?;
            Ok(Handles::Leader(handles))
        } else {
            let follower =
                Follower::new(self.config.clone(), self.ledger.clone(), self.term.clone());
            let handles = follower.run().await?;
            Ok(Handles::Follower(handles))
        }
    }
}

/// Unified handles view across standalone / leader / follower
/// bring-ups. Enum-shaped so each variant exposes exactly the state
/// that bring-up produced.
pub enum Handles {
    Standalone(StandaloneHandles),
    Leader(LeaderHandles),
    Follower(FollowerHandles),
}

/// Handles produced by a successful standalone bring-up. Single
/// gRPC server, nothing else.
pub struct StandaloneHandles {
    pub client_handle: JoinHandle<()>,
}

impl StandaloneHandles {
    pub fn abort(&self) {
        self.client_handle.abort();
    }
}

impl Handles {
    /// Abort every spawned task owned by this bring-up.
    pub fn abort(&self) {
        match self {
            Handles::Standalone(h) => h.abort(),
            Handles::Leader(h) => h.abort(),
            Handles::Follower(h) => h.abort(),
        }
    }

    /// Shared quorum tracker (leader only).
    pub fn quorum(&self) -> Option<Arc<Quorum>> {
        match self {
            Handles::Leader(h) => Some(h.quorum.clone()),
            _ => None,
        }
    }

    /// Cooperative shutdown flag for the peer subtree (leader only).
    pub fn running(&self) -> Option<Arc<AtomicBool>> {
        match self {
            Handles::Leader(h) => Some(h.running.clone()),
            _ => None,
        }
    }

    /// Borrow the underlying `LeaderHandles` if this bring-up is a leader.
    pub fn as_leader(&self) -> Option<&LeaderHandles> {
        match self {
            Handles::Leader(h) => Some(h),
            _ => None,
        }
    }

    /// Borrow the underlying `FollowerHandles` if this bring-up is a follower.
    pub fn as_follower(&self) -> Option<&FollowerHandles> {
        match self {
            Handles::Follower(h) => Some(h),
            _ => None,
        }
    }

    /// Client-facing Ledger gRPC server handle (always present).
    pub fn client_handle(&self) -> &JoinHandle<()> {
        match self {
            Handles::Standalone(h) => &h.client_handle,
            Handles::Leader(h) => &h.client_handle,
            Handles::Follower(h) => &h.client_handle,
        }
    }

    /// Peer-facing Node gRPC server handle. `None` in standalone mode
    /// (no Node service runs).
    pub fn node_handle(&self) -> Option<&JoinHandle<()>> {
        match self {
            Handles::Standalone(_) => None,
            Handles::Leader(h) => Some(&h.node_handle),
            Handles::Follower(h) => Some(&h.node_handle),
        }
    }
}
