//! `Cluster` — thin dispatcher. Constructs the embedded `Ledger`, then
//! hands control to [`Leader`] or [`Follower`] depending on `config.mode`.
//! All role-specific bring-up lives in the role files.

use crate::cluster::config::{ClusterConfig, ClusterMode};
use crate::cluster::follower::{Follower, FollowerHandles};
use crate::cluster::leader::{Leader, LeaderHandles};
use crate::cluster::{Quorum, Term};
use crate::ledger::Ledger;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::task::JoinHandle;

pub struct Cluster {
    config: ClusterConfig,
    ledger: Arc<Ledger>,
    term: Arc<Term>,
}

impl Cluster {
    /// Build (and start) the embedded Ledger, and open the durable term
    /// log under `ledger.storage.data_dir`. The gRPC servers and, on
    /// leaders, the replication fan-out are launched by [`Cluster::run`].
    pub fn new(config: ClusterConfig) -> std::io::Result<Self> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start()?;
        let term = Arc::new(Term::open_in_dir(&config.ledger.storage.data_dir)?);
        Ok(Self {
            config,
            ledger: Arc::new(ledger),
            term,
        })
    }

    pub fn ledger(&self) -> Arc<Ledger> {
        self.ledger.clone()
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Dispatch to the role-specific bring-up. Returns a unified
    /// `ClusterHandles` so generic callers (tests, `load_cluster`) don't
    /// need to branch on mode.
    pub async fn run(&self) -> Result<ClusterHandles, Box<dyn std::error::Error + Send + Sync>> {
        match self.config.mode {
            ClusterMode::Leader => {
                let leader = Leader::new(
                    self.config.clone(),
                    self.ledger.clone(),
                    self.term.clone(),
                );
                let handles = leader.run().await?;
                Ok(ClusterHandles::Leader(handles))
            }
            ClusterMode::Follower => {
                let follower = Follower::new(
                    self.config.clone(),
                    self.ledger.clone(),
                    self.term.clone(),
                );
                let handles = follower.run().await?;
                Ok(ClusterHandles::Follower(handles))
            }
        }
    }
}

/// Unified handles view over a leader-or-follower bring-up. Enum-shaped
/// so each variant exposes exactly the state that role produced.
pub enum ClusterHandles {
    Leader(LeaderHandles),
    Follower(FollowerHandles),
}

impl ClusterHandles {
    /// Abort every spawned task owned by this bring-up.
    pub fn abort(&self) {
        match self {
            ClusterHandles::Leader(h) => h.abort(),
            ClusterHandles::Follower(h) => h.abort(),
        }
    }

    /// Shared quorum tracker (leader only).
    pub fn quorum(&self) -> Option<Arc<Quorum>> {
        match self {
            ClusterHandles::Leader(h) => Some(h.quorum.clone()),
            ClusterHandles::Follower(_) => None,
        }
    }

    /// Cooperative shutdown flag for the peer subtree (leader only).
    pub fn running(&self) -> Option<Arc<AtomicBool>> {
        match self {
            ClusterHandles::Leader(h) => Some(h.running.clone()),
            ClusterHandles::Follower(_) => None,
        }
    }

    /// Borrow the underlying `LeaderHandles` if this bring-up is a leader.
    pub fn as_leader(&self) -> Option<&LeaderHandles> {
        match self {
            ClusterHandles::Leader(h) => Some(h),
            _ => None,
        }
    }

    /// Borrow the underlying `FollowerHandles` if this bring-up is a follower.
    pub fn as_follower(&self) -> Option<&FollowerHandles> {
        match self {
            ClusterHandles::Follower(h) => Some(h),
            _ => None,
        }
    }

    /// Client-facing Ledger gRPC server handle (always present).
    pub fn client_handle(&self) -> &JoinHandle<()> {
        match self {
            ClusterHandles::Leader(h) => &h.client_handle,
            ClusterHandles::Follower(h) => &h.client_handle,
        }
    }

    /// Peer-facing Node gRPC server handle (always present).
    pub fn node_handle(&self) -> &JoinHandle<()> {
        match self {
            ClusterHandles::Leader(h) => &h.node_handle,
            ClusterHandles::Follower(h) => &h.node_handle,
        }
    }
}
