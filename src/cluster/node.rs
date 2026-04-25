//! `ClusterNode` — top-level entry point for both deployment shapes.
//!
//! - **Standalone** (`config.cluster.is_none()`): spawns only the
//!   writable client-facing Ledger gRPC server. No Node service, no
//!   replication. Returns [`Handles::Standalone`].
//! - **Clustered**: builds a [`RoleSupervisor`] which owns the
//!   long-lived gRPC servers and the role-state atomics, then
//!   dispatches the boot role's role-specific tasks. Returns
//!   [`Handles::Cluster`].
//!
//! Stage 3b: the cluster path always goes through the supervisor.
//! Per-role bring-ups (`Leader`, future `Follower`/`Candidate`) no
//! longer own gRPC servers — only their role-task sub-trees.

use crate::cluster::config::Config;
use crate::cluster::server::Server;
use crate::cluster::supervisor::{RoleSupervisor, SupervisorHandles};
use crate::cluster::{
    ClusterCommitIndex, LedgerSlot, Quorum, RoleFlag, Term, role_flag::Role,
};
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::task::JoinHandle;

pub struct ClusterNode {
    config: Config,
    /// Wrapping the live `Arc<Ledger>` in a slot lets the
    /// supervisor swap it during a divergence reseed (ADR-0016 §9)
    /// without tearing down the gRPC servers.
    ledger_slot: Arc<LedgerSlot>,
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
            ledger_slot: Arc::new(LedgerSlot::new(Arc::new(ledger))),
            term,
        })
    }

    /// Return the **currently-live** `Arc<Ledger>`. After a
    /// divergence reseed the returned `Arc` may differ from the one
    /// observed before — callers must not retain it across reseeds.
    pub fn ledger(&self) -> Arc<Ledger> {
        self.ledger_slot.ledger()
    }

    /// Internal accessor to the slot itself (for the supervisor).
    pub fn ledger_slot(&self) -> &Arc<LedgerSlot> {
        &self.ledger_slot
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Dispatch to the standalone or clustered bring-up. Returns a
    /// unified `Handles`.
    pub async fn run(&self) -> Result<Handles, Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.is_clustered() {
            // Standalone: just the writable client-facing Ledger gRPC.
            // Construct a Leader-pinned `RoleFlag` so the LedgerHandler
            // accepts writes; nothing else mutates it in standalone.
            let client_addr = self.config.server.socket_addr()?;
            let role = Arc::new(RoleFlag::new(Role::Leader));
            let cluster_commit_index = ClusterCommitIndex::new();
            let server = Server::new(
                self.ledger_slot.clone(),
                client_addr,
                role,
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

        // Clustered: hand off to the supervisor, which owns the
        // long-lived gRPC servers + role-specific dispatch.
        let supervisor = RoleSupervisor::new(
            self.config.clone(),
            self.ledger_slot.clone(),
            self.term.clone(),
        )?;
        let handles = supervisor.run().await?;
        Ok(Handles::Cluster(handles))
    }
}

/// Unified handles view across standalone / clustered bring-ups.
pub enum Handles {
    Standalone(StandaloneHandles),
    Cluster(SupervisorHandles),
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
            Handles::Cluster(h) => h.abort(),
        }
    }

    /// Shared quorum tracker (clustered + currently-Leader only).
    /// `None` in standalone or when the cluster booted in a
    /// non-Leader role (Initializing).
    pub fn quorum(&self) -> Option<Arc<Quorum>> {
        match self {
            Handles::Cluster(h) => h.leader.as_ref().map(|l| l.quorum.clone()),
            _ => None,
        }
    }

    /// Cooperative shutdown flag for clustered peer sub-tasks.
    pub fn running(&self) -> Option<Arc<AtomicBool>> {
        match self {
            Handles::Cluster(h) => Some(h.running.clone()),
            _ => None,
        }
    }

    /// Borrow the supervisor handles for a clustered bring-up.
    pub fn as_cluster(&self) -> Option<&SupervisorHandles> {
        match self {
            Handles::Cluster(h) => Some(h),
            _ => None,
        }
    }

    /// Client-facing Ledger gRPC server handle (always present).
    pub fn client_handle(&self) -> &JoinHandle<()> {
        match self {
            Handles::Standalone(h) => &h.client_handle,
            Handles::Cluster(h) => &h.client_handle,
        }
    }

    /// Peer-facing Node gRPC server handle. `None` in standalone
    /// mode (no Node service runs).
    pub fn node_handle(&self) -> Option<&JoinHandle<()>> {
        match self {
            Handles::Standalone(_) => None,
            Handles::Cluster(h) => Some(&h.node_handle),
        }
    }
}
