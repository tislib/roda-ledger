//! `ClusterNode` — top-level entry point for both deployment shapes.
//!
//! - **Standalone** (`config.cluster.is_none()`): spawns only the
//!   writable client-facing Ledger gRPC server. No Node service, no
//!   replication, no raft. Returns [`Handles::Standalone`].
//! - **Clustered**: builds a [`RoleSupervisor`] which owns the
//!   long-lived gRPC servers and the raft driver, then dispatches the
//!   boot role's role-specific tasks. Returns [`Handles::Cluster`].
//!
//! Per ADR-0017 §"Required Invariants" #4 the term log is **not**
//! bumped on boot — it stays at whatever the durable persistence layer
//! reports until an actual election win advances it. The legacy
//! `Term::new_term(start_tx)` boot-time bump is gone.
//!
//! Lifecycle is RAII: dropping a `Handles` (or `StandaloneHandles` /
//! `SupervisorHandles`) is the shutdown. There is no separate
//! `shutdown()` / `abort()` API.

use crate::cluster_mirror::ClusterMirror;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::lifecycle::drain_in_drop;
use crate::server::Server;
use crate::supervisor::{RoleSupervisor, SupervisorHandles};
use crate::LedgerSlot;
use ledger::ledger::Ledger;
use raft::Role;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct ClusterNode {
    config: Config,
    /// Wrapping the live `Arc<Ledger>` in a slot lets the
    /// supervisor swap it during a divergence reseed (ADR-0016 §9)
    /// without tearing down the gRPC servers.
    ledger_slot: Arc<LedgerSlot>,
    /// Durable term + vote logs. Always opened, even in standalone
    /// mode, so the cluster path can pick up where the standalone
    /// path left off (and vice-versa). Per ADR-0017 §"Required
    /// Invariants" #4 the term is **not** bumped on boot.
    durable: Arc<DurablePersistence>,
}

impl ClusterNode {
    /// Build (and start) the embedded Ledger and open the durable
    /// term + vote logs under `ledger.storage.data_dir`. The gRPC
    /// server(s) are launched by [`ClusterNode::run`].
    pub fn new(config: Config) -> std::io::Result<Self> {
        let mut ledger = Ledger::new(config.ledger.clone());
        ledger.start()?;
        let durable = Arc::new(DurablePersistence::open(&config.ledger.storage.data_dir)?);
        info!(
            "cluster::new: opened durable persistence (term={}, voted_for={:?}, clustered={})",
            durable.term.get_current_term(),
            durable.vote.get_voted_for(),
            config.is_clustered()
        );
        Ok(Self {
            config,
            ledger_slot: Arc::new(LedgerSlot::new(Arc::new(ledger))),
            durable,
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
            // Construct a Leader-pinned `ClusterMirror` so the
            // LedgerHandler accepts writes; raft never runs in this
            // mode.
            let client_addr = self.config.server.socket_addr()?;
            let mirror = ClusterMirror::new();
            mirror.set_role_for_standalone(Role::Leader);
            let client_running = Arc::new(AtomicBool::new(true));
            let client_shutdown = Arc::new(Notify::new());
            let server = Server::new(
                self.ledger_slot.clone(),
                client_addr,
                mirror,
                self.durable.term.clone(),
                client_shutdown.clone(),
            );
            let client_handle = tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("standalone ledger gRPC server exited: {}", e);
                }
            });
            info!("standalone: client-facing Ledger gRPC up");
            return Ok(Handles::Standalone(StandaloneHandles {
                client_handle: Some(client_handle),
                client_running,
                client_shutdown,
            }));
        }

        // Clustered: hand off to the supervisor.
        let supervisor = RoleSupervisor::new(
            Arc::new(self.config.clone()),
            self.ledger_slot.clone(),
            self.durable.clone(),
        )?;
        let handles = supervisor.run().await?;
        Ok(Handles::Cluster(handles))
    }
}

/// Unified handles view across standalone / clustered bring-ups.
///
/// Lifecycle is RAII: dropping the value triggers cooperative shutdown
/// of every spawned task this bring-up owns. There is no separate
/// `shutdown()` method.
pub enum Handles {
    Standalone(StandaloneHandles),
    Cluster(SupervisorHandles),
}

/// Handles produced by a successful standalone bring-up. Single
/// gRPC server, nothing else.
pub struct StandaloneHandles {
    pub client_handle: Option<JoinHandle<()>>,
    /// Cooperative shutdown flag for the standalone gRPC server.
    pub client_running: Arc<AtomicBool>,
    /// Cooperative shutdown trigger for the client-facing Ledger
    /// gRPC server.
    pub client_shutdown: Arc<Notify>,
}

impl Drop for StandaloneHandles {
    fn drop(&mut self) {
        self.client_running.store(false, Ordering::Release);
        self.client_shutdown.notify_waiters();
        let client = self.client_handle.take();
        drain_in_drop("standalone", [client]);
    }
}

impl Handles {
    /// Cooperative shutdown flag for clustered peer sub-tasks.
    pub fn running(&self) -> Option<Arc<AtomicBool>> {
        match self {
            Handles::Cluster(h) => Some(h.running.clone()),
            _ => None,
        }
    }

    /// Lock-free read surface for raft state (clustered only). Returns
    /// `None` in standalone mode. Use `mirror.cluster_commit_index()`
    /// to read the quorum-committed watermark, `mirror.role()` for the
    /// current role, and so on.
    pub fn mirror(&self) -> Option<Arc<ClusterMirror>> {
        match self {
            Handles::Cluster(h) => Some(h.mirror.clone()),
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

    /// Whether the peer-facing Node gRPC server task is present.
    /// `false` in standalone mode (no Node service runs).
    pub fn has_node_handle(&self) -> bool {
        matches!(self, Handles::Cluster(_))
    }
}
