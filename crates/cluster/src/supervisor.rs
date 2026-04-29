//! `RoleSupervisor` — owns the long-lived gRPC servers and the raft
//! driver (ADR-0017). After the migration the supervisor's job
//! collapsed: the role state machine lives entirely inside the new
//! `raft` crate, driven by [`RaftHandle`]. The supervisor's
//! responsibility is now:
//!
//! 1. Open durable persistence and the cluster mirror.
//! 2. Build [`RaftHandle`].
//! 3. Spawn the long-lived **client** + **node** gRPC servers (these
//!    stay up across every role transition).
//! 4. Register the ledger's `on_commit` hook so local commits feed
//!    `Event::LocalCommitAdvanced` into the raft state machine.
//! 5. Fire one initial `Tick` so raft arms its election timer.
//!
//! Lifecycle is RAII: dropping `SupervisorHandles` flips every
//! cooperative flag, fires the gRPC servers' shutdown notifies, drains
//! the raft driver, and awaits all task handles for graceful
//! completion.

use crate::cluster_mirror::ClusterMirror;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::lifecycle::drain_in_drop;
use crate::node_handler::NodeHandlerCore;
use crate::raft_driver::{register_on_commit_hook, RaftHandle};
use crate::server::{NodeServerRuntime, Server};
use crate::{LedgerSlot, NodeHandler};
use spdlog::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Output of [`RoleSupervisor::run`].
///
/// Lifecycle is RAII: dropping a `SupervisorHandles` *is* the shutdown.
/// The `Drop` impl flips every cooperative flag, fires the gRPC servers'
/// shutdown notifies, drains the raft driver, and awaits all spawned
/// tasks for graceful completion (bounded by the timeout in
/// [`drain_in_drop`]). There is no separate `shutdown()` method.
pub struct SupervisorHandles {
    pub client_handle: Option<JoinHandle<()>>,
    pub node_handle: Option<JoinHandle<()>>,
    /// Cooperative shutdown flag mirroring `node_core.shutdown` for
    /// observability. Currently no consumer reads it externally; kept
    /// for symmetry with the standalone path.
    pub running: Arc<AtomicBool>,
    /// Lock-free read surface for `LedgerHandler` and external
    /// consumers (tests).
    pub mirror: Arc<ClusterMirror>,
    /// The raft driver. RAII-shut by dropping the last `Arc`.
    pub raft: Arc<RaftHandle>,
    /// Shared NodeHandler state. `Drop` flips its `shutdown` latch so
    /// any in-flight or subsequent gRPC handler refuses immediately —
    /// this is the in-process stand-in for "the process is gone".
    pub node_core: Arc<NodeHandlerCore>,
    /// Cooperative shutdown trigger for the client-facing Ledger
    /// gRPC server. `Drop` calls `notify_waiters()` so
    /// `serve_with_shutdown` resolves and the server task exits.
    pub client_shutdown: Arc<Notify>,
    /// Cooperative shutdown trigger for the peer-facing Node gRPC
    /// server.
    pub node_shutdown: Arc<Notify>,
}

impl Drop for SupervisorHandles {
    fn drop(&mut self) {
        // 1. Sync signals — fast, never block.
        //    Flip the gRPC-handler shutdown latch FIRST so any
        //    in-flight or newly-arriving handler returns
        //    `Status::unavailable` immediately. This severs the
        //    dying-follower's ability to keep its slot fresh in the
        //    leader's quorum view before we even start awaiting tasks.
        self.node_core
            .shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
        // Tell the raft driver to stop spawning new detached subtasks
        // (outbound RPC reply tasks, wakeup re-arms). Existing in-flight
        // ones complete naturally; new ones are no-ops.
        self.raft.drain();
        // Stop both gRPC servers from accepting new connections.
        self.client_shutdown.notify_waiters();
        self.node_shutdown.notify_waiters();

        // 2. Drain in dependency order: gRPC servers stop accepting
        //    work, in-flight handlers drain (they may still touch the
        //    raft mutex, which is fine — raft's `step` is short).
        let client = self.client_handle.take();
        let node = self.node_handle.take();
        drain_in_drop("supervisor", [client, node]);
        // 3. The Arc<RaftHandle> drops with the SupervisorHandles
        //    fields; its WakeupTask::Drop aborts the pending sleeper
        //    and detached outbound tasks self-terminate via Weak<Self>.
    }
}

pub struct RoleSupervisor {
    config: Arc<Config>,
    ledger_slot: Arc<LedgerSlot>,
    durable: Arc<DurablePersistence>,
    mirror: Arc<ClusterMirror>,
}

impl RoleSupervisor {
    pub fn new(
        config: Arc<Config>,
        ledger_slot: Arc<LedgerSlot>,
        durable: Arc<DurablePersistence>,
    ) -> std::io::Result<Self> {
        let mirror = ClusterMirror::new();
        Ok(Self {
            config,
            ledger_slot,
            durable,
            mirror,
        })
    }

    pub async fn run(&self) -> Result<SupervisorHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("RoleSupervisor::run requires a clustered config");

        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;

        // Build the raft driver. This snapshots initial state into the
        // mirror; the first `Tick` (fired below) will arm the election
        // timer and emit a `SetWakeup` action that the driver schedules.
        let raft = RaftHandle::open(
            self.config.clone(),
            self.ledger_slot.clone(),
            self.durable.clone(),
            self.mirror.clone(),
        )?;

        // Register the on-commit hook on the initial ledger. The hook is
        // a single `fetch_max` into a shared atomic — no async, no spawn.
        // Reseed (Action::TruncateLog) re-registers on every replacement
        // ledger inside the driver's reseed path.
        register_on_commit_hook(&self.ledger_slot.ledger(), raft.pending_local_commit());

        // Construct the gRPC handler core with the new raft handle.
        let node_core = Arc::new(NodeHandlerCore::new(
            self.ledger_slot.clone(),
            cluster.node.node_id,
            self.mirror.clone(),
            raft.clone(),
        ));

        let client_shutdown = Arc::new(Notify::new());
        let node_shutdown = Arc::new(Notify::new());

        // Client-facing Ledger gRPC server.
        let client_server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            self.mirror.clone(),
            self.durable.term.clone(),
            client_shutdown.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("supervisor: ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node gRPC server.
        let node_handler = NodeHandler::new(node_core.clone());
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(
            node_addr,
            node_handler,
            node_max_bytes,
            node_shutdown.clone(),
        );
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("supervisor: node gRPC server exited: {}", e);
            }
        });

        let running = Arc::new(AtomicBool::new(true));

        // Fire the initial Tick to arm the election timer. The driver's
        // `step(Tick)` returns at minimum a `SetWakeup` action that the
        // driver schedules; from there raft drives itself via the
        // wakeup task → `on_tick` → next `SetWakeup` cycle.
        debug!(
            "supervisor[node_id={}]: kicking initial tick to arm election timer",
            cluster.node.node_id
        );
        raft.on_tick().await;

        info!(
            "supervisor: cluster bring-up complete (node_id={}, peers={})",
            cluster.node.node_id,
            cluster.peers.len()
        );

        Ok(SupervisorHandles {
            client_handle: Some(client_handle),
            node_handle: Some(node_handle),
            running,
            mirror: self.mirror.clone(),
            raft,
            node_core,
            client_shutdown,
            node_shutdown,
        })
    }
}
