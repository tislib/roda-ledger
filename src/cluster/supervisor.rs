//! `RoleSupervisor` — owns the long-lived gRPC servers + the shared
//! role/state atomics, dispatches role-specific bring-ups across
//! transitions (ADR-0016 §2 / §3), and reseeds the embedded `Ledger`
//! on divergence detection (ADR-0016 §9).
//!
//! Stage 3c additions over 3b:
//!
//! - The supervisor reads / writes `Arc<LedgerSlot>` rather than
//!   `Arc<Ledger>` directly. Every gRPC handler receives the same
//!   slot, so swapping the underlying `Arc<Ledger>` is observed
//!   atomically across the process.
//! - On startup the supervisor spawns a **divergence watcher** task
//!   that polls `NodeHandlerCore::take_divergence_watermark()` at a
//!   small fixed cadence. When the handler stashes a watermark,
//!   the watcher kicks `reseed(watermark)`.
//! - `reseed`:
//!   1. Sets the role to `Initializing` so the freshly-started
//!      stages won't accept writes mid-rebuild.
//!   2. Constructs a new `Ledger` and runs
//!      `start_with_recovery_until(watermark)` (which truncates WAL
//!      bytes above `watermark` and rebuilds balances from the
//!      surviving prefix).
//!   3. Swaps the new `Arc<Ledger>` into the slot.
//!   4. Drops the old `Arc<Ledger>` on the supervisor's task — its
//!      `Drop` joins the previous pipeline threads here, **not**
//!      inside an unrelated handler thread.
//!
//! What's deferred to **Stage 4**: real elections — `RequestVote`,
//! candidate loop, election-timer expiry → Candidate.

use crate::cluster::config::Config;
use crate::cluster::leader::{Leader, LeaderHandles};
use crate::cluster::node_handler::NodeHandlerCore;
use crate::cluster::role_flag::Role;
use crate::cluster::server::{NodeServerRuntime, Server};
use crate::cluster::{ClusterCommitIndex, LedgerSlot, NodeHandler, RoleFlag, Term, Vote};
use crate::ledger::Ledger;
use spdlog::{error, info, warn};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Output of [`RoleSupervisor::run`].
pub struct SupervisorHandles {
    /// Client-facing Ledger gRPC server. Long-lived.
    pub client_handle: JoinHandle<()>,
    /// Peer-facing Node gRPC server. Long-lived. Always present in
    /// clustered mode.
    pub node_handle: JoinHandle<()>,
    /// Divergence-watcher loop. Long-lived. Polls
    /// `NodeHandlerCore::take_divergence_watermark` and triggers
    /// reseed.
    pub watcher_handle: JoinHandle<()>,
    /// Per-role transient bring-up. In Stage 3b this is `Some` only
    /// for the seed-leader case (single-node cluster); multi-node
    /// nodes sit in Initializing and produce `None` here.
    pub leader: Option<LeaderHandles>,
    /// Cooperative shutdown flag for peer-replication sub-tasks.
    pub running: Arc<AtomicBool>,
}

impl SupervisorHandles {
    /// Stop every long-lived task spawned by the supervisor.
    pub fn abort(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(leader) = &self.leader {
            for h in &leader.peer_handles {
                h.abort();
            }
        }
        self.watcher_handle.abort();
        self.client_handle.abort();
        self.node_handle.abort();
    }
}

pub struct RoleSupervisor {
    config: Config,
    ledger_slot: Arc<LedgerSlot>,
    term: Arc<Term>,
    /// Durable Raft persistent vote (ADR-0016 §4). Stage 4's
    /// `RequestVote` handler reads/writes this; Stage 3b/3c hold
    /// it for forward compatibility.
    vote: Arc<Vote>,
    role: Arc<RoleFlag>,
}

impl RoleSupervisor {
    pub fn new(
        config: Config,
        ledger_slot: Arc<LedgerSlot>,
        term: Arc<Term>,
    ) -> std::io::Result<Self> {
        let vote = Arc::new(Vote::open_in_dir(&config.ledger.storage.data_dir)?);
        let initial_role = match config.cluster.as_ref() {
            Some(cluster) if cluster.peers.len() == 1 => Role::Leader,
            Some(_) => Role::Initializing,
            None => Role::Initializing,
        };
        Ok(Self {
            config,
            ledger_slot,
            term,
            vote,
            role: Arc::new(RoleFlag::new(initial_role)),
        })
    }

    /// Spawn the long-lived gRPC servers + the divergence watcher,
    /// then dispatch the boot-role's role-specific tasks.
    pub async fn run(
        &self,
    ) -> Result<SupervisorHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("RoleSupervisor::run requires a clustered config");

        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;

        let cluster_commit_index = ClusterCommitIndex::new();

        let node_core = Arc::new(NodeHandlerCore::new(
            self.ledger_slot.clone(),
            self.config.node_id(),
            self.term.clone(),
            self.vote.clone(),
            self.role.clone(),
            Some(cluster_commit_index.clone()),
        ));

        // ── Long-lived client-facing Ledger gRPC server ───────────────
        let client_server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            self.role.clone(),
            self.term.clone(),
            cluster_commit_index.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("supervisor: ledger gRPC server exited: {}", e);
            }
        });

        // ── Long-lived peer-facing Node gRPC server ───────────────────
        let node_handler = NodeHandler::new(node_core.clone());
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("supervisor: node gRPC server exited: {}", e);
            }
        });

        let running = Arc::new(AtomicBool::new(true));

        // ── Divergence watcher (ADR-0016 §9) ──────────────────────────
        let watcher_handle = self.spawn_divergence_watcher(node_core.clone(), running.clone());

        // ── Initial role dispatch ─────────────────────────────────────
        let leader_handles = match self.role.get() {
            Role::Leader => {
                let leader = Leader::new(
                    self.config.clone(),
                    self.ledger_slot.clone(),
                    self.term.clone(),
                );
                Some(leader.run_role_tasks(running.clone()).await?)
            }
            Role::Initializing => {
                info!(
                    "supervisor: node_id={} entered Initializing (peers={})",
                    self.config.node_id(),
                    cluster.peers.len()
                );
                None
            }
            r => {
                info!("supervisor: unexpected boot role {:?}", r);
                None
            }
        };

        Ok(SupervisorHandles {
            client_handle,
            node_handle,
            watcher_handle,
            leader: leader_handles,
            running,
        })
    }

    /// Drive the divergence-watcher loop. Polls
    /// `NodeHandlerCore::take_divergence_watermark()` every
    /// [`Self::WATCHER_INTERVAL`]; on `Some(watermark)` it triggers a
    /// reseed.
    fn spawn_divergence_watcher(
        &self,
        node_core: Arc<NodeHandlerCore>,
        running: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let config = self.config.clone();
        let ledger_slot = self.ledger_slot.clone();
        let role = self.role.clone();
        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(watermark) = node_core.take_divergence_watermark() {
                    info!(
                        "supervisor: divergence detected (watermark={}); reseeding ledger",
                        watermark
                    );
                    if let Err(e) = reseed(&config, &ledger_slot, &role, watermark).await {
                        error!("supervisor: reseed failed: {}", e);
                    } else {
                        info!(
                            "supervisor: reseed complete (last_commit_id now={})",
                            ledger_slot.ledger().last_commit_id()
                        );
                    }
                }
                tokio::time::sleep(Self::WATCHER_INTERVAL).await;
            }
        })
    }

    const WATCHER_INTERVAL: Duration = Duration::from_millis(10);
}

/// Build a fresh `Ledger`, run `start_with_recovery_until(watermark)`,
/// and atomically swap it into the slot. Returns the old
/// `Arc<Ledger>` so the caller can drop it on its own task — the
/// `Drop` joins the previous pipeline threads, which we never want
/// to run inside a gRPC handler thread.
async fn reseed(
    config: &Config,
    ledger_slot: &Arc<LedgerSlot>,
    role: &Arc<RoleFlag>,
    watermark: u64,
) -> Result<(), std::io::Error> {
    // Stage 3c only handles the Initializing / Follower paths. If a
    // future stage adds a Leader-side reseed, drain peer tasks here
    // first (`running.store(false)` etc.) before swapping the slot.
    let prior_role = role.get();
    role.set(Role::Initializing);

    // Build the new Ledger off-thread so the runtime stays
    // responsive. `Ledger::start_with_recovery_until` is
    // synchronous; spawn_blocking keeps the tokio worker free.
    let ledger_cfg = config.ledger.clone();
    let new_ledger: Arc<Ledger> = tokio::task::spawn_blocking(move || -> Result<Arc<Ledger>, std::io::Error> {
        let mut ledger = Ledger::new(ledger_cfg);
        ledger.start_with_recovery_until(watermark)?;
        Ok(Arc::new(ledger))
    })
    .await
    .map_err(|e| std::io::Error::other(format!("reseed: spawn_blocking panicked: {}", e)))??;

    // Atomic swap. In-flight RPCs that already cloned the previous
    // `Arc` continue against the old ledger; new RPCs see the new
    // one. `_old` falls out of scope at the end of this function on
    // the supervisor's own task, so the synchronous `Ledger::Drop`
    // (which joins pipeline threads) runs here, not inside a
    // handler.
    let _old = ledger_slot.replace(new_ledger);

    // Restore role only if the supervisor wanted us back; in
    // practice the watcher leaves us in Initializing until Stage 4
    // elections drive us out. If the caller had been Leader on a
    // single-node cluster we restore — that path is reserved for
    // future stages.
    if matches!(prior_role, Role::Leader) {
        // Single-node Leader can re-promote immediately; Stage 4
        // adds the proper election-driven path.
        role.set(Role::Leader);
    } else {
        // Initializing / Follower: stay in Initializing until the
        // next AppendEntries from a (real, post-Stage-4) leader
        // resyncs us.
        warn!(
            "supervisor: reseed left node in Initializing (prior role {:?})",
            prior_role
        );
    }
    Ok(())
}
