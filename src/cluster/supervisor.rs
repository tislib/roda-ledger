//! `RoleSupervisor` — owns the long-lived gRPC servers and the
//! shared role/state atomics, dispatches role-specific bring-ups
//! across transitions (ADR-0016 §2 / §3).
//!
//! What's in scope for **Stage 3b** (this file as it stands):
//!
//! - Spawn the client-facing Ledger gRPC server **once** at startup.
//!   It reads `Arc<RoleFlag>` on every RPC; the supervisor flips
//!   the flag on transitions, so the same server serves Leader /
//!   Follower / Initializing without restart.
//! - Spawn the peer-facing Node gRPC server **once** at startup
//!   (only in clustered mode). Same shared-flag pattern.
//! - Carry an `ElectionTimer` skeleton per ADR-0016 §3.8. Stage 3b
//!   constructs it but does not consume the expiry; Stage 4 wires
//!   it up to the Candidate transition.
//! - Boot-role decision (ADR-0016 §2):
//!   - Single-node cluster (`peers.len() == 1`) → `Role::Leader`.
//!   - Multi-node cluster → `Role::Initializing`. No transition out
//!     of Initializing exists in Stage 3b, so multi-node clusters
//!     wait silently here until Stage 4 elections drive them.
//!
//! What's deferred to **Stage 3c**: divergence-driven `Ledger`
//! reseed. The supervisor will read `NodeHandlerCore::take_divergence_watermark`
//! and rebuild the Ledger via `start_with_recovery_until`.
//!
//! What's deferred to **Stage 4**: Candidate / Leader transitions
//! driven by `RequestVote`, term-bump-on-candidacy, election-timer
//! expiry → Candidate.

use crate::cluster::config::Config;
use crate::cluster::leader::{Leader, LeaderHandles};
use crate::cluster::node_handler::NodeHandlerCore;
use crate::cluster::role_flag::Role;
use crate::cluster::server::{NodeServerRuntime, Server};
use crate::cluster::{ClusterCommitIndex, NodeHandler, RoleFlag, Term, Vote};
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::task::JoinHandle;

/// Output of [`RoleSupervisor::run`] — the long-lived task handles
/// kept alive for the process lifetime, plus the shared shutdown
/// flag callers use to drain peer tasks (post-Stage 4 transitions
/// will drop the leader sub-tree without touching this).
pub struct SupervisorHandles {
    /// Client-facing Ledger gRPC server. Long-lived.
    pub client_handle: JoinHandle<()>,
    /// Peer-facing Node gRPC server. Long-lived. Always present in
    /// clustered mode.
    pub node_handle: JoinHandle<()>,
    /// Per-role transient bring-up. In Stage 3b this is `Some` only
    /// for the seed-leader case (single-node cluster); multi-node
    /// nodes sit in Initializing and produce `None` here. Stage 4
    /// fills this in for elected leaders.
    pub leader: Option<LeaderHandles>,
    /// Cooperative shutdown flag for peer-replication sub-tasks
    /// (leader only). Cluster-level shutdown drains peer tasks via
    /// this; gRPC servers are aborted via their `JoinHandle`.
    pub running: Arc<AtomicBool>,
}

impl SupervisorHandles {
    /// Stop every long-lived task spawned by the supervisor.
    pub fn abort(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);
        if let Some(leader) = &self.leader {
            for h in &leader.peer_handles {
                h.abort();
            }
        }
        self.client_handle.abort();
        self.node_handle.abort();
    }
}

pub struct RoleSupervisor {
    config: Config,
    ledger: Arc<Ledger>,
    term: Arc<Term>,
    /// Durable Raft persistent vote (ADR-0016 §4). Stage 4's
    /// `RequestVote` handler reads/writes this; Stage 3b just opens
    /// it to keep the persistent state consistent across modes.
    vote: Arc<Vote>,
    /// Shared role atomic. Threaded into every handler at construction
    /// time; the supervisor is the only writer.
    role: Arc<RoleFlag>,
}

impl RoleSupervisor {
    pub fn new(config: Config, ledger: Arc<Ledger>, term: Arc<Term>) -> std::io::Result<Self> {
        let vote = Arc::new(Vote::open_in_dir(&config.ledger.storage.data_dir)?);
        // Boot-role rule (ADR-0016 §2). Standalone never reaches the
        // supervisor — the standalone path lives in `ClusterNode::run`.
        let initial_role = match config.cluster.as_ref() {
            Some(cluster) if cluster.peers.len() == 1 => {
                // Single-node cluster (only self): boot straight to
                // Leader. Avoids an election round-trip for the
                // degenerate case where there's no one to ask.
                Role::Leader
            }
            Some(_) => {
                // Multi-node cluster: stay Initializing until Stage 4
                // elections decide otherwise.
                Role::Initializing
            }
            None => {
                // Should not happen — `ClusterNode::run` only
                // constructs a supervisor when `is_clustered()`.
                Role::Initializing
            }
        };
        Ok(Self {
            config,
            ledger,
            term,
            vote,
            role: Arc::new(RoleFlag::new(initial_role)),
        })
    }

    /// Spawn the long-lived gRPC servers, then dispatch the initial
    /// role-specific bring-up. Returns `SupervisorHandles` carrying
    /// every task handle the cluster process needs.
    ///
    /// In Stage 3b there is no transition loop — once we land in a
    /// role, we stay there. Stage 4 wraps this dispatch in a `loop`
    /// that re-runs on each transition.
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

        // Cluster-commit watermark. Built once and shared between the
        // LedgerHandler (which exposes it via wait levels and
        // `GetPipelineIndex`) and the NodeHandler (which advances it
        // on every successful AppendEntries). Leaders feed it from
        // their own `Quorum` via the ledger's commit hook
        // (Stage 3c work).
        let cluster_commit_index = ClusterCommitIndex::new();

        // Build the long-lived NodeHandlerCore once. Every gRPC RPC
        // (and the Stage 4 election machinery) shares this Arc.
        let node_core = Arc::new(NodeHandlerCore::new(
            self.ledger.clone(),
            self.config.node_id(),
            self.term.clone(),
            self.vote.clone(),
            self.role.clone(),
            Some(cluster_commit_index.clone()),
        ));

        // ── Long-lived client-facing Ledger gRPC server ───────────────
        let client_server = Server::new(
            self.ledger.clone(),
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

        // ── Initial role dispatch ─────────────────────────────────────
        let leader_handles = match self.role.get() {
            Role::Leader => {
                // Seed-leader path (single-node cluster in Stage 3b).
                // Spawn peer-replication tasks. The Leader bring-up
                // no longer owns gRPC servers — supervisor does.
                let leader = Leader::new(
                    self.config.clone(),
                    self.ledger.clone(),
                    self.term.clone(),
                );
                Some(leader.run_role_tasks(running.clone()).await?)
            }
            Role::Initializing => {
                // Multi-node cluster, pre-election. Nothing role-specific
                // to spawn yet — gRPC servers above are already up.
                // Stage 4 turns the election-timer expiry here into a
                // Candidate transition.
                info!(
                    "supervisor: node_id={} entered Initializing (peers={})",
                    self.config.node_id(),
                    cluster.peers.len()
                );
                None
            }
            r => {
                // Follower / Candidate aren't reachable from the
                // boot-role rule; reserved for Stage 4.
                info!("supervisor: unexpected boot role {:?}", r);
                None
            }
        };

        Ok(SupervisorHandles {
            client_handle,
            node_handle,
            leader: leader_handles,
            running,
        })
    }
}
