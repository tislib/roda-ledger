//! `Leader` — role-specific bring-up for the **role-task** part of
//! the leader role. Stage 3b moves the gRPC servers into
//! [`crate::cluster::supervisor::RoleSupervisor`] (so they can stay
//! up across role transitions); what remains here is the leader's
//! exclusive sub-tree:
//!
//! - one `PeerReplication` per peer in `cluster.peers` (excluding self),
//! - the cluster-wide `Arc<Quorum>` they advance,
//! - the `Ledger::on_commit` hook that publishes the leader's own
//!   commit watermark into its quorum slot.
//!
//! `LeaderHandles` carries the per-task `JoinHandle`s plus the shared
//! `Quorum` so tests + the load harness can read the cluster-wide
//! majority watermark.

use crate::cluster::config::Config;
use crate::cluster::peer_replication::{PeerReplication, ReplicationParams};
use crate::cluster::supervisor::TransitionTx;
use crate::cluster::{LedgerSlot, Quorum, Term};
use spdlog::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a leader node. Construct, then call
/// [`Leader::run_role_tasks`] from the supervisor.
///
/// The Quorum is **supervisor-owned** (Stage 4) and lives across
/// role transitions; Leader just resets stale peer slots on entry
/// and spawns the per-peer replication tasks. The `Ledger::on_commit`
/// hook is also supervisor-owned and re-registered on every ledger
/// swap, so it keeps publishing into the same Quorum across reseeds.
pub struct Leader {
    config: Config,
    ledger_slot: Arc<LedgerSlot>,
    term: Arc<Term>,
    quorum: Arc<Quorum>,
}

impl Leader {
    pub fn new(
        config: Config,
        ledger_slot: Arc<LedgerSlot>,
        term: Arc<Term>,
        quorum: Arc<Quorum>,
    ) -> Self {
        Self {
            config,
            ledger_slot,
            term,
            quorum,
        }
    }

    /// Spawn the role-specific tasks the leader needs (peer
    /// replication × N) and wire the on-commit hook that publishes
    /// the leader's local commit progress into its `Quorum` slot.
    /// Returns a `LeaderHandles` carrying the spawned handles plus
    /// the shared `Quorum`.
    ///
    /// The supervisor owns the gRPC servers and the cooperative
    /// `Arc<AtomicBool> running` flag — both come in as parameters
    /// so peer tasks can drain on supervisor shutdown.
    pub async fn run_role_tasks(
        &self,
        running: Arc<AtomicBool>,
        supervisor_running: Arc<AtomicBool>,
        transition_tx: TransitionTx,
    ) -> Result<LeaderHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("Leader::run_role_tasks requires a clustered config");

        info!(
            "leader: started under term {} (node_id={})",
            self.term.get_current_term(),
            self.config.node_id()
        );

        // The supervisor owns `self.quorum` for the process
        // lifetime. On Leader entry we wipe stale per-peer slots
        // (matches ADR-0016 §3.3a — leftover indices from a
        // previous leadership window or different leader's
        // perspective don't pollute the new leader's view). Self's
        // own slot is monotonic, untouched by `reset_peers`.
        let self_id = self.config.node_id();
        let self_slot: u32 = cluster
            .peers
            .iter()
            .position(|p| p.peer_id == self_id)
            .expect("validate() guarantees self is present in cluster.peers")
            as u32;
        self.quorum.reset_peers(self_slot);

        // Snapshot the live ledger once at bring-up. The on-commit
        // hook that publishes leader-commit progress into the
        // quorum is supervisor-owned and re-registered on every
        // ledger swap, so we don't touch it here.
        let ledger_for_leader = self.ledger_slot.ledger();
        // Seed the quorum from whatever the ledger has already
        // committed at bring-up time.
        self.quorum
            .advance(self_slot, ledger_for_leader.last_commit_id());

        let other_count = self.config.other_peers().count();
        let mut peer_handles: Vec<JoinHandle<()>> = Vec::with_capacity(other_count);

        if other_count == 0 {
            info!(
                "leader: node_id={} has no peers; no replication tasks started",
                self_id
            );
        } else {
            // Stamp the *current* term (post-bump) on outgoing
            // AppendEntries. Stage 4 will replace this snapshot with
            // an `Arc<Term>` read on each RPC so term bumps after
            // peer-task spawn are visible.
            let params = ReplicationParams::new(
                self_id,
                self.term.get_current_term(),
                cluster.append_entries_max_bytes,
                Duration::from_millis(cluster.replication_poll_ms.max(1)),
            );
            for (idx, peer) in cluster.peers.iter().enumerate() {
                if peer.peer_id == self_id {
                    continue;
                }
                let peer_slot = idx as u32;
                let replicator = PeerReplication::new(
                    peer.clone(),
                    peer_slot,
                    ledger_for_leader.clone(),
                    params.clone(),
                    running.clone(),
                    supervisor_running.clone(),
                    self.quorum.clone(),
                    transition_tx.clone(),
                );
                peer_handles.push(tokio::spawn(async move { replicator.run().await }));
            }
            info!(
                "leader: node_id={} replicating to {} peer(s)",
                self_id, other_count
            );
        }

        Ok(LeaderHandles { peer_handles })
    }
}

/// Handles + shared state produced by a successful
/// [`Leader::run_role_tasks`]. The supervisor owns the gRPC servers
/// and the long-lived `Quorum`; this is just the leader's
/// role-specific sub-tree.
pub struct LeaderHandles {
    /// One handle per peer replication task. Positional with
    /// `config.cluster.peers` (with self filtered out).
    pub peer_handles: Vec<JoinHandle<()>>,
}

impl LeaderHandles {
    /// Abort every peer task. The supervisor's gRPC servers are
    /// independent and have to be aborted separately.
    pub fn abort(&self) {
        for h in &self.peer_handles {
            h.abort();
        }
    }

    /// Cooperative drain — flip the supervisor's `running` flag and
    /// peer tasks exit on their next loop iteration. Caller is
    /// responsible for `await`ing `peer_handles` if drain order
    /// matters. Provided as a static helper so the supervisor can
    /// drain without holding a `&mut LeaderHandles`.
    pub fn drain(running: &Arc<AtomicBool>) {
        running.store(false, Ordering::Release);
    }
}
