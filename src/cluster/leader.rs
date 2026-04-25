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
use crate::cluster::{Quorum, Term};
use crate::ledger::Ledger;
use spdlog::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a leader node. Construct, then call
/// [`Leader::run_role_tasks`] from the supervisor.
pub struct Leader {
    config: Config,
    ledger: Arc<Ledger>,
    term: Arc<Term>,
}

impl Leader {
    pub fn new(config: Config, ledger: Arc<Ledger>, term: Arc<Term>) -> Self {
        Self {
            config,
            ledger,
            term,
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

        // Quorum sized to the full cluster (peers list now includes
        // self under ADR-0016 §1, so no `+1` adjustment is needed).
        let quorum = Arc::new(Quorum::new(self.config.cluster_size()));

        // Replication: one child task per *other* peer. Slot layout
        // in the quorum tracker is positional with
        // `config.cluster.peers`:
        //   [k] → the peer at index k in `config.cluster.peers`
        // Self has its own slot (the index where its peer_id lives in
        // the membership list); the leader's own commit progress
        // updates that slot via `ledger.on_commit`.
        let self_id = self.config.node_id();
        let self_slot: u32 = cluster
            .peers
            .iter()
            .position(|p| p.peer_id == self_id)
            .expect("validate() guarantees self is present in cluster.peers")
            as u32;

        // Hook the leader's own commit stream into the self slot.
        // Must be registered after the ledger is started (commits are
        // already flowing) but before peer tasks start ACKing. Seed
        // with the current commit id in case start-up committed
        // anything before we got here.
        let q_leader = quorum.clone();
        if self
            .ledger
            .on_commit(Arc::new(move |tx_id| q_leader.advance(self_slot, tx_id)))
            .is_err()
        {
            panic!("leader: on_commit handler already registered; skipping");
        }
        quorum.advance(self_slot, self.ledger.last_commit_id());

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
                    self.ledger.clone(),
                    params.clone(),
                    running.clone(),
                    quorum.clone(),
                );
                peer_handles.push(tokio::spawn(async move { replicator.run().await }));
            }
            info!(
                "leader: node_id={} replicating to {} peer(s)",
                self_id, other_count
            );
        }

        Ok(LeaderHandles {
            peer_handles,
            quorum,
        })
    }
}

/// Handles + shared state produced by a successful
/// [`Leader::run_role_tasks`]. The supervisor owns the gRPC servers;
/// this is just the leader's role-specific sub-tree.
pub struct LeaderHandles {
    /// One handle per peer replication task. Positional with
    /// `config.cluster.peers` (with self filtered out).
    pub peer_handles: Vec<JoinHandle<()>>,
    /// Cluster-wide majority tracker. Callers read `quorum.get()`
    /// for the latest majority-committed index.
    pub quorum: Arc<Quorum>,
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
