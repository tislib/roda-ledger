//! `Leader` — role-specific bring-up for the leader side of the cluster.
//!
//! Owns everything the leader role needs: the client-facing Ledger gRPC
//! server (writable), the peer-facing Node gRPC server (role = Leader),
//! and direct supervision of one [`PeerReplication`] sub-task per peer.
//! The top-level `tokio::spawn` for peer supervision lives here —
//! `Leader::run` spawns one child per peer.

use crate::cluster::config::Config;
use crate::cluster::node_handler::NodeHandler;
use crate::cluster::peer_replication::{PeerReplication, ReplicationParams};
use crate::cluster::proto::node::NodeRole;
use crate::cluster::server::{NodeServerRuntime, Server};
use crate::cluster::{Quorum, Term};
use crate::ledger::Ledger;
use spdlog::{error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Role-scoped bring-up for a leader node. Construct, then `run()`.
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

    /// Spawn every task the leader role needs. Returns a `LeaderHandles`
    /// carrying the spawned handles plus shared shutdown/observability
    /// state (`running`, `majority`, `peer_handles`).
    ///
    /// Also bumps the leader's durable term by 1 at bring-up. This is a
    /// temporary stand-in for a real election — see the ADR-016 work.
    /// `start_tx_id` is seeded from the ledger's current `last_commit_id`
    /// so `Term::get_term_at_tx` can answer for any subsequent write.
    pub async fn run(&self) -> Result<LeaderHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("Leader::run requires a clustered config");
        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;

        // Term bumping happens in `Ledger::start` for both standalone
        // and clustered paths so the term log is always one step
        // ahead of the next write. Read the (now-current) term for
        // logging only — no further bump here.
        info!(
            "leader: started under term {} (node_id={})",
            self.term.get_current_term(),
            self.config.node_id()
        );

        // Quorum sized to the full cluster (peers list now includes
        // self under ADR-0016 §1, so no `+1` adjustment is needed).
        let quorum = Arc::new(Quorum::new(self.config.cluster_size()));
        let cluster_commit_index = quorum.cluster_commit_index();

        // Client-facing Ledger server — full read/write on the leader.
        // Hands the shared Arc<Term> through so every submit and status
        // response can resolve the current + per-tx term without
        // round-tripping back to the leader state.
        let client_server = Server::new(
            self.ledger.clone(),
            client_addr,
            self.term.clone(),
            cluster_commit_index,
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("leader ledger gRPC server exited: {}", e);
            }
        });

        // Peer-facing Node server. The leader advertises NodeRole::Leader
        // so followers that misconfigure themselves as peer-targets of
        // this node get `RejectNotFollower` rather than silent success.
        let node_handler = NodeHandler::new(
            self.ledger.clone(),
            self.config.node_id(),
            self.term.clone(),
            NodeRole::Leader,
            None,
        );
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("leader node gRPC server exited: {}", e);
            }
        });

        // Replication: one child task per *other* peer (self is
        // excluded), all sharing one `Quorum` and one cooperative
        // shutdown flag. Slot layout in the quorum tracker is
        // positional with `config.cluster.peers`:
        //   [k] → the peer at index k in `config.cluster.peers`
        // Self has its own slot (the index where its peer_id lives in
        // the membership list); the leader's own commit progress
        // updates that slot via `ledger.on_commit`. This makes the
        // quorum tracker symmetric across the cluster.
        let running = Arc::new(AtomicBool::new(true));

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
            // AppendEntries. Term bumps after peer-task spawn aren't
            // reflected today; ADR-016 will replace this with an
            // Arc<Term> read on each RPC.
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
            client_handle,
            node_handle,
            peer_handles,
            running,
            quorum,
        })
    }
}

/// Handles + shared state produced by a successful `Leader::run`.
pub struct LeaderHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
    /// One handle per peer replication task. Positional with `config.peers`.
    pub peer_handles: Vec<JoinHandle<()>>,
    /// Cooperative shutdown flag shared with every peer sub-task. Flip
    /// to drain peer tasks without aborting.
    pub running: Arc<AtomicBool>,
    /// Cluster-wide majority tracker. Callers read `quorum.get()` for
    /// the latest majority-committed index.
    pub quorum: Arc<Quorum>,
}

impl LeaderHandles {
    /// Stop every leader-role task. Tests use this; production paths can
    /// wire a real shutdown channel via `NodeServerRuntime::run_with_shutdown`.
    pub fn abort(&self) {
        self.client_handle.abort();
        self.node_handle.abort();
        self.running.store(false, Ordering::Release);
        for h in &self.peer_handles {
            h.abort();
        }
    }
}
