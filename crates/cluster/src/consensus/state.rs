use crate::config::Config;
use crate::consensus::durable::DurablePersistence;
use crate::consensus::replication::ReplicationInputStream;
use crate::ledger_slot::LedgerSlot;
use crate::waiter::Waiter;
use proto::node::{NodeRole, PingRequest, PingResponse, RequestVoteRequest, RequestVoteResponse};
use raft::{NodeId, RaftConfig, RaftNode, RequestVote, Role, Term, TxId};
use spdlog::{debug, trace};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use storage::{TermRecord, VoteRecord};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(super) struct HandshakeSnapshot {
    pub self_id: NodeId,
    pub term: Term,
    pub term_first_tx: TxId,
    pub prev_log_tx_id: TxId,
    pub prev_log_term: Term,
    pub leader_commit: TxId,
}

pub struct Consensus {
    pub(super) ledger: Arc<LedgerSlot>,
    pub(super) raft_node: Mutex<RaftNode<DurablePersistence>>,
    pub(super) durable: DurablePersistence,
    pub(super) config: Arc<Config>,

    // replication driver
    pub(super) replication_input_tx: mpsc::Sender<ReplicationInputStream>,
    pub(super) replication_input_rx: Mutex<Option<mpsc::Receiver<ReplicationInputStream>>>,

    // role watcher
    pub(super) role_tx: watch::Sender<Role>,
    node_id: u64,

    // Shared reactive index watches (owned by the Waiter). Read by the
    // replication sender / cluster-commit driver; fed by `record_cluster_commit`.
    waiter: Arc<Waiter>,
}

impl Consensus {
    pub fn new(
        config: Config,
        ledger: Arc<LedgerSlot>,
        waiter: Arc<Waiter>,
    ) -> Result<Self, String> {
        if config.is_clustered() {
            Consensus::new_clustered(config, ledger, waiter)
        } else {
            Consensus::new_singleton(config, ledger, waiter)
        }
    }

    pub fn new_clustered(
        config: Config,
        ledger: Arc<LedgerSlot>,
        waiter: Arc<Waiter>,
    ) -> Result<Self, String> {
        let cluster = config
            .cluster
            .as_ref()
            .expect("run_clustered requires a clustered config");

        let self_id = cluster.node.node_id;
        let durable = DurablePersistence::open(&config.ledger.storage.data_dir, self_id)
            .map_err(|e| format!("{}", e))?;
        let peer_ids: Vec<u64> = cluster.peers.iter().map(|p| p.peer_id).collect();
        let raft_cfg = RaftConfig::default();
        let seed = seed_for(self_id);

        let persistence = DurablePersistence {
            node_id: self_id,
            term: durable.term.clone(),
            vote: durable.vote.clone(),
        };
        let node = RaftNode::new(self_id, peer_ids.clone(), persistence, raft_cfg, seed);
        let raft_term = node.current_term();
        let (replication_input_tx, replication_input_rx) = mpsc::channel(64);
        let (role_tx, _) = watch::channel(node.role());

        debug!(
            "consensus[{}]: initialized clustered (peers={}, raft_term={})",
            self_id,
            peer_ids.len(),
            raft_term
        );
        Ok(Consensus {
            ledger,
            durable,
            raft_node: Mutex::new(node),
            config: Arc::new(config),
            replication_input_tx,
            replication_input_rx: Mutex::new(Some(replication_input_rx)),
            role_tx,
            node_id: self_id,
            waiter,
        })
    }

    pub fn new_singleton(
        config: Config,
        ledger: Arc<LedgerSlot>,
        waiter: Arc<Waiter>,
    ) -> Result<Self, String> {
        let self_id = 1;
        let durable = DurablePersistence::open(&config.ledger.storage.data_dir, self_id)
            .map_err(|e| format!("{}", e))?;
        let peer_ids = vec![1];
        let raft_cfg = RaftConfig::default();
        let seed = seed_for(self_id);

        let persistence = DurablePersistence {
            node_id: self_id,
            term: durable.term.clone(),
            vote: durable.vote.clone(),
        };
        let mut node = RaftNode::new(self_id, peer_ids, persistence, raft_cfg, seed);
        // Singleton has no other voters; drive the election to
        // completion synchronously so the node is Leader at boot
        // (otherwise client writes race the first election timeout).
        let now = Instant::now();
        node.election().tick(now);
        node.election()
            .start(now + std::time::Duration::from_secs(60));
        let role = node.role();

        let (replication_input_tx, replication_input_rx) = mpsc::channel(64);
        let (role_tx, _) = watch::channel(role);

        debug!(
            "consensus[{}]: initialized singleton (role={:?})",
            self_id, role
        );
        Ok(Consensus {
            ledger,
            durable,
            raft_node: Mutex::new(node),
            config: Arc::new(config),
            replication_input_tx,
            replication_input_rx: Mutex::new(Some(replication_input_rx)),
            role_tx,
            node_id: self_id,
            waiter,
        })
    }

    #[inline]
    pub(super) fn node_id(&self) -> u64 {
        self.node_id
    }

    pub(super) fn replication_leader_handshake_snapshot(
        &self,
        peer_id: NodeId,
        anchor_override: Option<TxId>,
    ) -> HandshakeSnapshot {
        self.self_advance();
        let snap = {
            let mut node = self.raft_node.lock().expect("raft mutex poisoned");
            let prev_log_tx_id = match anchor_override {
                Some(a) => a,
                None => node
                    .replication()
                    .peer(peer_id)
                    .map(|p| p.match_index())
                    .unwrap_or(0),
            };
            HandshakeSnapshot {
                self_id: node.self_id(),
                term: node.current_term(),
                term_first_tx: node.current_term_first_tx(),
                prev_log_tx_id,
                prev_log_term: node.term_at_tx(prev_log_tx_id).unwrap_or(0),
                leader_commit: node.cluster_commit_index(),
            }
        };
        debug!(
            "consensus[{}]: handshake snapshot for peer={} term={} prev_log_tx_id={} prev_log_term={} leader_commit={}",
            self.node_id,
            peer_id,
            snap.term,
            snap.prev_log_tx_id,
            snap.prev_log_term,
            snap.leader_commit
        );
        snap
    }

    pub fn request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let raft_req = RequestVote {
            from: req.candidate_id,
            term: req.term,
            last_tx_id: req.last_tx_id,
            last_term: req.last_term,
        };
        let result = {
            let mut node = self.raft_node.lock().expect("raft mutex poisoned");
            node.election()
                .handle_request_vote(Instant::now(), raft_req)
        };
        debug!(
            "consensus[{}]: request_vote from candidate={} term={} -> granted={} term={}",
            self.node_id, req.candidate_id, req.term, result.granted, result.term
        );
        self.notify_role();
        RequestVoteResponse {
            term: result.term,
            vote_granted: result.granted,
        }
    }

    pub fn role_subscribe(&self) -> watch::Receiver<Role> {
        self.role_tx.subscribe()
    }

    pub(super) fn notify_role(&self) {
        let role = self.raft_node.lock().expect("raft mutex poisoned").role();
        self.role_tx.send_if_modified(|cur| {
            if *cur != role {
                debug!("consensus[{}]: role changed to {:?}", self.node_id, role);
                *cur = role;
                true
            } else {
                false
            }
        });
    }

    pub fn ping(&self, req: PingRequest) -> PingResponse {
        let (node_id, term, role) = {
            let node = self.raft_node.lock().expect("raft mutex poisoned");
            (node.self_id(), node.current_term(), node.role())
        };
        PingResponse {
            node_id,
            term,
            last_tx_id: self.ledger.current().last_snapshot_id(),
            role: role_to_proto(role) as i32,
            nonce: req.nonce,
        }
    }

    pub fn current_term(&self) -> u64 {
        self.durable
            .term
            .get_current_term()
            .max(self.durable.vote.get_current_term())
    }

    pub fn get_term_at_tx(&self, tx_id: u64) -> io::Result<Option<TermRecord>> {
        self.durable.term.get_term_at_tx(tx_id)
    }

    pub fn is_leader(&self) -> bool {
        self.raft_node
            .lock()
            .expect("raft mutex poisoned")
            .role()
            .is_leader()
    }

    pub fn cluster_commit_index(&self) -> u64 {
        self.raft_node
            .lock()
            .expect("raft mutex poisoned")
            .cluster_commit_index()
    }

    /// Leader heartbeat cadence, sourced from the raft config (derived from the
    /// election timeout, not separately configurable). The replication sender
    /// paces idle keepalives off this.
    pub(crate) fn heartbeat_interval(&self) -> std::time::Duration {
        self.raft_node
            .lock()
            .expect("raft mutex poisoned")
            .heartbeat_interval()
    }

    /// Shared index watches (owned by the Waiter), used by the replication
    /// sender and the cluster-commit driver.
    pub(crate) fn waiter(&self) -> &Arc<Waiter> {
        &self.waiter
    }

    /// Push the current raft `cluster_commit_index` into the waiter's watch.
    /// Call at every site the RaftNode advances it. Non-blocking.
    pub(crate) fn publish_cluster_commit(&self) {
        self.waiter
            .record_cluster_commit(self.cluster_commit_index());
    }

    pub fn self_advance(&self) {
        let ledger_index = self.ledger.current().last_snapshot_id();
        let moved = self
            .raft_node
            .lock()
            .expect("raft mutex poisoned")
            .advance_local_index(ledger_index);
        if moved {
            trace!(
                "consensus[{}]: self_advance to {}",
                self.node_id, ledger_index
            );
        }
        // Local progress may have advanced quorum (self-slot); publish either way.
        self.publish_cluster_commit();
    }

    /// Reactive driver: on every snapshot-index advance, feed the leader's own
    /// progress into quorum via `self_advance` (which publishes cluster_commit).
    /// This is what advances cluster_commit for a singleton (no peers) and keeps
    /// the leader's self-slot fresh without polling. Runs for every role; on a
    /// follower `self_advance` is a no-op on quorum.
    pub async fn run_cluster_commit_driver(self: Arc<Self>, cancel: CancellationToken) {
        let waiter = self.waiter.clone();
        let mut seen = waiter.snapshot().get();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = waiter.snapshot().wait_reach(seen + 1) => {
                    seen = waiter.snapshot().get();
                    self.self_advance();
                }
            }
        }
    }

    pub fn list_term_records(&self, from_term: u64, limit: usize) -> io::Result<Vec<TermRecord>> {
        self.durable.term.list_records(from_term, limit)
    }

    pub fn list_vote_records(&self, from_term: u64, limit: usize) -> io::Result<Vec<VoteRecord>> {
        self.durable.vote.list_records(from_term, limit)
    }
}

fn role_to_proto(role: Role) -> NodeRole {
    match role {
        Role::Leader => NodeRole::Leader,
        Role::Follower | Role::Candidate => NodeRole::Follower,
        Role::Initializing => NodeRole::Recovering,
    }
}

/// Per-process seed for the election-timer RNG. XOR-ing in `self_id`
/// keeps two nodes that booted at the same `SystemTime` from drawing
/// the same first-round timeout, which would create deterministic
/// split-vote storms in CI.
fn seed_for(self_id: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ self_id
}
