//! Role-gated election driver. Sibling to [`crate::replication`].
//!
//! Architecture mirrors `ReplicationLoop`: built in `RaftLoop::run`,
//! spawned alongside the command loop and the replication loop on
//! the same `LocalSet`. Owns:
//!
//! - the shared [`Rc<RefCell<RaftNode>>`] (every loop on this thread
//!   holds a clone);
//! - the inbound [`crate::command::RequestVoteMsg`] channel — peer-
//!   inbound RVs land here directly, so the command loop never sees
//!   them;
//! - a clone of the [`ReplicationGate`] so role transitions caused by
//!   elections (becoming Leader, stepping down on a higher-term reply)
//!   flip the gate without bouncing through the command loop.
//!
//! The loop drives the [`raft::Election`] borrow view in a single
//! tight cycle:
//!
//! 1. [`tick`] returns the next deadline (election timer or leader
//!    heartbeat). The loop sleeps until then or until an inbound RV
//!    arrives.
//! 2. [`start`] is called every iteration. On `true` we just became
//!    Candidate: pull `get_requests()`, dispatch all outbound `RV`s
//!    concurrently via `spawn_local`, collect outcomes, feed them
//!    into `handle_votes()`. May transition to Leader (won) or
//!    Follower (higher term observed).
//! 3. Inbound RV: drain the channel, call
//!    [`Election::handle_request_vote`], reply.
//!
//! Lifecycle: the loop exits when the inbound RV channel closes
//! (every `cmd_tx` clone dropped) — same shutdown trigger as the
//! command loop. Per-spawned RV task panics surface here as
//! `JoinError::is_panic()` and are re-raised, so any election-side
//! bug aborts the LocalSet thread instead of silently dropping votes.

use crate::LedgerSlot;
use crate::cluster_mirror::ClusterMirror;
use crate::command::RequestVoteMsg;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::replication::ReplicationGate;
use ::proto::node as proto;
use ::proto::node::node_client::NodeClient;
use ledger::transaction::Operation;
use raft::Role::Leader;
use raft::request_vote::RequestVoteRequest;
use raft::{NodeId, RaftNode, Role, VoteOutcome};
use spdlog::{debug, error};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Error};

/// Per-RPC timeout for outbound `RequestVote` calls. A peer that
/// fails to reply within this window is mapped to
/// `VoteOutcome::Failed`; the round proceeds with the rest.
const VOTE_RPC_TIMEOUT: Duration = Duration::from_millis(50);

/// Connect timeout for fresh `NodeClient` dials inside an RV round.
const VOTE_CONNECT_TIMEOUT: Duration = Duration::from_millis(10);

/// Single-task election driver. Built in `RaftLoop::run`, consumed by
/// [`Self::run`].
pub(crate) struct ConsensusLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    mirror: Arc<ClusterMirror>,
    ledger: Arc<LedgerSlot>,
    config: Arc<Config>,
    gate: ReplicationGate,
    rv_rx: mpsc::Receiver<RequestVoteMsg>,
    self_id: NodeId,
}

impl ConsensusLoop {
    pub(crate) fn new(
        node: Rc<RefCell<RaftNode<DurablePersistence>>>,
        mirror: Arc<ClusterMirror>,
        ledger: Arc<LedgerSlot>,
        config: Arc<Config>,
        gate: ReplicationGate,
        rv_rx: mpsc::Receiver<RequestVoteMsg>,
        self_id: NodeId,
    ) -> Self {
        Self {
            node,
            mirror,
            config,
            ledger,
            gate,
            rv_rx,
            self_id,
        }
    }

    pub(crate) async fn run(mut self) {
        let self_id = self.self_id;
        debug!("consensus_loop[{}]: started", self_id);

        let prev_role = self.node.borrow().role();
        loop {
            let now = Instant::now();

            // 1. Drive election-side time forward. `tick` is cheap and
            //    idempotent; it lazy-arms the election timer on the
            //    first call and returns the soonest deadline among the
            //    election timer / leader heartbeats.
            let wakeup = self.node.borrow_mut().election().tick(now);

            // 2. If the timer has expired (and we're not Leader),
            //    `start` transitions to Candidate. Returns true on
            //    transition; we then run an election round with
            //    concurrently-dispatched `RequestVote` RPCs.
            let started = self.node.borrow_mut().election().start(now);
            if started {
                let (term_after, role_after) = {
                    let n = self.node.borrow();
                    (n.current_term(), n.role())
                };
                debug!(
                    "consensus_loop[{}]: election.start() = true (term={}, role={:?})",
                    self_id, term_after, role_after
                );
                self.mirror.snapshot_from(&self.node.borrow());
                self.sync_gate_from_role(); // candidate → gate=false

                let requests = self.node.borrow_mut().election().get_requests();
                if requests.is_empty() {
                    debug!(
                        "consensus_loop[{}]: election.get_requests() empty (single-node short-circuit or no peers)",
                        self_id
                    );
                } else {
                    debug!(
                        "consensus_loop[{}]: election.get_requests() = {} requests at term={}",
                        self_id,
                        requests.len(),
                        term_after
                    );
                    let responses = self.send_request_votes_concurrent(requests).await;
                    debug!(
                        "consensus_loop[{}]: election round collected {} responses",
                        self_id,
                        responses.len()
                    );
                    let mut grant_votes_count = 0;
                    for (_, vote) in responses.iter() {
                        match vote {
                            VoteOutcome::Granted { .. } => {
                                grant_votes_count += 1;
                            }
                            VoteOutcome::Denied { .. } => {}
                            VoteOutcome::Failed => {}
                        }
                    }
                    let now = Instant::now();
                    self.node
                        .borrow_mut()
                        .election()
                        .handle_votes(now, responses);
                    let (post_term, post_role) = {
                        let n = self.node.borrow();
                        (n.current_term(), n.role())
                    };
                    debug!(
                        "consensus_loop[{}]: election.handle_votes() done — term={} role={:?}",
                        self_id, post_term, post_role
                    );
                    if post_role == Leader {
                        self.ledger.ledger().submit(Operation::NewTerm {
                            term: post_term,
                            node_id: self_id,
                            node_count: self
                                .config
                                .cluster
                                .as_ref()
                                .map(|c| c.peers.len())
                                .unwrap_or(0) as u16,
                            node_voted: grant_votes_count,
                        });
                    }
                    self.mirror.snapshot_from(&self.node.borrow());
                    self.sync_gate_from_role();
                }
            }

            let current_role = self.node.borrow().role();
            if !started && current_role == Leader && prev_role != Leader {
                // Node became leader without election (only in single node situation)
                self.ledger.ledger().submit(Operation::NewTerm {
                    term: self.node.borrow().current_term(),
                    node_id: self_id,
                    node_count: 1, // self
                    node_voted: 1, // self
                });
            }

            // 3. Sleep until wakeup or inbound RV command, whichever
            //    comes first.
            let sleep_at = wakeup.deadline;
            let sleep = tokio::time::sleep_until(sleep_at.into());
            tokio::pin!(sleep);

            tokio::select! {
                maybe_msg = self.rv_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => self.handle_request_vote_msg(msg),
                        None => {
                            debug!("consensus_loop[{}]: rv channel closed; shutting down", self_id);
                            break;
                        }
                    }
                }
                _ = &mut sleep => {
                    // Empty wakeup tick — intentionally not logged
                    // (would dominate the log at heartbeat cadence).
                }
            }
        }

        debug!("consensus_loop[{}]: exiting", self_id);
    }

    fn handle_request_vote_msg(&mut self, msg: RequestVoteMsg) {
        let self_id = self.self_id;
        let RequestVoteMsg { req, reply } = msg;
        debug!(
            "consensus_loop[{}]: cmd RequestVote candidate={} term={} last_tx_id={} last_term={}",
            self_id, req.candidate_id, req.term, req.last_tx_id, req.last_term
        );
        let result = self.node.borrow_mut().election().handle_request_vote(
            Instant::now(),
            RequestVoteRequest {
                from: req.candidate_id,
                term: req.term,
                last_tx_id: req.last_tx_id,
                last_term: req.last_term,
            },
        );
        self.mirror.snapshot_from(&self.node.borrow());
        // `handle_request_vote` may have stepped us down internally
        // (higher term observed). Sync the gate so replication pauses
        // immediately.
        self.sync_gate_from_role();
        debug!(
            "consensus_loop[{}]: RequestVote reply term={} granted={}",
            self_id, result.term, result.granted
        );
        if reply
            .send(proto::RequestVoteResponse {
                term: result.term,
                vote_granted: result.granted,
            })
            .is_err()
        {
            error!(
                "consensus_loop[{}]: RequestVote reply dropped (handler gone)",
                self_id
            );
        }
    }

    /// Dispatch every outbound `RequestVote` concurrently and collect
    /// per-peer `VoteOutcome`s. A single slow peer can no longer
    /// linearise the round — that was the whole point of the
    /// consensus refactor.
    ///
    /// Each RPC runs as a `tokio::task::spawn_local` so they make
    /// progress in parallel inside the surrounding `LocalSet`. Handles
    /// are awaited sequentially after spawn — the underlying tasks are
    /// already running, so the await loop is just a synchronisation
    /// barrier on the slowest reply (or `VOTE_RPC_TIMEOUT`, whichever
    /// fires first). Per-task panics are re-raised so any election-
    /// path bug aborts the LocalSet thread.
    async fn send_request_votes_concurrent(
        &self,
        requests: Vec<(NodeId, RequestVoteRequest)>,
    ) -> Vec<(NodeId, VoteOutcome)> {
        let candidate_id = self.self_id;
        let mut handles = Vec::with_capacity(requests.len());
        for (peer, req) in requests {
            let config = self.config.clone();
            let handle = tokio::task::spawn_local(async move {
                let outcome = send_one_request_vote(candidate_id, peer, req, config).await;
                (peer, outcome)
            });
            handles.push(handle);
        }
        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            match h.await {
                Ok(pair) => results.push(pair),
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => error!(
                    "consensus_loop[{}]: RequestVote task cancelled: {}",
                    candidate_id, e
                ),
            }
        }
        results
    }

    /// Mirror the current raft role into the gate. Covers election-
    /// path role flips (`start`'s candidate transition,
    /// `handle_votes`'s leader-on-win or follower-on-higher-term,
    /// `handle_request_vote`'s follower-on-higher-term). Idempotent —
    /// `watch::Sender::send` is a no-op when the value is unchanged.
    fn sync_gate_from_role(&self) {
        let (role, term) = {
            let n = self.node.borrow();
            (n.role(), n.current_term())
        };
        let playing = matches!(role, Role::Leader);
        self.gate.set_playing(playing);
        debug!(
            "consensus_loop[{}]: gate={} (role={:?}, term={})",
            self.self_id, playing, role, term
        );
    }
}

async fn send_one_request_vote(
    candidate_id: NodeId,
    peer: NodeId,
    req: RequestVoteRequest,
    config: Arc<Config>,
) -> VoteOutcome {
    let mut client = match get_node_client(&config, peer).await {
        Ok(c) => c,
        Err(err) => {
            error!(
                "consensus_loop[{}]: RequestVote → peer={} connect failed: {}",
                candidate_id, peer, err
            );
            return VoteOutcome::Failed;
        }
    };
    debug!(
        "consensus_loop[{}]: RequestVote → peer={} term={} last_tx_id={} last_term={}",
        candidate_id, peer, req.term, req.last_tx_id, req.last_term
    );
    let rpc_fut = client.request_vote(proto::RequestVoteRequest {
        term: req.term,
        candidate_id,
        last_tx_id: req.last_tx_id,
        last_term: req.last_term,
    });
    let resp = match tokio::time::timeout(VOTE_RPC_TIMEOUT, rpc_fut).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(e)) => {
            error!(
                "consensus_loop[{}]: RequestVote → peer={} rpc failed: {}",
                candidate_id, peer, e
            );
            return VoteOutcome::Failed;
        }
        Err(_) => {
            error!(
                "consensus_loop[{}]: RequestVote → peer={} timed out after {:?}",
                candidate_id, peer, VOTE_RPC_TIMEOUT
            );
            return VoteOutcome::Failed;
        }
    };
    debug!(
        "consensus_loop[{}]: RequestVote ← peer={} term={} granted={}",
        candidate_id, peer, resp.term, resp.vote_granted
    );
    if resp.vote_granted {
        VoteOutcome::Granted { term: resp.term }
    } else {
        VoteOutcome::Denied { term: resp.term }
    }
}

async fn get_node_client(
    config: &Config,
    node_id: NodeId,
) -> Result<NodeClient<Channel>, ConnectError> {
    let host = config
        .cluster
        .as_ref()
        .ok_or(ConnectError::NotClustered)?
        .peers
        .iter()
        .find(|p| p.peer_id == node_id)
        .ok_or(ConnectError::UnknownPeer(node_id))?
        .host
        .clone();

    match tokio::time::timeout(VOTE_CONNECT_TIMEOUT, NodeClient::connect(host)).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(ConnectError::Transport(e)),
        Err(_) => Err(ConnectError::Timeout),
    }
}

#[derive(Debug)]
enum ConnectError {
    NotClustered,
    UnknownPeer(NodeId),
    Timeout,
    Transport(Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::NotClustered => write!(f, "not clustered"),
            ConnectError::UnknownPeer(id) => write!(f, "unknown peer {}", id),
            ConnectError::Timeout => write!(f, "timeout"),
            ConnectError::Transport(e) => write!(f, "transport error: {}", e),
        }
    }
}
