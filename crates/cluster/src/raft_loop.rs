//! `RaftLoop` — single-owner driver for `RaftNode`. Replaces the
//! mutex-based `RaftHandle`. The loop owns `RaftNode` as
//! `Rc<RefCell<...>>` (it's the only writer), receives commands on an
//! mpsc channel, and drives the state machine through the
//! [`raft::Election`] borrow view.
//!
//! Election rounds dispatch all `RequestVote` RPCs *concurrently*
//! via `futures::future::join_all` — one slow peer no longer
//! linearises the whole round.
//!
//! Replication is unchanged: per-peer `ReplicationLoop` tasks drive
//! AE through the [`raft::Replication`] / [`raft::PeerReplication`]
//! borrow views, gated on a `ReplicationGate` that the command loop
//! flips on every state transition (cluster polls `node.role()`
//! after each call — there is no action stream).

use crate::cluster_mirror::ClusterMirror;
use crate::command::Command;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::ledger_slot::LedgerSlot;
use crate::replication::{ReplicationGate, ReplicationLoop};
use ::proto::node as proto;
use ::proto::node::RequestVoteResponse;
use ::proto::node::node_client::NodeClient;
use raft::request_vote::RequestVoteRequest;
use raft::{
    AppendEntriesDecision, LogEntryRange, NodeId, RaftNode, RejectReason, Role, Term, TxId,
    VoteOutcome,
};
use spdlog::{error, info};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::decode_records;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::transport::{Channel, Error};

/// Bound on the inbound command channel. Picked to absorb a burst of
/// in-flight RPCs without blocking gRPC handlers, while still giving
/// backpressure under sustained overload.
pub(crate) const COMMAND_CHANNEL_DEPTH: usize = 1024;

/// Per-RPC timeout for outbound `RequestVote` calls inside an
/// election round. Mirrors `RaftConfig::rpc_timeout`. A peer that
/// fails to reply within this window is recorded as
/// `VoteOutcome::Failed`; the round proceeds with the rest.
const VOTE_RPC_TIMEOUT: Duration = Duration::from_millis(500);

/// Connect timeout for fresh `NodeClient` dials.
const VOTE_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);

/// AE reply parked until the local ledger's commit watermark covers
/// `last_tx_id`. The follower's gRPC handler is awaiting `reply`; we
/// hold it here while the pipeline fsyncs the entries that arrived on
/// this AE, then drain by sending a successful response built from
/// raft's getters at drain time.
struct ParkedReply {
    reply: oneshot::Sender<proto::AppendEntriesResponse>,
    /// Term to stamp into the response. The follower's AE handler
    /// always replies at the leader's term (which is also our current
    /// term after `validate_append_entries_request` updated it), but
    /// recording it explicitly avoids re-reading raft state on drain.
    term: Term,
}

pub struct RaftLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,

    cmd_rx: mpsc::Receiver<Command>,

    /// Soonest pending wakeup pulled from `Election::tick(now).deadline`
    /// — drives the `select!` sleep arm. Always `Some` after the
    /// first iteration; the library guarantees a valid `Instant`.
    next_wakeup: Option<Instant>,
    pub ledger: Arc<LedgerSlot>,

    /// Pause/play signal source for the replication loop. Held in an
    /// `Option` so the command loop can drop it (`= None`) on exit —
    /// dropping the underlying `watch::Sender` is the cooperative
    /// shutdown signal that lets per-peer replication tasks return.
    gate: Option<ReplicationGate>,
    /// Initial receiver paired with `gate`. Taken in `run` to hand
    /// off to the replication loop.
    gate_rx: Option<watch::Receiver<bool>>,

    last_sequence_index: TxId,
    last_write_index: TxId,
    last_commit_index: TxId,
}

impl RaftLoop {
    /// Build a loop ready to be driven by [`RaftLoop::run`]. The
    /// caller owns the inbound channel: it creates `(cmd_tx, cmd_rx)`,
    /// hands `cmd_tx` to the gRPC handlers, and passes `cmd_rx` here.
    /// The loop must be driven inside a `LocalSet` because it owns
    /// `Rc<RefCell<RaftNode>>` and is `!Send`.
    pub fn new(
        node: RaftNode<DurablePersistence>,
        ledger: Arc<LedgerSlot>,
        mirror: Arc<ClusterMirror>,
        config: Arc<Config>,
        cmd_rx: mpsc::Receiver<Command>,
    ) -> Self {
        let (gate, gate_rx) = ReplicationGate::new();
        Self {
            node: Rc::new(RefCell::new(node)),
            ledger: ledger.clone(),
            mirror,
            config,
            cmd_rx,
            next_wakeup: None,
            gate: Some(gate),
            gate_rx: Some(gate_rx),
            last_sequence_index: ledger.ledger().last_compute_id(),
            last_write_index: ledger.ledger().last_compute_id(),
            last_commit_index: ledger.ledger().last_commit_id(),
        }
    }

    pub async fn run(mut self) {
        let gate_rx = self.gate_rx.take().expect("gate_rx initialized in new");
        let self_id = self.node.borrow().self_id();
        let replication = ReplicationLoop::new(
            self.node.clone(),
            self.ledger.clone(),
            self.mirror.clone(),
            self.config.clone(),
            gate_rx,
            self_id,
        );
        let replication_loop_handle = replication.run();
        let command_loop_handle = self.run_command_loop();

        tokio::join!(command_loop_handle, replication_loop_handle);
    }

    async fn run_command_loop(&mut self) {
        let self_id = self.node.borrow().self_id();
        info!("raft_loop[{}]: started", self_id);

        loop {
            let now = Instant::now();

            // 1. Drive election-side time forward. `tick` is cheap
            //    and idempotent; it lazy-arms the election timer on
            //    the first call and returns the soonest deadline
            //    among the election timer / leader heartbeats.
            let wakeup = self.node.borrow_mut().election().tick(now);
            self.next_wakeup = Some(wakeup.deadline);

            // 2. Mirror the ledger watermarks into raft. Logging both
            //    pre- and post-advance so we can see how the call
            //    moved `local_write_index` / `local_commit_index` /
            //    `cluster_commit_index`.
            let commit_index = self.ledger.ledger().last_commit_id();
            let write_index = self.ledger.ledger().last_compute_id();
            let (pre_w, pre_c, pre_cci, role_pre) = {
                let n = self.node.borrow();
                (
                    n.write_index(),
                    n.commit_index(),
                    n.cluster_commit_index(),
                    n.role(),
                )
            };
            if write_index != pre_w || commit_index != pre_c {
                self.node.borrow_mut().advance(write_index, commit_index);
                let post_cci = self.node.borrow().cluster_commit_index();
                info!(
                    "raft_loop[{}]: advance role={:?} write={}->{} commit={}->{} cluster_commit={}->{}",
                    self_id, role_pre, pre_w, write_index, pre_c, commit_index, pre_cci, post_cci
                );
                self.mirror.snapshot_from(&self.node.borrow());
            }

            // 3. If the timer has expired (and we're not Leader),
            //    `start` transitions to Candidate. Returns true on
            //    transition; we then run an election round with
            //    concurrently-dispatched `RequestVote` RPCs.
            let started = self.node.borrow_mut().election().start(now);
            if started {
                let (term_after, role_after) = {
                    let n = self.node.borrow();
                    (n.current_term(), n.role())
                };
                info!(
                    "raft_loop[{}]: election.start() = true (term={}, role={:?})",
                    self_id, term_after, role_after
                );
                self.mirror.snapshot_from(&self.node.borrow());
                self.sync_gate_from_role(); // candidate → gate=false

                let requests = self.node.borrow_mut().election().get_requests();
                if requests.is_empty() {
                    info!(
                        "raft_loop[{}]: election.get_requests() empty (single-node short-circuit or no peers)",
                        self_id
                    );
                } else {
                    info!(
                        "raft_loop[{}]: election.get_requests() = {} requests at term={}",
                        self_id,
                        requests.len(),
                        term_after
                    );
                    let responses = self.send_request_votes_concurrent(requests).await;
                    info!(
                        "raft_loop[{}]: election round collected {} responses",
                        self_id,
                        responses.len()
                    );
                    let now = Instant::now();
                    self.node
                        .borrow_mut()
                        .election()
                        .handle_votes(now, responses);
                    let (post_term, post_role) = {
                        let n = self.node.borrow();
                        (n.current_term(), n.role())
                    };
                    info!(
                        "raft_loop[{}]: election.handle_votes() done — term={} role={:?}",
                        self_id, post_term, post_role
                    );
                    self.mirror.snapshot_from(&self.node.borrow());
                    // May have flipped to Leader (won) or Follower
                    // (higher term observed in batch).
                    self.sync_gate_from_role();
                }
                // Refresh wakeup after the round — Candidate may have
                // become Leader (heartbeat schedule) or Follower
                // (election timer reset).
                let wakeup = self.node.borrow_mut().election().tick(Instant::now());
                self.next_wakeup = Some(wakeup.deadline);
            }

            // 4. Sleep until wakeup or until an inbound command
            //    arrives, whichever fires first.
            let sleep_at = self.next_wakeup.unwrap_or(now + Duration::from_millis(50));
            let sleep = tokio::time::sleep_until(sleep_at.into());
            tokio::pin!(sleep);

            tokio::select! {
                maybe_cmd = self.cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => {
                            self.handle_command(cmd).await;
                        }
                        // Channel closed — supervisor dropped the last
                        // sender. Graceful shutdown.
                        None => {
                            info!("raft_loop[{}]: cmd channel closed; shutting down", self_id);
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

        info!("raft_loop[{}]: exiting", self_id);
        // Drop the replication gate's `watch::Sender` so per-peer
        // replication tasks see `gate_rx.changed()` return Err and
        // exit cooperatively. RAII shutdown — no explicit notify.
        self.gate = None;
    }

    /// Mirror the current raft role into the gate. Covers the paths
    /// where the role flips inside a direct method
    /// (`Election::handle_request_vote`, `validate_append_entries_request`,
    /// `Replication::append_result`, `Election::handle_votes`)
    /// without surfacing an action. Idempotent —
    /// `watch::Sender::send` is a no-op when the value is unchanged.
    fn sync_gate_from_role(&self) {
        if let Some(gate) = self.gate.as_ref() {
            let (self_id, role, term) = {
                let n = self.node.borrow();
                (n.self_id(), n.role(), n.current_term())
            };
            let playing = matches!(role, Role::Leader);
            gate.set_playing(playing);
            info!(
                "raft_loop[{}]: gate={} (role={:?}, term={})",
                self_id, playing, role, term
            );
        }
    }

    /// Apply a command to raft. Routes inbound RPCs to typed handlers.
    async fn handle_command(&mut self, cmd: Command) {
        let self_id = self.node.borrow().self_id();
        match cmd {
            Command::AppendEntries { req, reply } => {
                info!(
                    "raft_loop[{}]: cmd AppendEntries from={} term={} prev_tx={} prev_term={} from_tx={} to_tx={} leader_commit={} wal_bytes={}",
                    self_id,
                    req.leader_id,
                    req.term,
                    req.prev_tx_id,
                    req.prev_term,
                    req.from_tx_id,
                    req.to_tx_id,
                    req.leader_commit_tx_id,
                    req.wal_bytes.len()
                );
                self.handle_append_entries(req, reply).await;
            }
            Command::RequestVote { req, reply } => {
                info!(
                    "raft_loop[{}]: cmd RequestVote candidate={} term={} last_tx_id={} last_term={}",
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
                // `handle_request_vote` may have stepped us down
                // internally (higher term observed). Sync the gate so
                // replication pauses immediately.
                self.sync_gate_from_role();
                info!(
                    "raft_loop[{}]: RequestVote reply term={} granted={}",
                    self_id, result.term, result.granted
                );
                // The handler may have dropped (client disconnect /
                // gRPC timeout); in that case the durable vote
                // already happened — drop the reply silently rather
                // than crashing the raft loop.
                if reply
                    .send(RequestVoteResponse {
                        term: result.term,
                        vote_granted: result.granted,
                    })
                    .is_err()
                {
                    info!(
                        "raft_loop[{}]: RequestVote reply dropped (handler gone)",
                        self_id
                    );
                }
            }
        }
    }

    async fn handle_append_entries(
        &mut self,
        req: proto::AppendEntriesRequest,
        reply: oneshot::Sender<proto::AppendEntriesResponse>,
    ) {
        let self_id = self.node.borrow().self_id();
        let now = Instant::now();
        let entries = if req.from_tx_id == 0 || req.to_tx_id < req.from_tx_id {
            LogEntryRange::empty()
        } else {
            LogEntryRange::new(req.from_tx_id, req.to_tx_id - req.from_tx_id + 1, req.term)
        };
        let decision = self.node.borrow_mut().validate_append_entries_request(
            now,
            req.leader_id,
            req.term,
            req.prev_tx_id,
            req.prev_term,
            entries,
            req.leader_commit_tx_id,
        );
        self.mirror.snapshot_from(&self.node.borrow());
        // Inbound AE always lands a `transition_to_follower` (when
        // `term >= current_term`); pause replication immediately if we
        // were the leader.
        self.sync_gate_from_role();

        match decision {
            AppendEntriesDecision::Reject {
                reason,
                truncate_after,
            } => {
                info!(
                    "raft_loop[{}]: AE Reject reason={:?} truncate_after={:?}",
                    self_id, reason, truncate_after
                );
                if let Some(after) = truncate_after {
                    // TODO: truncate the ledger's entry log past `after`.
                    // The term-log mirror is already truncated
                    // synchronously inside validate; the ledger does not
                    // yet expose a `truncate_wal_after` — wire one up
                    // alongside the rest of the WAL-replication
                    // scaffolding.
                    info!(
                        "raft_loop[{}]: AE Reject truncate WAL TODO (after={})",
                        self_id, after
                    );
                }
                let resp = build_ae_response(&self.node.borrow(), false, Some(reason));
                info!(
                    "raft_loop[{}]: AE Reject reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
            AppendEntriesDecision::Accept { append: None } => {
                let commit_index = self.ledger.ledger().last_commit_id();
                let (pre_w, pre_c, pre_cci) = {
                    let n = self.node.borrow();
                    (n.write_index(), n.commit_index(), n.cluster_commit_index())
                };
                self.node.borrow_mut().advance(0, commit_index);
                let post_cci = self.node.borrow().cluster_commit_index();
                info!(
                    "raft_loop[{}]: AE Accept(heartbeat) leader_commit={} ledger_commit={} advance write={}->{} commit={}->{} cluster_commit={}->{}",
                    self_id,
                    req.leader_commit_tx_id,
                    commit_index,
                    pre_w,
                    pre_w,
                    pre_c,
                    commit_index.max(pre_c),
                    pre_cci,
                    post_cci
                );
                self.mirror.snapshot_from(&self.node.borrow());

                // Heartbeat: cluster_commit was already updated
                // inline inside validate.
                let resp = build_ae_response(&self.node.borrow(), true, None);
                info!(
                    "raft_loop[{}]: AE Accept(heartbeat) reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
            AppendEntriesDecision::Accept {
                append: Some(range),
            } => {
                let last_tx_id = range
                    .last_tx_id()
                    .expect("Accept{append: Some} carries a non-empty range");
                info!(
                    "raft_loop[{}]: AE Accept(entries) range start_tx={} count={} term={} leader_commit={}",
                    self_id, range.start_tx_id, range.count, range.term, req.leader_commit_tx_id
                );
                let entries = decode_records(&req.wal_bytes);
                if let Err(e) = self.ledger.ledger().append_wal_entries(entries) {
                    error!(
                        "raft_loop[{}]: append_wal_entries failed start={} last={}: {}",
                        self_id, range.start_tx_id, last_tx_id, e
                    );
                    let resp = build_ae_response(
                        &self.node.borrow(),
                        false,
                        Some(RejectReason::LogMismatch),
                    );
                    let _ = reply.send(resp);
                    return;
                }
                info!(
                    "raft_loop[{}]: AE Accept(entries) queued to ledger pipeline last_tx={}",
                    self_id, last_tx_id
                );

                let commit_index = self.ledger.ledger().last_commit_id();
                let (pre_w, pre_c, pre_cci) = {
                    let n = self.node.borrow();
                    (n.write_index(), n.commit_index(), n.cluster_commit_index())
                };
                self.node.borrow_mut().advance(last_tx_id, commit_index);
                let post_cci = self.node.borrow().cluster_commit_index();
                info!(
                    "raft_loop[{}]: AE Accept(entries) advance write={}->{} commit={}->{} cluster_commit={}->{} (ledger_commit={})",
                    self_id,
                    pre_w,
                    last_tx_id.max(pre_w),
                    pre_c,
                    commit_index.max(pre_c),
                    pre_cci,
                    post_cci,
                    commit_index
                );
                self.mirror.snapshot_from(&self.node.borrow());

                let resp = build_ae_response(&self.node.borrow(), true, None);
                info!(
                    "raft_loop[{}]: AE Accept(entries) reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
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
    /// fires first).
    async fn send_request_votes_concurrent(
        &self,
        requests: Vec<(NodeId, RequestVoteRequest)>,
    ) -> Vec<(NodeId, VoteOutcome)> {
        let candidate_id = self.node.borrow().self_id();
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
                Err(e) => {
                    error!(
                        "raft_loop[{}]: RequestVote task join error: {}",
                        candidate_id, e
                    );
                }
            }
        }
        results
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
                "raft_loop[{}]: RequestVote → peer={} connect failed: {}",
                candidate_id, peer, err
            );
            return VoteOutcome::Failed;
        }
    };
    info!(
        "raft_loop[{}]: RequestVote → peer={} term={} last_tx_id={} last_term={}",
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
                "raft_loop[{}]: RequestVote → peer={} rpc failed: {}",
                candidate_id, peer, e
            );
            return VoteOutcome::Failed;
        }
        Err(_) => {
            error!(
                "raft_loop[{}]: RequestVote → peer={} timed out after {:?}",
                candidate_id, peer, VOTE_RPC_TIMEOUT
            );
            return VoteOutcome::Failed;
        }
    };
    info!(
        "raft_loop[{}]: RequestVote ← peer={} term={} granted={}",
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
pub enum ConnectError {
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

/// Build the proto response from raft's getters.
fn build_ae_response(
    node: &RaftNode<DurablePersistence>,
    success: bool,
    reject_reason: Option<RejectReason>,
) -> proto::AppendEntriesResponse {
    let reject_code = match reject_reason {
        None => proto::RejectReason::RejectNone as u32,
        Some(RejectReason::TermBehind) => proto::RejectReason::RejectTermStale as u32,
        Some(RejectReason::LogMismatch) => proto::RejectReason::RejectPrevMismatch as u32,
        // RpcTimeout is a leader-side bookkeeping signal; never
        // surfaces on a follower's reply path.
        Some(RejectReason::RpcTimeout) => proto::RejectReason::RejectNone as u32,
    };
    proto::AppendEntriesResponse {
        term: node.current_term(),
        success,
        last_commit_id: node.commit_index(),
        last_write_id: node.write_index(),
        reject_reason: reject_code,
    }
}
