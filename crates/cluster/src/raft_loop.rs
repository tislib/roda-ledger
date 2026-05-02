//! `RaftLoop` — single-owner driver for `RaftNode`. Replaces the
//! mutex-based `RaftHandle`. The loop owns `RaftNode` as `&mut self`,
//! receives commands on an mpsc channel, and is the only thing that
//! ever calls `node.step()`.
//!
//! This file is the skeleton: types, fields, `spawn` entry point, and
//! a minimal `run()` that compiles and idles correctly. Inbound RPC
//! handling, action dispatch, parked replies, ledger polling, and
//! outbound RPC dispatch are stubbed with TODOs and filled in
//! incrementally.

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
    AppendEntriesDecision, Event, LogEntryRange, NodeId, RaftNode, RejectReason, Role,
};
use spdlog::{error, info};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::transport::{Channel, Error};

/// Bound on the inbound command channel. Picked to absorb a burst of
/// in-flight RPCs without blocking gRPC handlers, while still giving
/// backpressure under sustained overload.
pub(crate) const COMMAND_CHANNEL_DEPTH: usize = 1024;

/// Fallback wakeup cadence when raft has no pending deadline of its own
/// (single-node leader with no peers, no election timer). Keeps the
/// ledger-polling loop alive so commit watermarks advance.
const FALLBACK_WAKEUP: Duration = Duration::from_millis(50);

pub struct RaftLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,

    cmd_rx: mpsc::Receiver<Command>,

    /// Most recent `Action::SetWakeup` deadline — drives the `select!`
    /// sleep arm. `None` ⇒ use `FALLBACK_WAKEUP`.
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
            ledger,
            mirror,
            config,
            cmd_rx,
            next_wakeup: None,
            gate: Some(gate),
            gate_rx: Some(gate_rx),
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
        let self_id =  self.node.borrow().self_id();
        info!("raft_loop: started (self_id={})", self_id);

        // Kick the election timer by issuing one initial Tick. Raft
        // will emit `SetWakeup` and we'll sleep on that deadline next
        // iteration.
        self.step(Event::Tick).await;

        loop {
            let sleep_until = self
                .next_wakeup
                .unwrap_or_else(|| Instant::now() + FALLBACK_WAKEUP);
            let sleep = tokio::time::sleep_until(sleep_until.into());
            tokio::pin!(sleep);

            tokio::select! {
                maybe_cmd = self.cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        // Channel closed — supervisor dropped the last
                        // sender. Graceful shutdown.
                        None => break,
                    }
                }
                _ = &mut sleep => {
                    self.step(Event::Tick).await;
                }
            }
        }

        info!("raft_loop: exiting (self_id={})", self_id);
        // Drop the replication gate's `watch::Sender` so per-peer
        // replication tasks see `gate_rx.changed()` return Err and
        // exit cooperatively. RAII shutdown — no explicit notify.
        self.gate = None;
    }

    /// Mirror the current raft role into the gate. Covers the paths
    /// where `transition_to_follower` is invoked from a direct method
    /// (`request_vote`, `validate_append_entries_request`,
    /// `replication_append_result`) without surfacing an
    /// `Action::BecomeRole`. Idempotent — `watch::Sender::send` is a
    /// no-op when the value is unchanged.
    fn sync_gate_from_role(&self) {
        if let Some(gate) = self.gate.as_ref() {
            let role = self.node.borrow().role();
            gate.set_playing(matches!(role, Role::Leader));
        }
    }

    /// Apply a command to raft. Routes inbound RPCs to typed handlers,
    /// outbound RPC replies into the `Replication` API.
    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::AppendEntries { req, reply } => {
                self.handle_append_entries(req, reply).await;
            }
            Command::RequestVote { req, reply } => {
                info!("raft_loop: RequestVote: {:?}", req);
                let result =  self.node.borrow_mut().request_vote(
                    Instant::now(),
                    RequestVoteRequest {
                        from: req.candidate_id,
                        term: req.term,
                        last_tx_id: req.last_tx_id,
                        last_term: req.last_term,
                    },
                );
                self.mirror.snapshot_from(&self.node.borrow());
                // `request_vote` may have stepped us down internally
                // (higher term observed). Sync the gate so replication
                // pauses immediately rather than next command-loop tick.
                self.sync_gate_from_role();
                info!("raft_loop: RequestVote result: {:?}", result);
                reply
                    .send(RequestVoteResponse {
                        term: result.term,
                        vote_granted: result.granted,
                    })
                    .unwrap();
            }
        }
    }

    async fn handle_append_entries(
        &mut self,
        req: proto::AppendEntriesRequest,
        reply: oneshot::Sender<proto::AppendEntriesResponse>,
    ) {
        let now = Instant::now();
        let entries = if req.from_tx_id == 0 || req.to_tx_id < req.from_tx_id {
            LogEntryRange::empty()
        } else {
            LogEntryRange::new(req.from_tx_id, req.to_tx_id - req.from_tx_id + 1, req.term)
        };
        let decision =  self.node.borrow_mut().validate_append_entries_request(
            now,
            req.leader_id,
            req.term,
            req.prev_tx_id,
            req.prev_term,
            entries,
            req.leader_commit_tx_id,
        );
        self.mirror.snapshot_from(& self.node.borrow());
        // Inbound AE always lands a `transition_to_follower` (when
        // `term >= current_term`); pause replication immediately if we
        // were the leader.
        self.sync_gate_from_role();

        match decision {
            AppendEntriesDecision::Reject {
                reason,
                truncate_after,
            } => {
                if let Some(_after) = truncate_after {
                    // TODO: truncate the ledger's entry log past
                    // `_after`. The term-log mirror is already
                    // truncated synchronously inside validate. The
                    // ledger today does not expose a public
                    // `truncate_wal_after` — wire one up alongside
                    // the rest of the WAL-replication scaffolding.
                }
                let _ = reply.send(build_ae_response(& self.node.borrow(), false, Some(reason)));
            }
            AppendEntriesDecision::Accept { append: None } => {
                // Heartbeat: cluster_commit was already updated
                // inline inside validate.
                let _ = reply.send(build_ae_response(& self.node.borrow(), true, None));
            }
            AppendEntriesDecision::Accept {
                append: Some(_range),
            } => {
                // TODO: parse `req.wal_bytes` into `Vec<WalEntry>`,
                // call `self.ledger.ledger().append_wal_entries(...)`,
                // park `reply` in `pending_ae_replies` keyed by
                // `_range.last_tx_id().unwrap()`. The main loop's
                // ledger-watermark polling drains the parked reply
                // (after `node.advance(write, commit)`) using
                // `build_ae_response(true, None)` from getters at
                // drain time.
                drop(reply);
            }
            AppendEntriesDecision::Fatal { reason } => {
                self.mirror.set_fatal();
                spdlog::error!(
                    "raft_loop: validate_append_entries_request returned Fatal: {}",
                    reason
                );
                drop(reply);
            }
        }
    }

    /// Step raft, snapshot the mirror, dispatch each emitted action.
    /// `dispatch_action` may produce a follow-up [`Event`] (e.g. an
    /// `Event::RequestVoteReply` after the outbound `RequestVote`
    /// RPC completes); we drain those follow-ups in a loop instead
    /// of recursing — `dispatch_action` calling `step` recursively
    /// would require an unbounded async-future size and won't
    /// compile.
    async fn step(&mut self, event: Event) {
        let mut pending: std::collections::VecDeque<Event> = std::collections::VecDeque::new();
        pending.push_back(event);
        while let Some(ev) = pending.pop_front() {
            let actions =  self.node.borrow_mut().step(Instant::now(), ev);
            self.mirror.snapshot_from(& self.node.borrow());
            for action in actions {
                if let Some(follow_up) = self.dispatch_action(action).await {
                    pending.push_back(follow_up);
                }
            }
        }
    }

    /// Process one action emitted by `node.step()`. Returns a
    /// follow-up [`Event`] that the surrounding `step` loop should
    /// feed back into the state machine — typically the
    /// `Event::RequestVoteReply` synthesised from an outbound
    /// `RequestVote` RPC's response. `None` means "no follow-up".
    async fn dispatch_action(&mut self, action: raft::Action) -> Option<Event> {
        use raft::Action::*;
        match action {
            SetWakeup { at } => {
                info!("raft_loop: SetWakeup: {:?}", at);
                self.next_wakeup = Some(at);
                None
            }
            FatalError { reason } => {
                error!("raft_loop: FatalError: {}", reason);
                self.mirror.set_fatal();
                // Loop continues running but raft itself is now frozen
                // — every subsequent step() returns no actions. The
                // supervisor observes via mirror.is_fatal() and tears
                // down the process.
                None
            }
            SendRequestVote {
                to,
                term,
                last_tx_id,
                last_term,
            } => {
                let mut client = match self.get_node_client(to).await {
                    Ok(c) => c,
                    Err(err) => {
                        error!(
                            "raft_loop: SendRequestVote: connect to peer {} failed: {}",
                            to, err
                        );
                        return None;
                    }
                };
                info!("raft_loop: SendRequestVote: {:?}", (to, term, last_tx_id));
                let candidate_id =  self.node.borrow().self_id();
                let resp = match client
                    .request_vote(proto::RequestVoteRequest {
                        term,
                        candidate_id,
                        last_tx_id,
                        last_term,
                    })
                    .await
                {
                    Ok(r) => r.into_inner(),
                    Err(e) => {
                        error!(
                            "raft_loop: SendRequestVote: rpc to peer {} failed: {}",
                            to, e
                        );
                        return None;
                    }
                };
                info!("raft_loop: SendRequestVote result: {:?}", resp);
                // Surface the reply as a follow-up event; the outer
                // `step` loop will feed it through `node.step()`,
                // dispatch the resulting `BecomeRole(Leader)` /
                // `SetWakeup` actions, and snapshot the mirror.
                Some(Event::RequestVoteReply {
                    from: to,
                    term: resp.term,
                    granted: resp.vote_granted,
                })
            }
            BecomeRole { role, term } => {
                info!("raft_loop: BecomeRole: {:?} {:?}", role, term);
                // The control point: replication plays only while we
                // are leader. Any other role transition pauses it.
                if let Some(gate) = self.gate.as_ref() {
                    gate.set_playing(matches!(role, Role::Leader));
                }
                None
            }
        }
    }

    async fn get_node_client(&self, node_id: NodeId) -> Result<NodeClient<Channel>, ConnectError> {
        let timeout = Duration::from_millis(100);
        let host = self
            .config
            .cluster
            .as_ref()
            .ok_or(ConnectError::NotClustered)?
            .peers
            .iter()
            .find(|p| p.peer_id == node_id)
            .ok_or(ConnectError::UnknownPeer(node_id))?
            .host
            .clone();

        match tokio::time::timeout(timeout, NodeClient::connect(host)).await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(e)) => Err(ConnectError::Transport(e)),
            Err(_) => Err(ConnectError::Timeout),
        }
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

/// Build the proto response from raft's getters. Free function so
/// it can be called with a `MutexGuard<RaftNode>` already held —
/// avoids needing a second lock just to read `current_term()` /
/// `commit_index()` / `write_index()`.
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
