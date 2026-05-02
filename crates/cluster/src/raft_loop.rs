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
use ::proto::node as proto;
use ::proto::node::RequestVoteResponse;
use ::proto::node::node_client::NodeClient;
use raft::request_vote::RequestVoteRequest;
use raft::{
    Action, AppendEntriesDecision, AppendResult, Event, LogEntryRange, NodeId, RaftNode,
    RejectReason, TxId,
};
use spdlog::{error, info};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::WalTailer;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};
use tokio::time::error::Elapsed;
use tonic::transport::{Channel, Error};

/// Bound on the inbound command channel. Picked to absorb a burst of
/// in-flight RPCs without blocking gRPC handlers, while still giving
/// backpressure under sustained overload.
const COMMAND_CHANNEL_DEPTH: usize = 1024;

/// Fallback wakeup cadence when raft has no pending deadline of its own
/// (single-node leader with no peers, no election timer). Keeps the
/// ledger-polling loop alive so commit watermarks advance.
const FALLBACK_WAKEUP: Duration = Duration::from_millis(50);

/// Reply parked until follower-side WAL durability covers `tx_id`.
/// The cluster owns the gRPC `oneshot` while the ledger pipeline is
/// fsyncing the entries that arrived on this AppendEntries; the main
/// loop drains the parked reply once the ledger's commit watermark
/// (which collapses with the WAL durability watermark in the current
/// design — see ADR-0017) covers the parked `last_tx_id`.
struct ParkedReply {
    /// The oneshot the gRPC handler is awaiting.
    reply: oneshot::Sender<proto::AppendEntriesResponse>,
    /// Term to stamp into the success response.
    term: u64,
    /// `last_tx_id` to report — equals the map key.
    last_tx_id: TxId,
}

/// Per-peer leader-side replication state.
///
/// Each peer gets its own `WalTailer` because the tailer cursor is
/// stateful per-stream — peers replicate at different `next_index`
/// values, so a single shared tailer would thrash.
struct PeerReplicator {
    /// gRPC URL of the peer's `Node` service. `tonic::Channel`-style
    /// scheme + host + port (e.g. `http://10.0.0.2:50061`).
    host: String,
    /// Bookmarked stream over the leader's WAL. Reads bytes for the
    /// `[from_tx_id..]` range on each replication tick that pulls a
    /// non-empty `AppendEntriesRequest`.
    tailer: WalTailer,
}

pub struct RaftLoop {
    node: RaftNode<DurablePersistence>,
    ledger: Arc<LedgerSlot>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,

    cmd_rx: mpsc::Receiver<Command>,

    /// Most recent `Action::SetWakeup` deadline — drives the `select!`
    /// sleep arm. `None` ⇒ use `FALLBACK_WAKEUP`.
    next_wakeup: Option<Instant>,
}

impl RaftLoop {
    /// Spawn the loop and return the channel handle for the gRPC
    /// handlers to clone. The returned `JoinHandle` is owned by the
    /// supervisor so its `Drop` can await graceful exit.
    pub fn spawn(
        node: RaftNode<DurablePersistence>,
        ledger: Arc<LedgerSlot>,
        mirror: Arc<ClusterMirror>,
        config: Arc<Config>,
    ) -> (mpsc::Sender<Command>, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_DEPTH);

        let raft_loop = Self {
            node,
            ledger,
            mirror,
            config,
            cmd_rx,
            next_wakeup: None,
        };

        let join = tokio::spawn(raft_loop.run());

        (cmd_tx, join)
    }

    async fn run(mut self) {
        let self_id = self.node.self_id();
        info!("raft_loop: started (self_id={})", self_id);

        // Kick the election timer by issuing one initial Tick. Raft
        // will emit `SetWakeup` and we'll sleep on that deadline next
        // iteration.
        self.step(Event::Tick).await;

        let sleep_until = self
            .next_wakeup
            .unwrap_or_else(|| Instant::now() + FALLBACK_WAKEUP);
        let sleep = tokio::time::sleep_until(sleep_until.into());
        tokio::pin!(sleep);

        loop {
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
                let result = self.node.request_vote(
                    Instant::now(),
                    RequestVoteRequest {
                        from: req.candidate_id,
                        term: req.term,
                        last_tx_id: req.last_tx_id,
                        last_term: req.last_term,
                    },
                );
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
        let decision = self.node.validate_append_entries_request(
            now,
            req.leader_id,
            req.term,
            req.prev_tx_id,
            req.prev_term,
            entries,
            req.leader_commit_tx_id,
        );
        self.mirror.snapshot_from(&self.node);

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
                let _ = reply.send(build_ae_response(&self.node, false, Some(reason)));
            }
            AppendEntriesDecision::Accept { append: None } => {
                // Heartbeat: cluster_commit was already updated
                // inline inside validate.
                let _ = reply.send(build_ae_response(&self.node, true, None));
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

    /// Step raft, snapshot the mirror, dispatch any actions. The single
    /// path through which `node.step()` is called from the command
    /// loop.
    async fn step(&mut self, event: Event) {
        let actions = {
            let actions = self.node.step(Instant::now(), event);
            self.mirror.snapshot_from(&self.node);
            actions
        };
        for action in actions {
            self.dispatch_action(action).await;
        }
    }

    async fn dispatch_action(&mut self, action: raft::Action) {
        use raft::Action::*;
        match action {
            SetWakeup { at } => {
                info!("raft_loop: SetWakeup: {:?}", at);
                self.next_wakeup = Some(at);
            }
            FatalError { reason } => {
                error!("raft_loop: FatalError: {}", reason);
                self.mirror.set_fatal();
                spdlog::error!("raft_loop: FatalError: {}", reason);
                // Loop continues running but raft itself is now frozen
                // — every subsequent step() returns no actions. The
                // supervisor observes via mirror.is_fatal() and tears
                // down the process.
            }
            SendRequestVote {
                to,
                term,
                last_tx_id,
                last_term,
            } => {
                let mut client = self.get_node_client(to).await;

                match client {
                    Err(err) => {
                        error!("raft_loop: SendRequestVote: failed to get client: {}", err);
                        return;
                    }
                    Ok(mut client) => {
                        let result = client.request_vote(proto::RequestVoteRequest {
                            term,
                            candidate_id: self.node.self_id(),
                            last_tx_id,
                            last_term,
                        }).await.unwrap().into_inner();
                        self.node.step(Instant::now(), Event::RequestVoteReply {
                            from: to,
                            term: result.term,
                            granted: result.vote_granted,
                        });
                    }
                }
            }
            BecomeRole { role, term } => {
                info!("raft_loop: BecomeRole: {:?} {:?}", role, term);
            }
            _ => {}
        }
    }

    async fn get_node_client(
        &self,
        node_id: NodeId,
    ) -> Result<NodeClient<Channel>, ConnectError> {
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
