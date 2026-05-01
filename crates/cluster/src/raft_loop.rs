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
use raft::{AppendEntriesDecision, Event, LogEntryRange, RaftNode, RejectReason, TxId};
use spdlog::info;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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

pub struct RaftLoop {
    node: RaftNode<DurablePersistence>,
    ledger: Arc<LedgerSlot>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,

    cmd_rx: mpsc::Receiver<Command>,
    /// Cloned into spawned outbound RPC tasks so they can post their
    /// results back as `Command::OutboundReply`.
    cmd_tx_for_outbound: mpsc::Sender<Command>,

    /// AE replies awaiting WAL durability. Keyed by the tx_id that must
    /// be durably written before the success reply fires. Drained on
    /// every loop iteration after `last_seen_write` advances.
    pending_ae_replies: BTreeMap<TxId, Vec<ParkedReply>>,

    /// Last `last_wal_write_id()` observed from the ledger. Used to
    /// detect changes between iterations and to feed the new write
    /// watermark into `RaftNode::advance(write, commit)` (TODO once
    /// the production poll loop is wired in).
    last_seen_write: TxId,
    /// Last `last_commit_id()` observed. Feeds the commit watermark
    /// into `RaftNode::advance(write, commit)` so the leader's
    /// quorum self-slot picks it up (TODO).
    last_seen_commit: TxId,

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
            cmd_tx_for_outbound: cmd_tx.clone(),
            pending_ae_replies: BTreeMap::new(),
            last_seen_write: 0,
            last_seen_commit: 0,
            next_wakeup: None,
        };
        let join = tokio::spawn(raft_loop.run());
        (cmd_tx, join)
    }

    async fn run(mut self) {
        info!("raft_loop: started (self_id={})", self.node.self_id());

        // Kick the election timer by issuing one initial Tick. Raft
        // will emit `SetWakeup` and we'll sleep on that deadline next
        // iteration.
        self.step(Event::Tick);

        loop {
            // TODO: poll ledger watermarks → call
            //       `node.advance(write, commit)` then `step(Tick)`
            //       (ADR-0017 §"Driver call pattern"). The cluster
            //       commit advance is observed by re-snapshotting
            //       the mirror after each step — there is no
            //       dedicated `Action::AdvanceClusterCommit`.
            // TODO: drain pending_ae_replies up to last_seen_write.

            let sleep_until = self
                .next_wakeup
                .unwrap_or_else(|| Instant::now() + FALLBACK_WAKEUP);
            let sleep = tokio::time::sleep_until(sleep_until.into());
            tokio::pin!(sleep);

            tokio::select! {
                maybe_cmd = self.cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => self.handle_command(cmd),
                        // Channel closed — supervisor dropped the last
                        // sender. Graceful shutdown.
                        None => break,
                    }
                }
                _ = &mut sleep => {
                    self.step(Event::Tick);
                }
            }
        }

        info!("raft_loop: exiting (self_id={})", self.node.self_id());
        // TODO: drain pending_ae_replies — drop the oneshots so awaiting
        //       gRPC handlers see `Status::internal("loop dropped reply")`
        //       rather than hanging.
    }

    /// Apply a command to raft. Routes inbound RPCs to typed handlers,
    /// outbound replies straight into `step`.
    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::AppendEntries { req, reply } => {
                self.handle_append_entries(req, reply);
            }
            Command::RequestVote { req: _, reply: _ } => {
                // TODO: build Event::RequestVoteRequest, step, send
                //       the reply synchronously from the resulting
                //       SendRequestVoteReply action.
            }
        }
    }

    /// Follower-side `AppendEntries` handling. Implements the
    /// validate → I/O → advance → reply pattern (ADR-0017 §"Driver
    /// call pattern"):
    ///
    /// 1. Convert the proto request into raft's parameter shape and
    ///    call `node.validate_append_entries_request(...)`. The
    ///    library updates its internal state machine (term
    ///    observation, follower transition, election timer reset,
    ///    synchronous term-log truncation, watermark clamping,
    ///    heartbeat-path `cluster_commit` advance) and returns an
    ///    `AppendEntriesDecision` describing what the cluster must
    ///    do.
    /// 2. On `Reject`: optionally truncate the entry log, build the
    ///    response from raft's getters, send synchronously.
    /// 3. On `Accept { append: None }` (heartbeat): build the
    ///    response from raft's getters, send synchronously.
    /// 4. On `Accept { append: Some(range) }`: kick the ledger to
    ///    durably append `req.wal_bytes`, park the oneshot in
    ///    `pending_ae_replies` keyed by `range.last_tx_id()`. The
    ///    main loop will observe the ledger's commit watermark
    ///    advancing past that key, call `node.advance(...)` to
    ///    update raft's watermarks, and drain the parked reply with
    ///    the post-advance getters.
    /// 5. On `Fatal`: surface to the supervisor and drop the
    ///    oneshot (the gRPC handler sees channel-closed).
    fn handle_append_entries(
        &mut self,
        req: proto::AppendEntriesRequest,
        reply: oneshot::Sender<proto::AppendEntriesResponse>,
    ) {
        let now = Instant::now();
        let entries = if req.from_tx_id == 0 || req.to_tx_id < req.from_tx_id {
            LogEntryRange::empty()
        } else {
            LogEntryRange::new(
                req.from_tx_id,
                req.to_tx_id - req.from_tx_id + 1,
                req.term,
            )
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
                let _ = reply.send(self.build_ae_response(false, Some(reason)));
            }
            AppendEntriesDecision::Accept { append: None } => {
                // Heartbeat: cluster_commit was already updated
                // inline inside validate.
                let _ = reply.send(self.build_ae_response(true, None));
            }
            AppendEntriesDecision::Accept { append: Some(_range) } => {
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

    /// Build the proto response from raft's getters. The cluster
    /// stamps `term` / `last_commit_id` / `last_write_id` from the
    /// post-validate (and post-advance, for the entries case)
    /// state — see ADR-0017 §"AE reply: write vs commit watermark".
    fn build_ae_response(
        &self,
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
            term: self.node.current_term(),
            success,
            last_commit_id: self.node.commit_index(),
            last_write_id: self.node.write_index(),
            reject_reason: reject_code,
        }
    }

    /// Step raft, snapshot the mirror, dispatch any actions. The single
    /// path through which `node.step()` is called.
    fn step(&mut self, event: Event) {
        let actions = self.node.step(Instant::now(), event);
        self.mirror.snapshot_from(&self.node);
        for action in actions {
            self.dispatch_action(action);
        }
    }

    fn dispatch_action(&mut self, action: raft::Action) {
        use raft::Action::*;
        match action {
            SetWakeup { at } => {
                self.next_wakeup = Some(at);
            }
            FatalError { reason } => {
                self.mirror.set_fatal();
                spdlog::error!("raft_loop: FatalError: {}", reason);
                // Loop continues running but raft itself is now frozen
                // — every subsequent step() returns no actions. The
                // supervisor observes via mirror.is_fatal() and tears
                // down the process.
            }
            // Follower-side AE handling lives in `handle_append_entries`
            // (validate → I/O → advance → reply); raft no longer
            // emits `SendAppendEntriesReply` / `AppendLog` /
            // `TruncateLog`.
            // TODO: SendAppendEntries → spawn outbound dispatch task
            // TODO: SendRequestVote   → spawn outbound dispatch task
            // TODO: SendRequestVoteReply   → reply via current request's oneshot
            // TODO: BecomeRole → already in mirror, no-op
            _ => {}
        }
    }
}