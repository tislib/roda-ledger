//! Replication driver — owns both directions of `AppendEntries`:
//!
//! - **Leader-side outbound**: per-peer tasks. Each task ticks at
//!   `cluster.replication_poll_ms`, pulls the next `AppendEntries`
//!   request from [`raft::PeerReplication::get_append_range`], reads
//!   WAL bytes from its own [`WalTailer`], sends the gRPC, and feeds
//!   the [`raft::AppendResult`] back through
//!   [`raft::PeerReplication::append_result`]. Gated on the shared
//!   [`ReplicationGate`] (only runs while `playing == true`).
//! - **Follower-side inbound**: a single main task drains the
//!   [`AppendEntriesMsg`] mpsc channel that the gRPC `NodeHandler`
//!   posts to, calls [`raft::RaftNode::validate_append_entries_request`],
//!   appends accepted entries to the ledger pipeline, and replies
//!   over the gRPC handler's `oneshot`. Same task also polls the
//!   ledger watermarks every `LEDGER_POLL_CADENCE` and feeds them
//!   into raft via `node.advance(write, commit)` so the leader's
//!   quorum self-slot can move regardless of inbound RPC traffic.
//!
//! Both directions share the [`Rc<RefCell<RaftNode>>`] and a clone
//! of the [`ReplicationGate`] sender. The gate has multiple owners
//! ([`crate::consensus::ConsensusLoop`] holds another clone); per-
//! peer task receivers exit only when **every** clone drops.
//!
//! Lifecycle: when the inbound AE channel closes (every `ae_tx`
//! clone dropped), the main task exits and drops its gate clone.
//! Per-peer tasks await the consensus loop's gate clone to also drop
//! before they see `Err` and exit. `run()` joins every per-peer
//! handle with panic re-raise so any task panic aborts the LocalSet
//! thread instead of silently dropping a peer's progress.

use crate::cluster_mirror::ClusterMirror;
use crate::command::AppendEntriesMsg;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::ledger_slot::LedgerSlot;
use ::proto::node as proto;
use ::proto::node::node_client::NodeClient;
use ledger::ledger::Ledger;
use log::{info, warn};
use raft::{
    AppendEntriesDecision, AppendResult, LogEntryRange, NodeId, RaftNode,
    RejectReason as RaftRejectReason, TxId,
};
use spdlog::{debug, error};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::{WalTailer, decode_records};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;

/// RPC timeout for outbound `AppendEntries`. Hardcoded for now;
/// promote to [`crate::config::ClusterSection`] if heartbeat-cadence
/// and per-RPC deadline ever need to diverge.
// TODO: promote to config alongside replication_poll_ms.
const REPLICATION_RPC_TIMEOUT: Duration = Duration::from_millis(500);

/// Cadence at which the main task polls the ledger for fresh commit /
/// write watermarks when no inbound AE arrives. Keeps the leader's
/// quorum self-slot moving without depending on RPC traffic.
const LEDGER_POLL_CADENCE: Duration = Duration::from_millis(50);

/// Pause/play signal source. Created in `RaftLoop::run`, cloned into
/// `ConsensusLoop` (so the election path can flip the gate on every
/// role transition) and kept here on `ReplicationLoop`'s main task
/// (for AE-side step-downs). Receivers exit when **every** clone
/// drops — that is the per-peer shutdown trigger.
pub(crate) struct ReplicationGate {
    tx: watch::Sender<bool>,
}

impl ReplicationGate {
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { tx }, rx)
    }

    /// Set the gate to `playing`. Idempotent.
    pub(crate) fn set_playing(&self, playing: bool) {
        self.tx.send(playing).unwrap();
    }
}

/// Replication driver. Built in `RaftLoop::run`; consumed by
/// [`Self::run`].
pub(crate) struct ReplicationLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    ledger: Arc<LedgerSlot>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,
    /// Owned by the main task; flipped on AE-side role changes.
    /// `Option` so the main task can drop it on exit, freeing per-
    /// peer receivers to see `Err(_)` once `ConsensusLoop`'s clone
    /// also drops.
    /// Read-side handed to per-peer tasks at spawn time.
    gate_rx: watch::Receiver<bool>,
    /// Inbound AE channel posted to by the gRPC `NodeHandler`.
    ae_rx: mpsc::Receiver<AppendEntriesMsg>,
    self_id: NodeId,

    /// Last `(write, commit)` pair forwarded into raft via
    /// `node.advance`. Skip the call when neither has moved.
    last_seen_write: TxId,
    last_seen_commit: TxId,
}

impl ReplicationLoop {
    pub(crate) fn new(
        node: Rc<RefCell<RaftNode<DurablePersistence>>>,
        ledger: Arc<LedgerSlot>,
        mirror: Arc<ClusterMirror>,
        config: Arc<Config>,
        gate_rx: watch::Receiver<bool>,
        ae_rx: mpsc::Receiver<AppendEntriesMsg>,
        self_id: NodeId,
    ) -> Self {
        let last_w = ledger.ledger().last_compute_id();
        let last_c = ledger.ledger().last_commit_id();
        Self {
            node,
            ledger,
            mirror,
            config,
            gate_rx,
            ae_rx,
            self_id,
            last_seen_write: last_w,
            last_seen_commit: last_c,
        }
    }

    pub(crate) async fn run(mut self) {
        let self_id = self.self_id;
        debug!("replication_loop/l[{}]: started", self_id);

        // Spawn per-peer outbound AE tasks.
        let cluster = match self.config.cluster.as_ref() {
            Some(c) => c,
            None => {
                debug!(
                    "replication_loop/l[{}]: no cluster section; main task only",
                    self_id
                );
                self.run_main_loop().await;
                return;
            }
        };
        let tick_interval = Duration::from_millis(cluster.replication_poll_ms);
        let append_max_bytes = cluster.append_entries_max_bytes;
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for peer in self.config.other_peers() {
            let replicator = PeerReplicator {
                node: self.node.clone(),
                mirror: self.mirror.clone(),
                peer_id: peer.peer_id,
                self_id,
                host: peer.host.clone(),
                ledger: self.ledger.clone(),
                tailer: self.ledger.ledger().wal_tailer(),
                gate_rx: self.gate_rx.clone(),
                tick_interval,
                rpc_timeout: REPLICATION_RPC_TIMEOUT,
                append_max_bytes,
            };
            handles.push(tokio::task::spawn_local(peer_replication_loop(replicator)));
        }
        debug!(
            "replication_loop/l[{}]: spawned {} peer task(s) (tick_interval={:?})",
            self_id,
            handles.len(),
            tick_interval
        );

        // Run the inbound-AE / ledger-advance loop until the AE
        // channel closes.
        self.run_main_loop().await;

        // Drain per-peer handles. Re-raise panics so any task panic
        // aborts the LocalSet thread; cancellation is benign.
        for h in handles {
            match h.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => error!(
                    "replication_loop/l[{}]: peer task cancelled: {}",
                    self_id, e
                ),
            }
        }
        debug!(
            "replication_loop/l[{}]: all peer tasks drained; exiting",
            self_id
        );
    }

    /// Single-task event loop: poll ledger watermarks, drain inbound
    /// AE, sleep on `LEDGER_POLL_CADENCE` between empty windows.
    async fn run_main_loop(&mut self) {
        let self_id = self.self_id;
        loop {
            self.poll_ledger_watermark();

            let sleep_at = Instant::now() + LEDGER_POLL_CADENCE;
            let sleep = tokio::time::sleep_until(sleep_at.into());
            tokio::pin!(sleep);

            tokio::select! {
                maybe_msg = self.ae_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            self.handle_append_entries(msg).await;
                        }
                        None => {
                            debug!("replication_loop/l[{}]: ae channel closed; main loop exiting", self_id);
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
    }

    /// Poll the ledger's `(compute, commit)` watermarks; if either
    /// has moved since last seen, forward both into raft and snapshot
    /// the mirror. Logs both pre- and post-advance index triples.
    fn poll_ledger_watermark(&mut self) {
        let self_id = self.self_id;
        let write_index = self.ledger.ledger().last_compute_id();
        let commit_index = self.ledger.ledger().last_commit_id();
        if write_index == self.last_seen_write && commit_index == self.last_seen_commit {
            return;
        }
        let (pre_w, pre_c, pre_cci, role_pre) = {
            let n = self.node.borrow();
            (
                n.write_index(),
                n.commit_index(),
                n.cluster_commit_index(),
                n.role(),
            )
        };
        self.node.borrow_mut().advance_write_index(write_index);
        self.node.borrow_mut().advance_commit_index(commit_index);
        let post_cci = self.node.borrow().cluster_commit_index();
        debug!(
            "replication_loop/l[{}]: advance role={:?} write={}->{} commit={}->{} cluster_commit={}->{} (ledger compute={} commit={})",
            self_id,
            role_pre,
            pre_w,
            write_index.max(pre_w),
            pre_c,
            commit_index.max(pre_c),
            pre_cci,
            post_cci,
            write_index,
            commit_index
        );
        self.last_seen_write = write_index;
        self.last_seen_commit = commit_index;
        self.mirror.snapshot_from(&self.node.borrow());
    }

    async fn handle_append_entries(&mut self, msg: AppendEntriesMsg) {
        let AppendEntriesMsg { req, reply } = msg;
        let self_id = self.self_id;
        debug!(
            "replication_loop/f[{}]: cmd AppendEntries from={} term={} prev_tx={} prev_term={} from_tx={} to_tx={} leader_commit={} wal_bytes={}",
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
        let now = Instant::now();
        let entries = if req.from_tx_id == 0 || req.to_tx_id < req.from_tx_id {
            LogEntryRange::empty()
        } else {
            LogEntryRange::new(req.from_tx_id, req.to_tx_id - req.from_tx_id + 1, req.term)
        };
        // Capture pre-validate raft state so the Reject branch below can
        // detect a silent cluster_commit_index clamp inside
        // validate_append_entries_request — that's the canary for
        // "leader asked to truncate below committed", which would mean
        // our reseed-watermark is unsafe.
        let pre_cluster_commit = self.node.borrow().cluster_commit_index();
        let pre_local_commit = self.node.borrow().commit_index();
        let pre_local_write = self.node.borrow().write_index();
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

        match decision {
            AppendEntriesDecision::Reject {
                reason,
                truncate_after,
                conflict_term,
                conflict_index,
            } => {
                warn!(
                    "replication_loop/f[{}]: AE Reject reason={:?} truncate_after={:?} conflict_term={} conflict_index={}",
                    self_id, reason, truncate_after, conflict_term, conflict_index
                );
                if let Some(after) = truncate_after {
                    let lcid = self.ledger.ledger().last_commit_id();
                    let lcomp = self.ledger.ledger().last_compute_id();
                    let unsafe_truncate = after < pre_cluster_commit;
                    if unsafe_truncate {
                        error!(
                            "replication_loop/f[{}]: AE Reject UNSAFE TRUNCATE — after={} < pre_cluster_commit={} (silent clamp): pre(local_commit={} local_write={}) ledger(commit_id={} compute_id={}) — Raft Leader Completeness violated upstream",
                            self_id,
                            after,
                            pre_cluster_commit,
                            pre_local_commit,
                            pre_local_write,
                            lcid,
                            lcomp
                        );
                    } else {
                        warn!(
                            "replication_loop/f[{}]: AE Reject reseed: after={} pre(cluster_commit={} local_commit={} local_write={}) ledger(commit_id={} compute_id={})",
                            self_id,
                            after,
                            pre_cluster_commit,
                            pre_local_commit,
                            pre_local_write,
                            lcid,
                            lcomp
                        );
                    }
                    if let Err(e) = self.reseed_ledger(after) {
                        error!(
                            "replication_loop/f[{}]: reseed_ledger({}) failed: {}",
                            self_id, after, e
                        );
                    }
                }
                let resp = build_ae_response(
                    &self.node.borrow(),
                    false,
                    Some(reason),
                    conflict_term,
                    conflict_index,
                );
                warn!(
                    "replication_loop/f[{}]: AE Reject reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
            AppendEntriesDecision::Accept { append: None } => {
                // let commit_index = self.ledger.ledger().last_commit_id();
                // self.node.borrow_mut().advance_commit_index(commit_index);
                // self.mirror.snapshot_from(&self.node.borrow());

                let resp = build_ae_response(&self.node.borrow(), true, None, 0, 0);
                let _ = reply.send(resp);
            }
            AppendEntriesDecision::Accept {
                append: Some(range),
            } => {
                let last_tx_id = range
                    .last_tx_id()
                    .expect("Accept{append: Some} carries a non-empty range");
                debug!(
                    "replication_loop/f[{}]: AE Accept(entries) range start_tx={} count={} term={} leader_commit={}",
                    self_id, range.start_tx_id, range.count, range.term, req.leader_commit_tx_id
                );
                let entries = decode_records(&req.wal_bytes);
                if let Err(e) = self.ledger.ledger().append_wal_entries(entries) {
                    error!(
                        "replication_loop/f[{}]: append_wal_entries failed start={} last={}: {}",
                        self_id, range.start_tx_id, last_tx_id, e
                    );
                    let resp = build_ae_response(
                        &self.node.borrow(),
                        false,
                        Some(RaftRejectReason::LogMismatch),
                        0,
                        0,
                    );
                    let _ = reply.send(resp);
                    return;
                }
                debug!(
                    "replication_loop/f[{}]: AE Accept(entries) queued to ledger pipeline last_tx={}",
                    self_id, last_tx_id
                );

                let commit_index = self.ledger.ledger().last_commit_id();
                let (pre_w, pre_c, pre_cci) = {
                    let n = self.node.borrow();
                    (n.write_index(), n.commit_index(), n.cluster_commit_index())
                };
                self.node.borrow_mut().advance_write_index(last_tx_id);
                self.node.borrow_mut().advance_commit_index(commit_index);
                let post_cci = self.node.borrow().cluster_commit_index();
                debug!(
                    "replication_loop/f[{}]: AE Accept(entries) advance write={}->{} commit={}->{} cluster_commit={}->{} (ledger_commit={})",
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

                let resp = build_ae_response(&self.node.borrow(), true, None, 0, 0);
                debug!(
                    "replication_loop/f[{}]: AE Accept(entries) reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
        }
    }

    /// Tear down the live `Ledger` and rebuild it via
    /// `start_with_recovery_until(watermark)` (ADR-0016 §9). Invoked
    /// from the AE-Reject path when raft asks to truncate below an
    /// already-applied tx_id: WAL is rewound on disk, in-memory
    /// pipeline state is rebuilt from the truncated WAL, and the new
    /// ledger is atomically swapped into the slot.
    ///
    /// OLD must be fully dropped (its pipeline OS threads joined)
    /// BEFORE NEW is started — otherwise both ledgers would write to
    /// the same data dir and races on `wal.bin` corrupt the truncation.
    /// We achieve that by swapping in a not-yet-started sentinel,
    /// `Arc::try_unwrap`-ing OLD (with a bounded retry loop for any
    /// in-flight gRPC clones), dropping it, and only then starting
    /// NEW and replacing the sentinel.
    fn reseed_ledger(&mut self, watermark: TxId) -> std::io::Result<()> {
        let self_id = self.self_id;
        info!(
            "replication_loop/f[{}]: reseed: starting (watermark={})",
            self_id, watermark
        );

        // 1) Replace the slot with a not-yet-started sentinel so no
        //    new caller picks up OLD. Existing short-lived clones
        //    (in-flight gRPC handlers) will release as they finish.
        let sentinel = Ledger::new(self.config.ledger.clone());
        let mut old_arc = self.ledger.replace(Arc::new(sentinel));

        // 2) Spin until OLD's strong count drops to 1, then unwrap and
        //    drop synchronously. `Ledger::Drop` joins pipeline threads
        //    and writes `wal.stop`; we have to be sure those threads
        //    are gone before NEW touches the data dir.
        let deadline = Instant::now() + Duration::from_secs(5);
        let old = loop {
            match Arc::try_unwrap(old_arc) {
                Ok(l) => break l,
                Err(arc) => {
                    old_arc = arc;
                    if Instant::now() > deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "reseed: old Ledger had outstanding refs after 5s on node {}",
                                self_id
                            ),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        };
        drop(old);
        debug!(
            "replication_loop/f[{}]: reseed: old ledger dropped",
            self_id
        );

        // 3) Build NEW and start it with bounded recovery — this
        //    physically truncates WAL above `watermark`, replays the
        //    surviving range to rebuild balances, and starts stages.
        let mut new = Ledger::new(self.config.ledger.clone());
        new.start_with_recovery_until(watermark)?;

        // 4) Atomically install NEW. The sentinel had no started
        //    stages so its Drop is cheap.
        let sentinel_arc = self.ledger.replace(Arc::new(new));
        drop(sentinel_arc);

        // 5) Reset cached watermarks so the next `poll_ledger_watermark`
        //    sees the truncated state and doesn't re-bump raft's
        //    `local_commit_index` from a stale ledger view.
        self.last_seen_write = watermark;
        self.last_seen_commit = watermark;

        debug!(
            "replication_loop/f[{}]: reseed: complete (watermark={})",
            self_id, watermark
        );
        Ok(())
    }
}

struct PeerReplicator {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    mirror: Arc<ClusterMirror>,
    peer_id: NodeId,
    self_id: NodeId,
    host: String,
    tailer: WalTailer,
    gate_rx: watch::Receiver<bool>,
    tick_interval: Duration,
    rpc_timeout: Duration,
    append_max_bytes: usize,
    ledger: Arc<LedgerSlot>,
}

async fn peer_replication_loop(mut r: PeerReplicator) {
    debug!(
        "replication_loop/l[{}]: peer={} task started (host={})",
        r.self_id, r.peer_id, r.host
    );
    loop {
        if !*r.gate_rx.borrow_and_update() {
            // Cursor is stale across pause windows: we may rejoin at a
            // different `next_index` if the leader regressed peers
            // before we get to play again.
            debug!(
                "replication_loop/l[{}]: peer={} gate=false → reset tailer, await flip",
                r.self_id, r.peer_id
            );
            r.tailer.reset();
            match r.gate_rx.changed().await {
                Ok(()) => {
                    // Re-fetch the tailer from the slot — a divergence
                    // reseed (ADR-0016 §9) while we were parked may
                    // have replaced the underlying `Arc<Storage>`,
                    // leaving the cached one pointing at a torn-down
                    // ledger.
                    r.tailer = r.ledger.ledger().wal_tailer();
                    debug!(
                        "replication_loop/l[{}]: peer={} gate flipped, resuming",
                        r.self_id, r.peer_id
                    );
                    continue;
                }
                Err(err) => {
                    error!(
                        "replication_loop/l[{}]: peer={} gate_rx.changed() error: {}",
                        r.self_id, r.peer_id, err
                    );
                    debug!(
                        "replication_loop/l[{}]: peer={} gate dropped (shutdown)",
                        r.self_id, r.peer_id
                    );
                    break;
                }
            }
        }
        replicate_once(&mut r).await;

        // self advance
        let write_index = r.ledger.ledger().last_compute_id();
        let commit_index = r.ledger.ledger().last_commit_id();
        r.node.borrow_mut().advance_write_index(write_index);
        r.node.borrow_mut().advance_commit_index(commit_index);
        r.mirror.snapshot_from(&r.node.borrow());

        let next = Instant::now() + r.tick_interval;
        tokio::select! {
            _ = sleep_until(next.into()) => {
                // Empty wakeup tick — intentionally not logged
                // (would dominate the log at heartbeat cadence).
            }
            result = r.gate_rx.changed() => {
                match result {
                    Ok(()) => {
                        debug!(
                            "replication_loop/l[{}]: peer={} gate flipped during sleep",
                            r.self_id, r.peer_id
                        );
                    }
                    Err(_) => {
                        debug!(
                            "replication_loop/l[{}]: peer={} gate dropped during sleep (shutdown)",
                            r.self_id, r.peer_id
                        );
                        break;
                    }
                }
            }
        }
    }
    debug!(
        "replication_loop/l[{}]: peer={} task exiting",
        r.self_id, r.peer_id
    );
}

/// Outcome of the leader-side "build the next AE" step. Pure function;
/// no side effects on `RaftNode` or `WalTailer` here — the orchestrator
/// in `replicate_once` decides whether to send the request or reset the
/// tailer based on this.
#[derive(Debug)]
pub(crate) enum AeAction {
    /// Issue this proto request to the peer.
    Send(proto::AppendEntriesRequest),
    /// Tail returned 0 bytes for a non-empty entries range; the cursor
    /// has been exhausted past undelivered bytes. The orchestrator
    /// resets the tailer and skips the tick.
    ResetTailerSkip,
}

/// Whether the tail call produced bytes or came up empty for a
/// non-empty entries range.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TailAttempt {
    Bytes(Vec<u8>),
    Empty,
}

/// Pure decision: given the raft-issued AE request and the tailer's
/// outcome for any non-empty entries range, produce the action the
/// orchestrator should take. Heartbeats (empty entries range) bypass
/// the tailer entirely.
pub(crate) fn build_ae_action(
    self_id: NodeId,
    req: &raft::AppendEntriesRequest,
    tail: Option<TailAttempt>,
) -> AeAction {
    let common = |to_tx_id: TxId, wal_bytes: Vec<u8>| proto::AppendEntriesRequest {
        leader_id: self_id,
        term: req.term,
        prev_tx_id: req.prev_log_tx_id,
        prev_term: req.prev_log_term,
        from_tx_id: req.entries.start_tx_id,
        to_tx_id,
        wal_bytes,
        leader_commit_tx_id: req.leader_commit,
    };
    if req.entries.is_empty() {
        debug_assert!(tail.is_none(), "heartbeat AE must not consult the tailer");
        let to_tx_id = req.entries.start_tx_id.saturating_sub(1);
        return AeAction::Send(common(to_tx_id, Vec::new()));
    }
    match tail.expect("entries AE must include a tail attempt") {
        TailAttempt::Empty => AeAction::ResetTailerSkip,
        TailAttempt::Bytes(wal_bytes) => {
            let to_tx_id = req
                .entries
                .last_tx_id()
                .expect("non-empty range has a last_tx_id");
            AeAction::Send(common(to_tx_id, wal_bytes))
        }
    }
}

async fn replicate_once(r: &mut PeerReplicator) {
    let now = Instant::now();
    let request = {
        let mut node = r.node.borrow_mut();
        match node.replication().peer(r.peer_id) {
            Some(mut p) => p.get_append_range(now),
            None => {
                // Not leader for this peer — silent (would log every
                // tick during follower phase).
                return;
            }
        }
    };
    let Some(req) = request else {
        // Gated on heartbeat / in_flight — silent (every tick).
        return;
    };

    let tail_attempt = if req.entries.is_empty() {
        None
    } else {
        let from_tx_id = req.entries.start_tx_id;
        let mut buffer = vec![0u8; r.append_max_bytes];
        let written = r.tailer.tail(from_tx_id, &mut buffer) as usize;
        if written == 0 {
            Some(TailAttempt::Empty)
        } else {
            info!("wrote {} out of {} max bytes", written, r.append_max_bytes);
            buffer.truncate(written);
            Some(TailAttempt::Bytes(buffer))
        }
    };

    let proto_req = match build_ae_action(r.self_id, &req, tail_attempt) {
        AeAction::Send(p) => {
            debug!(
                "replication_loop/l[{}]: peer={} → AE term={} prev_tx={} prev_term={} from_tx={} to_tx={} count={} leader_commit={} wal_bytes={}",
                r.self_id,
                r.peer_id,
                p.term,
                p.prev_tx_id,
                p.prev_term,
                p.from_tx_id,
                p.to_tx_id,
                req.entries.count,
                p.leader_commit_tx_id,
                p.wal_bytes.len()
            );
            p
        }
        AeAction::ResetTailerSkip => {
            // Cursor exhausted: reset so the next tick re-seeks from
            // from_tx_id. Don't clear in_flight here — the existing
            // 500ms rpc_timeout sweep handles that. Clearing it would
            // re-arm get_append_range immediately and, on a freshly
            // elected leader with stale term_at_tx for early tx_ids,
            // cause a heartbeat with mismatched prev_log_term that
            // triggers a spurious follower reseed loop.
            debug!(
                "replication_loop/l[{}]: peer={} tailer empty for from_tx={}; resetting tailer",
                r.self_id, r.peer_id, req.entries.start_tx_id
            );
            r.tailer.reset();
            return;
        }
    };

    let result = send_append_entries_rpc(
        r.self_id,
        r.peer_id,
        r.host.clone(),
        proto_req,
        r.rpc_timeout,
    )
    .await;

    match &result {
        AppendResult::Success {
            term,
            last_write_id,
            last_commit_id,
        } => {
            debug!(
                "replication_loop/l[{}]: peer={} ← AE Success term={} last_write_id={} last_commit_id={}",
                r.self_id, r.peer_id, term, last_write_id, last_commit_id
            );
        }
        AppendResult::Reject {
            term,
            reason,
            last_write_id,
            last_commit_id,
            conflict_term,
            conflict_index,
        } => {
            warn!(
                "replication_loop/l[{}]: peer={} ← AE Reject term={} reason={:?} last_write_id={} last_commit_id={} conflict_term={} conflict_index={}",
                r.self_id,
                r.peer_id,
                term,
                reason,
                last_write_id,
                last_commit_id,
                conflict_term,
                conflict_index
            );
        }
        AppendResult::Timeout => {
            debug!(
                "replication_loop/l[{}]: peer={} ← AE Timeout (treated as no-progress)",
                r.self_id, r.peer_id
            );
        }
    }

    let mut node = r.node.borrow_mut();
    if let Some(mut p) = node.replication().peer(r.peer_id) {
        p.append_result(now, result);
        debug!(
            "replication_loop/l[{}]: peer={} append_result fed: next_index={} match_index={}",
            r.self_id,
            r.peer_id,
            p.next_index(),
            p.match_index()
        );
    } else {
        debug!(
            "replication_loop/l[{}]: peer={} append_result: no longer leader, dropping result",
            r.self_id, r.peer_id
        );
    }
    r.mirror.snapshot_from(&node);
}

async fn send_append_entries_rpc(
    self_id: NodeId,
    peer_id: NodeId,
    host: String,
    req: proto::AppendEntriesRequest,
    timeout: Duration,
) -> AppendResult {
    let mut client = match tokio::time::timeout(timeout, NodeClient::connect(host.clone())).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            error!(
                "replication_loop/l[{}]: peer={} connect to {} failed: {}",
                self_id, peer_id, host, e
            );
            return AppendResult::Timeout;
        }
        Err(_) => {
            error!(
                "replication_loop/l[{}]: peer={} connect to {} timed out after {:?}",
                self_id, peer_id, host, timeout
            );
            return AppendResult::Timeout;
        }
    };
    let resp = match tokio::time::timeout(timeout, client.append_entries(req)).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(e)) => {
            error!(
                "replication_loop/l[{}]: peer={} append_entries rpc failed: {}",
                self_id, peer_id, e
            );
            return AppendResult::Timeout;
        }
        Err(_) => {
            error!(
                "replication_loop/l[{}]: peer={} append_entries timed out after {:?}",
                self_id, peer_id, timeout
            );
            return AppendResult::Timeout;
        }
    };
    if resp.success {
        AppendResult::Success {
            term: resp.term,
            last_write_id: resp.last_write_id,
            last_commit_id: resp.last_commit_id,
        }
    } else {
        AppendResult::Reject {
            term: resp.term,
            reason: proto_reject_to_raft(resp.reject_reason),
            last_write_id: resp.last_write_id,
            last_commit_id: resp.last_commit_id,
            conflict_term: resp.conflict_term,
            conflict_index: resp.conflict_index,
        }
    }
}

fn proto_reject_to_raft(code: u32) -> RaftRejectReason {
    if code == proto::RejectReason::RejectTermStale as u32 {
        RaftRejectReason::TermBehind
    } else {
        // RejectPrevMismatch and the legacy reject codes (CRC,
        // sequence, WAL append, not-follower) all collapse into
        // LogMismatch — they tell the leader to walk `next_index`
        // back. The §5.3 catch-up path will discover the agreement
        // point on subsequent RPCs.
        RaftRejectReason::LogMismatch
    }
}

/// Build the proto response from raft's getters. `conflict_term` /
/// `conflict_index` are the Raft §5.3 fast-backoff hint and are only
/// non-zero on `LogMismatch` rejects.
fn build_ae_response(
    node: &RaftNode<DurablePersistence>,
    success: bool,
    reject_reason: Option<RaftRejectReason>,
    conflict_term: u64,
    conflict_index: u64,
) -> proto::AppendEntriesResponse {
    let reject_code = match reject_reason {
        None => proto::RejectReason::RejectNone as u32,
        Some(RaftRejectReason::TermBehind) => proto::RejectReason::RejectTermStale as u32,
        Some(RaftRejectReason::LogMismatch) => proto::RejectReason::RejectPrevMismatch as u32,
        // RpcTimeout is a leader-side bookkeeping signal; never
        // surfaces on a follower's reply path.
        Some(RaftRejectReason::RpcTimeout) => proto::RejectReason::RejectNone as u32,
    };
    proto::AppendEntriesResponse {
        term: node.current_term(),
        success,
        last_commit_id: node.commit_index(),
        last_write_id: node.write_index(),
        reject_reason: reject_code,
        conflict_term,
        conflict_index,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the pure decision logic that `replicate_once`
    //! sits on top of. The orchestration itself (gRPC dispatch,
    //! `RaftNode::replication()` borrow dance) is exercised by the
    //! integration tests in `crates/cluster/tests/`; here we lock in
    //! the behaviors that have been subtle in practice:
    //!
    //! - Heartbeats (`req.entries.is_empty()`) skip the tailer
    //!   entirely and ship with empty `wal_bytes`.
    //! - Entries AEs with non-zero tail bytes ship those bytes
    //!   verbatim with the matching `to_tx_id`.
    //! - Entries AEs whose tail returned 0 produce a
    //!   `ResetTailerSkip` so the orchestrator re-seeks the cursor
    //!   on the next tick instead of shipping a phantom heartbeat
    //!   with stale `prev_log_term` (which would loop a freshly-
    //!   elected leader into spurious follower reseeds — see the
    //!   `no_leader_blocks_writes` safety test).
    //!
    //! `LogEntryRange::empty()` has `start_tx_id = 0`; the
    //! heartbeat's `from_tx_id`/`to_tx_id` shape is `(0, 0)` — the
    //! follower treats `to_tx_id < from_tx_id` as empty too.
    use super::*;
    use raft::{AppendEntriesRequest, LogEntryRange};

    fn mk_req(start_tx: TxId, count: u64, term: u64) -> AppendEntriesRequest {
        AppendEntriesRequest {
            to: 2,
            term: 5,
            prev_log_tx_id: start_tx.saturating_sub(1),
            prev_log_term: 3,
            entries: if count == 0 {
                LogEntryRange::empty()
            } else {
                LogEntryRange::new(start_tx, count, term)
            },
            leader_commit: 7,
        }
    }

    #[test]
    fn heartbeat_request_skips_tailer_and_ships_empty_bytes() {
        let req = mk_req(0, 0, 0);
        let action = build_ae_action(1, &req, None);
        let p = match action {
            AeAction::Send(p) => p,
            AeAction::ResetTailerSkip => panic!("heartbeat should send"),
        };
        assert_eq!(p.leader_id, 1);
        assert_eq!(p.term, 5);
        assert_eq!(p.wal_bytes, Vec::<u8>::new());
        assert_eq!(p.from_tx_id, 0);
        // Heartbeat's to_tx_id = start_tx_id.saturating_sub(1) so the
        // follower's `to_tx_id < from_tx_id` check yields an empty range.
        assert_eq!(p.to_tx_id, 0);
        assert_eq!(p.leader_commit_tx_id, 7);
    }

    #[test]
    fn entries_request_with_bytes_ships_them_and_sets_to_tx_id() {
        let req = mk_req(10, 3, 4);
        let bytes = vec![0xAA; 120];
        let action = build_ae_action(1, &req, Some(TailAttempt::Bytes(bytes.clone())));
        let p = match action {
            AeAction::Send(p) => p,
            AeAction::ResetTailerSkip => panic!("entries with bytes should send"),
        };
        assert_eq!(p.from_tx_id, 10);
        assert_eq!(p.to_tx_id, 12);
        assert_eq!(p.wal_bytes, bytes);
        assert_eq!(p.term, 5);
        assert_eq!(p.prev_tx_id, 9);
        assert_eq!(p.prev_term, 3);
    }

    #[test]
    fn entries_request_with_empty_tail_yields_reset_skip() {
        // The critical bug-locus: a tail==0 must NOT degrade to a
        // heartbeat-shaped AE. Sending a heartbeat carries the
        // leader's stale `prev_log_term` from its persistence, which
        // can mismatch a freshly-observed term on the follower and
        // trigger a follower reseed loop after re-election. The
        // orchestrator instead resets the tailer and lets the next
        // tick re-seek + reload the bytes.
        let req = mk_req(10, 3, 4);
        let action = build_ae_action(1, &req, Some(TailAttempt::Empty));
        assert!(
            matches!(action, AeAction::ResetTailerSkip),
            "tail==0 must produce ResetTailerSkip, not Send (got {action:?})"
        );
    }

    #[test]
    #[should_panic(expected = "heartbeat AE must not consult the tailer")]
    fn heartbeat_with_tail_input_panics_in_debug() {
        // Guard: the caller of `build_ae_action` must not even call
        // the tailer for heartbeats. Passing a tail attempt is a
        // programming error and `replicate_once` is shaped to avoid it.
        let req = mk_req(0, 0, 0);
        let _ = build_ae_action(1, &req, Some(TailAttempt::Empty));
    }
}
