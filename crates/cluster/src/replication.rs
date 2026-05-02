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
use raft::{
    AppendEntriesDecision, AppendResult, LogEntryRange, NodeId, RaftNode,
    RejectReason as RaftRejectReason, Role, TxId,
};
use spdlog::{error, info};
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
    tx: Arc<watch::Sender<bool>>,
}

impl ReplicationGate {
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { tx: Arc::new(tx) }, rx)
    }

    /// Set the gate to `playing`. Idempotent.
    pub(crate) fn set_playing(&self, playing: bool) {
        let _ = self.tx.send(playing);
    }

    pub(crate) fn hello(&self) {
    }
}

impl Drop for ReplicationGate {
    fn drop(&mut self) {
        panic!("replication_gate: dropped");
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
        info!("replication_loop[{}]: started", self_id);

        // Spawn per-peer outbound AE tasks.
        let cluster = match self.config.cluster.as_ref() {
            Some(c) => c,
            None => {
                info!(
                    "replication_loop[{}]: no cluster section; main task only",
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
                tailer: self.ledger.ledger().wal_tailer(),
                gate_rx: self.gate_rx.clone(),
                tick_interval,
                rpc_timeout: REPLICATION_RPC_TIMEOUT,
                append_max_bytes,
            };
            handles.push(tokio::task::spawn_local(peer_replication_loop(replicator)));
        }
        info!(
            "replication_loop[{}]: spawned {} peer task(s) (tick_interval={:?})",
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
                Err(e) => error!("replication_loop[{}]: peer task cancelled: {}", self_id, e),
            }
        }
        info!(
            "replication_loop[{}]: all peer tasks drained; exiting",
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
                            info!("replication_loop[{}]: ae channel closed; main loop exiting", self_id);
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
        let commit_index = self.ledger.ledger().last_commit_id();
        let write_index = self.ledger.ledger().last_compute_id();
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
        self.node.borrow_mut().advance(write_index, commit_index);
        let post_cci = self.node.borrow().cluster_commit_index();
        info!(
            "replication_loop[{}]: advance role={:?} write={}->{} commit={}->{} cluster_commit={}->{} (ledger compute={} commit={})",
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
        info!(
            "replication_loop[{}]: cmd AppendEntries from={} term={} prev_tx={} prev_term={} from_tx={} to_tx={} leader_commit={} wal_bytes={}",
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
            } => {
                info!(
                    "replication_loop[{}]: AE Reject reason={:?} truncate_after={:?}",
                    self_id, reason, truncate_after
                );
                if let Some(after) = truncate_after {
                    info!(
                        "replication_loop[{}]: AE Reject truncate WAL TODO (after={})",
                        self_id, after
                    );
                }
                let resp = build_ae_response(&self.node.borrow(), false, Some(reason));
                info!(
                    "replication_loop[{}]: AE Reject reply term={} last_write_id={} last_commit_id={}",
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
                    "replication_loop[{}]: AE Accept(heartbeat) leader_commit={} ledger_commit={} advance write={}->{} commit={}->{} cluster_commit={}->{}",
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

                let resp = build_ae_response(&self.node.borrow(), true, None);
                info!(
                    "replication_loop[{}]: AE Accept(heartbeat) reply term={} last_write_id={} last_commit_id={}",
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
                    "replication_loop[{}]: AE Accept(entries) range start_tx={} count={} term={} leader_commit={}",
                    self_id, range.start_tx_id, range.count, range.term, req.leader_commit_tx_id
                );
                let entries = decode_records(&req.wal_bytes);
                if let Err(e) = self.ledger.ledger().append_wal_entries(entries) {
                    error!(
                        "replication_loop[{}]: append_wal_entries failed start={} last={}: {}",
                        self_id, range.start_tx_id, last_tx_id, e
                    );
                    let resp = build_ae_response(
                        &self.node.borrow(),
                        false,
                        Some(RaftRejectReason::LogMismatch),
                    );
                    let _ = reply.send(resp);
                    return;
                }
                info!(
                    "replication_loop[{}]: AE Accept(entries) queued to ledger pipeline last_tx={}",
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
                    "replication_loop[{}]: AE Accept(entries) advance write={}->{} commit={}->{} cluster_commit={}->{} (ledger_commit={})",
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
                    "replication_loop[{}]: AE Accept(entries) reply term={} last_write_id={} last_commit_id={}",
                    self_id, resp.term, resp.last_write_id, resp.last_commit_id
                );
                let _ = reply.send(resp);
            }
        }
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
}

async fn peer_replication_loop(mut r: PeerReplicator) {
    info!(
        "replication_loop[{}]: peer={} task started (host={})",
        r.self_id, r.peer_id, r.host
    );
    loop {
        if !*r.gate_rx.borrow_and_update() {
            // Cursor is stale across pause windows: we may rejoin at a
            // different `next_index` if the leader regressed peers
            // before we get to play again.
            info!(
                "replication_loop[{}]: peer={} gate=false → reset tailer, await flip",
                r.self_id, r.peer_id
            );
            r.tailer.reset();
            match r.gate_rx.changed().await {
                Ok(()) => {
                    info!(
                        "replication_loop[{}]: peer={} gate flipped, resuming",
                        r.self_id, r.peer_id
                    );
                    continue;
                }
                Err(err) => {
                    error!(
                        "replication_loop[{}]: peer={} gate_rx.changed() error: {}",
                        r.self_id, r.peer_id, err
                    );
                    info!(
                        "replication_loop[{}]: peer={} gate dropped (shutdown)",
                        r.self_id, r.peer_id
                    );
                    break;
                }
            }
        }
        replicate_once(&mut r).await;
        let next = Instant::now() + r.tick_interval;
        tokio::select! {
            _ = sleep_until(next.into()) => {
                // Empty wakeup tick — intentionally not logged
                // (would dominate the log at heartbeat cadence).
            }
            result = r.gate_rx.changed() => {
                match result {
                    Ok(()) => {
                        info!(
                            "replication_loop[{}]: peer={} gate flipped during sleep",
                            r.self_id, r.peer_id
                        );
                    }
                    Err(_) => {
                        info!(
                            "replication_loop[{}]: peer={} gate dropped during sleep (shutdown)",
                            r.self_id, r.peer_id
                        );
                        break;
                    }
                }
            }
        }
    }
    info!(
        "replication_loop[{}]: peer={} task exiting",
        r.self_id, r.peer_id
    );
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

    let to_tx_id;
    let wal_bytes: Vec<u8>;
    if req.entries.is_empty() {
        to_tx_id = req.entries.start_tx_id.saturating_sub(1);
        wal_bytes = Vec::new();
        info!(
            "replication_loop[{}]: peer={} → AE heartbeat term={} prev_tx={} prev_term={} leader_commit={}",
            r.self_id,
            r.peer_id,
            req.term,
            req.prev_log_tx_id,
            req.prev_log_term,
            req.leader_commit
        );
    } else {
        let from_tx_id = req.entries.start_tx_id;
        let last_tx_id = req
            .entries
            .last_tx_id()
            .expect("non-empty range has a last_tx_id");
        let mut buffer = vec![0u8; r.append_max_bytes];
        let written = r.tailer.tail(from_tx_id, &mut buffer) as usize;
        if written == 0 {
            info!(
                "replication_loop[{}]: peer={} tailer empty for from_tx={} (skip; retry next tick)",
                r.self_id, r.peer_id, from_tx_id
            );
            return;
        }
        buffer.truncate(written);
        wal_bytes = buffer;
        to_tx_id = last_tx_id;
        info!(
            "replication_loop[{}]: peer={} → AE entries term={} prev_tx={} prev_term={} from_tx={} to_tx={} count={} leader_commit={} wal_bytes={}",
            r.self_id,
            r.peer_id,
            req.term,
            req.prev_log_tx_id,
            req.prev_log_term,
            from_tx_id,
            last_tx_id,
            req.entries.count,
            req.leader_commit,
            written
        );
    }

    let proto_req = proto::AppendEntriesRequest {
        leader_id: r.self_id,
        term: req.term,
        prev_tx_id: req.prev_log_tx_id,
        prev_term: req.prev_log_term,
        from_tx_id: req.entries.start_tx_id,
        to_tx_id,
        wal_bytes,
        leader_commit_tx_id: req.leader_commit,
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
            info!(
                "replication_loop[{}]: peer={} ← AE Success term={} last_write_id={} last_commit_id={}",
                r.self_id, r.peer_id, term, last_write_id, last_commit_id
            );
        }
        AppendResult::Reject {
            term,
            reason,
            last_write_id,
            last_commit_id,
        } => {
            info!(
                "replication_loop[{}]: peer={} ← AE Reject term={} reason={:?} last_write_id={} last_commit_id={}",
                r.self_id, r.peer_id, term, reason, last_write_id, last_commit_id
            );
        }
        AppendResult::Timeout => {
            info!(
                "replication_loop[{}]: peer={} ← AE Timeout (treated as no-progress)",
                r.self_id, r.peer_id
            );
        }
    }

    let mut node = r.node.borrow_mut();
    if let Some(mut p) = node.replication().peer(r.peer_id) {
        p.append_result(now, result);
        info!(
            "replication_loop[{}]: peer={} append_result fed: next_index={} match_index={}",
            r.self_id,
            r.peer_id,
            p.next_index(),
            p.match_index()
        );
    } else {
        info!(
            "replication_loop[{}]: peer={} append_result: no longer leader, dropping result",
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
                "replication_loop[{}]: peer={} connect to {} failed: {}",
                self_id, peer_id, host, e
            );
            return AppendResult::Timeout;
        }
        Err(_) => {
            error!(
                "replication_loop[{}]: peer={} connect to {} timed out after {:?}",
                self_id, peer_id, host, timeout
            );
            return AppendResult::Timeout;
        }
    };
    let resp = match tokio::time::timeout(timeout, client.append_entries(req)).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(e)) => {
            error!(
                "replication_loop[{}]: peer={} append_entries rpc failed: {}",
                self_id, peer_id, e
            );
            return AppendResult::Timeout;
        }
        Err(_) => {
            error!(
                "replication_loop[{}]: peer={} append_entries timed out after {:?}",
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

/// Build the proto response from raft's getters.
fn build_ae_response(
    node: &RaftNode<DurablePersistence>,
    success: bool,
    reject_reason: Option<RaftRejectReason>,
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
    }
}
