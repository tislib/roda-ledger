//! Role-gated leader-side replication.
//!
//! Architecture:
//!
//! - [`ReplicationGate`] is the watcher. It wraps a
//!   [`tokio::sync::watch::Sender<bool>`]; the command loop owns it
//!   and toggles it from the `Action::BecomeRole` dispatch arm. `true`
//!   means "play" (this node is leader); `false` means "pause".
//! - [`ReplicationLoop::run`] spawns one [`peer_replication_loop`]
//!   task per peer at startup and awaits all of them. Each task owns
//!   its own [`WalTailer`] (cursors are stateful per-peer), an
//!   [`Rc<RefCell<RaftNode>>`] clone, and a `watch::Receiver<bool>`
//!   clone.
//! - While the gate is `true`, each peer task ticks at
//!   `cluster.replication_poll_ms`: it pulls the next `AppendEntries`
//!   via [`raft::PeerReplication::get_append_range`], reads WAL bytes
//!   from its tailer, sends the gRPC, and feeds the
//!   [`raft::AppendResult`] back via
//!   [`raft::PeerReplication::append_result`] directly. No
//!   [`crate::Command`] variant is involved — the per-peer task talks
//!   to raft via the shared `RefCell` on the same `LocalSet`.
//! - Lifecycle: when the command loop exits, it drops the
//!   [`ReplicationGate`] (and with it the underlying `watch::Sender`).
//!   Every peer task's `gate_rx.changed().await` returns `Err`; each
//!   task returns; [`ReplicationLoop::run`] joins the handles so
//!   in-flight gRPCs complete before the `LocalSet` tears down. There
//!   is no explicit `shutdown()` method.

use crate::cluster_mirror::ClusterMirror;
use crate::config::Config;
use crate::durable::DurablePersistence;
use crate::ledger_slot::LedgerSlot;
use ::proto::node as proto;
use ::proto::node::node_client::NodeClient;
use raft::{AppendResult, NodeId, RaftNode, RejectReason as RaftRejectReason};
use spdlog::{error, info};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::WalTailer;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep_until;

/// RPC timeout for outbound `AppendEntries`. Hardcoded for now;
/// promote to [`crate::config::ClusterSection`] if heartbeat-cadence
/// and per-RPC deadline ever need to diverge.
// TODO: promote to config alongside replication_poll_ms.
const REPLICATION_RPC_TIMEOUT: Duration = Duration::from_millis(500);

/// Pause/play signal source. Owned by the command loop; toggled in
/// the `Action::BecomeRole` dispatch arm. Receivers exit when this
/// drops — that is the replication loop's shutdown trigger.
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
        let _ = self.tx.send(playing);
    }
}

/// Per-peer task spawner. Built in `RaftLoop::run`; consumed by
/// [`Self::run`].
pub(crate) struct ReplicationLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    ledger: Arc<LedgerSlot>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,
    gate_rx: watch::Receiver<bool>,
    self_id: NodeId,
}

impl ReplicationLoop {
    pub(crate) fn new(
        node: Rc<RefCell<RaftNode<DurablePersistence>>>,
        ledger: Arc<LedgerSlot>,
        mirror: Arc<ClusterMirror>,
        config: Arc<Config>,
        gate_rx: watch::Receiver<bool>,
        self_id: NodeId,
    ) -> Self {
        Self {
            node,
            ledger,
            mirror,
            config,
            gate_rx,
            self_id,
        }
    }

    pub(crate) async fn run(self) {
        let self_id = self.self_id;
        let cluster = match self.config.cluster.as_ref() {
            Some(c) => c,
            None => {
                info!(
                    "replication_loop[{}]: no cluster section; exiting",
                    self_id
                );
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

        for h in handles {
            let _ = h.await;
        }
        info!("replication_loop[{}]: all peer tasks drained; exiting", self_id);
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
                Err(_) => {
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
    info!("replication_loop[{}]: peer={} task exiting", r.self_id, r.peer_id);
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
            // Tailer didn't surface bytes (segment rotation, transient
            // race). Skip; next tick re-pulls a fresh request.
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

    let result =
        send_append_entries_rpc(r.self_id, r.peer_id, r.host.clone(), proto_req, r.rpc_timeout)
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
