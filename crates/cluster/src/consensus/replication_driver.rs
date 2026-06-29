use crate::config::PeerConfig;
use crate::consensus::replication::ReplicationInputStream;
use crate::consensus::state::Consensus;
use proto::node::node_client::NodeClient;
use proto::node::{
    ReplicationFollowerMessage, ReplicationFollowerMessageIndexUpdate, ReplicationLeaderMessage,
    ReplicationLeaderMessageHandshake, ReplicationLeaderMessageHeartBeat,
    ReplicationLeaderMessageWalUpdate, replication_follower_message, replication_leader_message,
};
use raft::{AppendResult, Role};
use spdlog::{debug, error, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::{WalTailer, decode_records, last_tx_id_in};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::{JoinSet, yield_now};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Status, Streaming, transport::Channel};

const RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

impl Consensus {
    pub async fn run_replication_driver(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let nid = self.node_id();
        let input_rx = self
            .replication_input_rx
            .lock()
            .expect("replication_input_rx mutex poisoned")
            .take()
            .ok_or("replication driver started twice")?;

        debug!("replication_driver[{}]: started", nid);
        let stream_loop = tokio::spawn({
            let this = self.clone();
            let c = cancel.clone();
            async move { this.replication_stream_loop(input_rx, c).await }
        });
        let push_loop = tokio::spawn({
            let this = self.clone();
            let c = cancel.clone();
            async move { this.replication_push_loop(c).await }
        });

        cancel.cancelled().await;
        let _ = stream_loop.await;
        let _ = push_loop.await;
        debug!("replication_driver[{}]: exited", nid);
        Ok(())
    }

    async fn replication_stream_loop(
        self: Arc<Self>,
        mut input_rx: mpsc::Receiver<ReplicationInputStream>,
        cancel: CancellationToken,
    ) {
        let nid = self.node_id();
        let mut active_cancel: Option<CancellationToken> = None;
        let mut tasks: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                maybe = input_rx.recv() => {
                    let Some(stream) = maybe else { break };
                    let had_prev = active_cancel.is_some();
                    if let Some(prev) = active_cancel.take() { prev.cancel(); }
                    debug!(
                        "replication_stream[{}]: new inbound stream (cancelled_previous={})",
                        nid, had_prev
                    );
                    let ct = cancel.child_token();
                    active_cancel = Some(ct.clone());
                    let this = self.clone();
                    tasks.spawn(async move {
                        run_follower_session(this, stream, ct).await;
                    });
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(e)) = joined {
                        warn!(
                            "replication_stream[{}]: follower session joined with error: {}",
                            nid, e
                        );
                    }
                }
            }
        }
        if let Some(c) = active_cancel.take() {
            c.cancel();
        }
        while tasks.join_next().await.is_some() {}
    }

    async fn replication_push_loop(self: Arc<Self>, cancel: CancellationToken) {
        let nid = self.node_id();
        let mut role_rx = self.role_subscribe();
        let mut leader_cancel: Option<CancellationToken> = None;
        let mut leader_tasks: JoinSet<()> = JoinSet::new();

        // Singleton boots already-Leader; watcher fires only on changes, so seed.
        let mut is_leader = matches!(*role_rx.borrow_and_update(), Role::Leader);
        if is_leader {
            debug!(
                "replication_driver[{}]: became Leader; spawning peer pushers",
                nid
            );
            leader_cancel = Some(self.clone().spawn_peer_pushers(&cancel, &mut leader_tasks));
        }

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                changed = role_rx.changed() => {
                    if changed.is_err() { break; }
                    let now_leader = matches!(*role_rx.borrow_and_update(), Role::Leader);
                    if now_leader && !is_leader {
                        debug!(
                            "replication_driver[{}]: became Leader; spawning peer pushers",
                            nid
                        );
                        leader_cancel = Some(self.clone().spawn_peer_pushers(&cancel, &mut leader_tasks));
                    } else if !now_leader && is_leader {
                        debug!(
                            "replication_driver[{}]: lost leadership; cancelling peer pushers",
                            nid
                        );
                        if let Some(lc) = leader_cancel.take() { lc.cancel(); }
                        while leader_tasks.join_next().await.is_some() {}
                    }
                    is_leader = now_leader;
                }
            }
        }

        if let Some(lc) = leader_cancel.take() {
            lc.cancel();
        }
        while leader_tasks.join_next().await.is_some() {}
    }

    fn spawn_peer_pushers(
        self: Arc<Self>,
        parent: &CancellationToken,
        tasks: &mut JoinSet<()>,
    ) -> CancellationToken {
        let nid = self.node_id();
        let lc = parent.child_token();
        for peer in self.config.other_peers().cloned().collect::<Vec<_>>() {
            debug!(
                "replication_leader[{} peer={}]: pusher spawned",
                nid, peer.peer_id
            );
            let this = self.clone();
            let ct = lc.clone();
            tasks.spawn(async move {
                run_peer_push(this, peer, ct).await;
            });
        }
        lc
    }
}

async fn run_follower_session(
    consensus: Arc<Consensus>,
    stream: ReplicationInputStream,
    cancel: CancellationToken,
) {
    let nid = consensus.node_id();
    let ReplicationInputStream { mut inbound, tx } = stream;

    let hs = tokio::select! {
        biased;
        _ = cancel.cancelled() => return,
        msg = inbound.message() => match msg {
            Ok(Some(ReplicationLeaderMessage {
                message: Some(replication_leader_message::Message::Handshake(h)),
            })) => h,
            Ok(Some(_)) => {
                error!("replication_follower[{}]: first frame was not Handshake", nid);
                return;
            }
            Ok(None) => {
                debug!("replication_follower[{}]: stream closed before handshake", nid);
                return;
            }
            Err(e) => {
                error!("replication_follower[{}]: handshake recv error: {}", nid, e);
                return;
            }
        },
    };

    debug!(
        "replication_follower[{}]: got handshake leader={} term={} prev_log_tx_id={} prev_log_term={}",
        nid, hs.leader_id, hs.leader_term, hs.prev_log_tx_id, hs.prev_log_term
    );
    let resp = consensus.replication_follower_handshake(hs).await;
    debug!(
        "replication_follower[{}]: handshake decision success={} reject={} last_term_curr_tx_id={}",
        nid, resp.success, resp.reject_reason, resp.last_term_curr_tx_id
    );
    let accepted = resp.success;
    let _ = tx
        .send(Ok(ReplicationFollowerMessage {
            message: Some(replication_follower_message::Message::HandshakeResponse(
                resp,
            )),
        }))
        .await;
    if !accepted {
        return;
    }

    // Split the session into two parallel tasks so applying never blocks on the
    // ack and the ack reflects the follower's true applied index:
    //  - receiver (Loop 1): apply leader messages to the ledger, no inline ack;
    //  - acker (Loop 2): report the follower's snapshot index whenever it moves.
    let session_cancel = cancel.child_token();
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn({
        let c = consensus.clone();
        let ct = session_cancel.clone();
        async move { run_follower_receiver(c, inbound, ct).await }
    });
    tasks.spawn({
        let c = consensus.clone();
        let ct = session_cancel.clone();
        async move { run_follower_acker(c, tx, ct).await }
    });
    let _ = tasks.join_next().await;
    session_cancel.cancel();
    while tasks.join_next().await.is_some() {}
}

/// Loop 1: receive leader messages and apply them to the ledger. No ack — the
/// follower's progress is reported reactively by [`run_follower_acker`].
async fn run_follower_receiver(
    consensus: Arc<Consensus>,
    mut inbound: Streaming<ReplicationLeaderMessage>,
    cancel: CancellationToken,
) {
    let nid = consensus.node_id();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            msg = inbound.message() => match msg {
                Ok(Some(ReplicationLeaderMessage { message: Some(m) })) => match m {
                    replication_leader_message::Message::WalUpdate(u) => {
                        if !apply_wal_update(&consensus, u) { return; }
                    }
                    replication_leader_message::Message::Heartbeat(h) => {
                        apply_heartbeat(&consensus, h);
                    }
                    replication_leader_message::Message::Handshake(_) => {
                        error!("replication_follower[{}]: unexpected mid-stream handshake", nid);
                        return;
                    }
                },
                Ok(_) => return,
                Err(e) => {
                    debug!("replication_follower[{}]: transport closed: {}", nid, e);
                    return;
                }
            }
        }
    }
}

/// Loop 2: park on the follower's snapshot index and send an `IndexUpdate` to
/// the leader each time it advances — the reactive replacement for the inline
/// per-message ack. Reports the *true* applied index (post-fsync), not the
/// stale read the old code took right after enqueue.
async fn run_follower_acker(
    consensus: Arc<Consensus>,
    tx: Sender<Result<ReplicationFollowerMessage, Status>>,
    cancel: CancellationToken,
) {
    let nid = consensus.node_id();
    let snapshot = consensus.waiter().snapshot();
    let mut last_acked = snapshot.get();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = snapshot.wait_reach(last_acked + 1) => {
                let current = snapshot.get();
                let sent = tx
                    .send(Ok(ReplicationFollowerMessage {
                        message: Some(replication_follower_message::Message::IndexUpdate(
                            ReplicationFollowerMessageIndexUpdate {
                                local_commit_id: current,
                            },
                        )),
                    }))
                    .await;
                if sent.is_err() {
                    debug!("replication_follower[{}]: ack channel closed", nid);
                    return;
                }
                last_acked = current;
            }
        }
    }
}

fn apply_wal_update(c: &Arc<Consensus>, u: ReplicationLeaderMessageWalUpdate) -> bool {
    let nid = c.node_id();
    let entries = decode_records(&u.wal_binary);
    let count = entries.len();
    let ledger = c.ledger.current();
    if let Err(e) = ledger.append_wal_entries(entries) {
        error!(
            "replication_follower[{}]: append_wal_entries failed: {}",
            nid, e
        );
        return false;
    }
    let local_index = ledger.last_snapshot_id();
    debug!(
        "replication_follower[{}]: WAL update applied bytes={} records={} local_commit={} leader_cluster_commit={}",
        nid,
        u.wal_binary.len(),
        count,
        local_index,
        u.cluster_commit_id
    );
    {
        let mut node = c.raft_node.lock().expect("raft mutex poisoned");
        node.note_leader_activity(Instant::now());
        node.advance_local_index(local_index);
        node.advance_cluster_index(u.cluster_commit_id);
    }
    c.publish_cluster_commit();
    true
}

fn apply_heartbeat(c: &Arc<Consensus>, h: ReplicationLeaderMessageHeartBeat) {
    let nid = c.node_id();
    let local_index = c.ledger.current().last_snapshot_id();
    {
        let mut node = c.raft_node.lock().expect("raft mutex poisoned");
        node.note_leader_activity(Instant::now());
        // Sync local_commit_index from the ledger so the subsequent
        // advance_cluster_index doesn't clamp to a stale value.
        node.advance_local_index(local_index);
        node.advance_cluster_index(h.cluster_commit_id);
    }
    c.publish_cluster_commit();
    debug!(
        "replication_follower[{}]: heartbeat received cluster_commit={} local_index={}",
        nid, h.cluster_commit_id, local_index
    );
}

#[allow(clippy::large_enum_variant)]
enum HandshakeOutcome {
    Active {
        out_tx: mpsc::Sender<ReplicationLeaderMessage>,
        inbound: Streaming<ReplicationFollowerMessage>,
        snap_term: u64,
        last_term_curr_tx_id: u64,
    },
    Rejected {
        last_term_curr_tx_id: u64,
    },
    Cancelled,
    Transient,
}

enum PushOutcome {
    /// A `WalUpdate` was sent. `drained` is true when the read filled
    /// less than the full buffer — i.e. the WAL had nothing more to
    /// hand us this round and the next iteration would almost
    /// certainly come up empty. The sender backs off in that case so
    /// the loop doesn't spin between trivial pushes when load is light.
    Sent {
        drained: bool,
        /// Highest `tx_id` in the batch just shipped (resolved from the WAL
        /// bytes). The sender tracks this to park on `commit.wait_reach(+1)`.
        last_tx_id: u64,
    },
    Idle,
    StreamClosed,
}

async fn run_peer_push(consensus: Arc<Consensus>, peer: PeerConfig, cancel: CancellationToken) {
    let nid = consensus.node_id();
    let pid = peer.peer_id;
    let mut anchor_override: Option<u64> = None;
    loop {
        if cancel.is_cancelled() {
            debug!("replication_leader[{} peer={}]: pusher cancelled", nid, pid);
            return;
        }
        match open_session(&consensus, &peer, anchor_override, &cancel).await {
            HandshakeOutcome::Active {
                out_tx,
                inbound,
                snap_term,
                last_term_curr_tx_id,
            } => {
                seed_peer_match_index(&consensus, peer.peer_id, snap_term, last_term_curr_tx_id);
                debug!(
                    "replication_leader[{} peer={}]: handshake accepted, seeded match_index={}",
                    nid, pid, last_term_curr_tx_id
                );
                anchor_override = None;
                run_peer_session(
                    &consensus,
                    &peer,
                    out_tx,
                    inbound,
                    last_term_curr_tx_id,
                    &cancel,
                )
                .await;
            }
            HandshakeOutcome::Rejected {
                last_term_curr_tx_id,
            } => {
                anchor_override = Some(last_term_curr_tx_id);
            }
            HandshakeOutcome::Cancelled => {
                debug!("replication_leader[{} peer={}]: pusher cancelled", nid, pid);
                return;
            }
            HandshakeOutcome::Transient => {}
        }
        if !backoff_or_cancel(&cancel).await {
            debug!("replication_leader[{} peer={}]: pusher cancelled", nid, pid);
            return;
        }
    }
}

async fn open_session(
    consensus: &Arc<Consensus>,
    peer: &PeerConfig,
    anchor_override: Option<u64>,
    cancel: &CancellationToken,
) -> HandshakeOutcome {
    let nid = consensus.node_id();
    let pid = peer.peer_id;
    let mut client = match tokio::time::timeout(
        PEER_CONNECT_TIMEOUT,
        NodeClient::<Channel>::connect(peer.host.clone()),
    )
    .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            debug!(
                "replication_leader[{} peer={}]: connect failed: {}",
                nid, pid, e
            );
            return HandshakeOutcome::Transient;
        }
        Err(_) => {
            debug!(
                "replication_leader[{} peer={}]: connect timed out after {:?}",
                nid, pid, PEER_CONNECT_TIMEOUT
            );
            return HandshakeOutcome::Transient;
        }
    };

    let (out_tx, out_rx) = mpsc::channel::<ReplicationLeaderMessage>(64);
    let outbound = ReceiverStream::new(out_rx);
    let mut inbound = match client.replication(outbound).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            debug!(
                "replication_leader[{} peer={}]: open stream failed: {}",
                nid, pid, e
            );
            return HandshakeOutcome::Transient;
        }
    };

    let snap = consensus.replication_leader_handshake_snapshot(peer.peer_id, anchor_override);
    let snap_term = snap.term;
    let handshake = ReplicationLeaderMessage {
        message: Some(replication_leader_message::Message::Handshake(
            ReplicationLeaderMessageHandshake {
                leader_id: snap.self_id,
                leader_term: snap.term,
                leader_term_first_tx_id: snap.term_first_tx,
                prev_log_tx_id: snap.prev_log_tx_id,
                prev_log_term: snap.prev_log_term,
                leader_commit: snap.leader_commit,
            },
        )),
    };
    if out_tx.send(handshake).await.is_err() {
        debug!(
            "replication_leader[{} peer={}]: handshake send failed (channel closed)",
            nid, pid
        );
        return HandshakeOutcome::Transient;
    }

    tokio::select! {
        biased;
        _ = cancel.cancelled() => HandshakeOutcome::Cancelled,
        r = inbound.message() => match r {
            Ok(Some(ReplicationFollowerMessage {
                message: Some(replication_follower_message::Message::HandshakeResponse(h)),
            })) => {
                if !h.success {
                    debug!(
                        "replication_leader[{} peer={}]: handshake rejected reason={} last_term={} last_term_curr_tx_id={}",
                        nid, pid, h.reject_reason, h.last_term, h.last_term_curr_tx_id
                    );
                    HandshakeOutcome::Rejected { last_term_curr_tx_id: h.last_term_curr_tx_id }
                } else {
                    HandshakeOutcome::Active {
                        out_tx,
                        inbound,
                        snap_term,
                        last_term_curr_tx_id: h.last_term_curr_tx_id,
                    }
                }
            }
            Ok(Some(_)) => {
                debug!(
                    "replication_leader[{} peer={}]: handshake: unexpected non-handshake frame",
                    nid, pid
                );
                HandshakeOutcome::Transient
            }
            Ok(None) => {
                debug!(
                    "replication_leader[{} peer={}]: handshake: stream closed",
                    nid, pid
                );
                HandshakeOutcome::Transient
            }
            Err(e) => {
                debug!(
                    "replication_leader[{} peer={}]: handshake: recv error: {}",
                    nid, pid, e
                );
                HandshakeOutcome::Transient
            }
        }
    }
}

fn seed_peer_match_index(consensus: &Consensus, peer_id: u64, snap_term: u64, last_commit_id: u64) {
    let mut node = consensus.raft_node.lock().expect("raft mutex poisoned");
    if let Some(mut pr) = node.replication().peer(peer_id) {
        pr.append_result(
            Instant::now(),
            AppendResult::Success {
                term: snap_term,
                last_commit_id,
            },
        );
    }
    debug!(
        "replication_leader[{} peer={}]: match_index seeded term={} last_commit={}",
        consensus.node_id(),
        peer_id,
        snap_term,
        last_commit_id
    );
}

async fn run_peer_session(
    consensus: &Arc<Consensus>,
    peer: &PeerConfig,
    out_tx: mpsc::Sender<ReplicationLeaderMessage>,
    inbound: Streaming<ReplicationFollowerMessage>,
    start_tx_id: u64,
    cancel: &CancellationToken,
) {
    let nid = consensus.node_id();
    let pid = peer.peer_id;
    debug!(
        "replication_leader[{} peer={}]: session started start_tx_id={}",
        nid, pid, start_tx_id
    );
    let session_cancel = cancel.child_token();
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn({
        let c = consensus.clone();
        let p = peer.clone();
        let ct = session_cancel.clone();
        async move { run_peer_sender(c, p, out_tx, start_tx_id, ct).await }
    });
    tasks.spawn({
        let c = consensus.clone();
        let pid = peer.peer_id;
        let ct = session_cancel.clone();
        async move { run_peer_receiver(c, pid, inbound, ct).await }
    });
    let _ = tasks.join_next().await;
    session_cancel.cancel();
    while tasks.join_next().await.is_some() {}
    debug!("replication_leader[{} peer={}]: session ended", nid, pid);
}

async fn run_peer_sender(
    consensus: Arc<Consensus>,
    peer: PeerConfig,
    out_tx: mpsc::Sender<ReplicationLeaderMessage>,
    start_tx_id: u64,
    cancel: CancellationToken,
) {
    let nid = consensus.node_id();
    let pid = peer.peer_id;
    let cluster = consensus
        .config
        .cluster
        .as_ref()
        .expect("run_peer_sender requires a clustered config");
    // Heartbeat cadence comes from raft (election-timeout-derived), not config.
    let heartbeat = consensus.heartbeat_interval();
    let max_bytes = cluster.append_entries_max_bytes;
    // Tailer is pre-positioned at `start_tx_id + 1`; subsequent
    // `tail()` calls just stream forward from that cursor.
    let next_tx = start_tx_id + 1;
    let mut tailer = consensus.ledger.current().wal_tailer(next_tx);
    let mut buffer = vec![0u8; max_bytes];
    // Highest tx_id shipped so far, tracked across iterations from the bytes the
    // tailer actually streamed. The idle park waits for `commit` to reach
    // `last_sent_tx_id + 1`; a value re-read from the watch each pass would race
    // with concurrent advances and skip a transaction.
    let mut last_sent_tx_id = start_tx_id;
    debug!(
        "replication_leader[{} peer={}]: sender started next_tx={}",
        nid, pid, next_tx
    );
    loop {
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                debug!(
                    "replication_leader[{} peer={}]: sender exiting (cancelled)",
                    nid, pid
                );
                return;
            }
            r = push_one(&consensus, pid, &out_tx, &mut tailer, &mut buffer) => r,
        };
        match outcome {
            // Full buffer of WAL bytes — keep the loop tight so we
            // don't artificially cap leader→follower throughput. A
            // `yield_now` returns control to the runtime so other
            // peer tasks (and the receiver) get a fair turn.
            PushOutcome::Sent {
                drained: false,
                last_tx_id,
            } => {
                last_sent_tx_id = last_sent_tx_id.max(last_tx_id);
                yield_now().await;
                continue;
            }
            // Drained the WAL with a partial buffer — fall through to the park.
            PushOutcome::Sent {
                drained: true,
                last_tx_id,
            } => last_sent_tx_id = last_sent_tx_id.max(last_tx_id),
            // Heartbeat sent on an empty WAL — nothing new shipped.
            PushOutcome::Idle => {}
            PushOutcome::StreamClosed => {
                debug!(
                    "replication_leader[{} peer={}]: sender: stream closed",
                    nid, pid
                );
                return;
            }
        }
        // Drained the WAL (partial buffer) or sent a heartbeat on an empty WAL.
        // Park until `commit` reaches the tx after the last one we shipped
        // (reactive — new durable bytes to replicate). `heartbeat` is the only
        // periodic wake, firing an idle keepalive every raft heartbeat interval;
        // it re-arms each idle pass, so a data send resets it (Raft: a data
        // AppendEntries counts as the heartbeat).
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                debug!(
                    "replication_leader[{} peer={}]: sender exiting (cancelled)",
                    nid, pid
                );
                return;
            }
            _ = consensus.waiter().commit().wait_reach(last_sent_tx_id + 1) => {}
            _ = sleep(heartbeat) => {}
        }
    }
}

async fn run_peer_receiver(
    consensus: Arc<Consensus>,
    peer_id: u64,
    mut inbound: Streaming<ReplicationFollowerMessage>,
    cancel: CancellationToken,
) {
    let nid = consensus.node_id();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                debug!(
                    "replication_leader[{} peer={}]: receiver exiting (cancelled)",
                    nid, peer_id
                );
                return;
            }
            recv = inbound.message() => match recv {
                Ok(Some(ReplicationFollowerMessage {
                    message: Some(replication_follower_message::Message::IndexUpdate(u)),
                })) => {
                    debug!(
                        "replication_leader[{} peer={}]: index update local_commit={}",
                        nid, peer_id, u.local_commit_id
                    );
                    apply_peer_index_update(&consensus, peer_id, u.local_commit_id);
                    consensus.self_advance();
                }
                Ok(_) => {
                    debug!(
                        "replication_leader[{} peer={}]: receiver: stream closed",
                        nid, peer_id
                    );
                    return;
                }
                Err(e) => {
                    debug!(
                        "replication_leader[{} peer={}]: receiver recv error: {}",
                        nid, peer_id, e
                    );
                    return;
                }
            }
        }
    }
}

fn apply_peer_index_update(consensus: &Consensus, peer_id: u64, last_commit_id: u64) {
    let mut node = consensus.raft_node.lock().expect("raft mutex poisoned");
    let term = node.current_term();
    if let Some(mut pr) = node.replication().peer(peer_id) {
        pr.append_result(
            Instant::now(),
            AppendResult::Success {
                term,
                last_commit_id,
            },
        );
    }
    debug!(
        "replication_leader[{} peer={}]: peer match advanced last_commit={}",
        consensus.node_id(),
        peer_id,
        last_commit_id
    );
}

async fn push_one(
    c: &Arc<Consensus>,
    peer_id: u64,
    out_tx: &mpsc::Sender<ReplicationLeaderMessage>,
    tailer: &mut WalTailer,
    buffer: &mut [u8],
) -> PushOutcome {
    let nid = c.node_id();
    c.self_advance();
    let cluster_commit_id = c
        .raft_node
        .lock()
        .expect("raft mutex poisoned")
        .cluster_commit_index();

    let n = tailer.tail(buffer) as usize;
    if n == 0 {
        return match out_tx.send(heartbeat_msg(cluster_commit_id)).await {
            Ok(()) => {
                debug!(
                    "replication_leader[{} peer={}]: heartbeat sent cluster_commit={}",
                    nid, peer_id, cluster_commit_id
                );
                PushOutcome::Idle
            }
            Err(_) => PushOutcome::StreamClosed,
        };
    }
    debug!(
        "replication_leader[{} peer={}]: WalUpdate bytes={} cluster_commit={}",
        nid, peer_id, n, cluster_commit_id
    );
    // Resolve the highest tx_id shipped from the bytes themselves (zero-copy):
    // the tailer streams *written* WAL, which may run ahead of `commit`, so the
    // commit watch alone can't tell the sender where it is — the bytes can.
    let last_tx_id = last_tx_id_in(&buffer[..n]).unwrap_or(0);
    if out_tx
        .send(wal_update_msg(&buffer[..n], cluster_commit_id))
        .await
        .is_err()
    {
        return PushOutcome::StreamClosed;
    }
    // If the WAL filled the whole buffer there's almost certainly
    // more data behind it — keep the loop tight. If the WAL handed
    // back less than the buffer, we've drained what was available
    // and the caller should back off instead of spinning.
    PushOutcome::Sent {
        drained: n < buffer.len(),
        last_tx_id,
    }
}

fn heartbeat_msg(cluster_commit_id: u64) -> ReplicationLeaderMessage {
    ReplicationLeaderMessage {
        message: Some(replication_leader_message::Message::Heartbeat(
            ReplicationLeaderMessageHeartBeat { cluster_commit_id },
        )),
    }
}

fn wal_update_msg(wal_binary: &[u8], cluster_commit_id: u64) -> ReplicationLeaderMessage {
    ReplicationLeaderMessage {
        message: Some(replication_leader_message::Message::WalUpdate(
            ReplicationLeaderMessageWalUpdate {
                wal_binary: wal_binary.to_vec(),
                cluster_commit_id,
            },
        )),
    }
}

/// Returns `false` when the cancel fired during backoff.
async fn backoff_or_cancel(cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = sleep(RECONNECT_BACKOFF) => true,
    }
}
