// /// Replication loop — runs as its own `tokio::spawn`'d task,
// /// alongside the command loop. Owns the per-peer
// /// [`PeerReplicator`] map (one [`WalTailer`] per peer, since cursors
// /// are stateful and each peer's `next_index` lives at a different
// /// point in the WAL).
// ///
// /// On every tick (`replication_tick_interval`, taken from
// /// `config.cluster.replication_poll_ms`) the loop:
// ///
// /// 1. Locks the shared [`RaftNode`] briefly to enumerate peers.
// /// 2. For each peer, locks again briefly to call
// ///    [`Replication::peer(p).get_append_range(now)`]
// ///    (`raft::PeerReplication::get_append_range`). The gate inside
// ///    raft returns `None` for peers with no pending update
// ///    (heartbeat deadline not elapsed, RPC in-flight, or this node
// ///    isn't the leader) — those peers are skipped.
// /// 3. For peers that do have an update, the loop reads the WAL byte
// ///    range from that peer's [`WalTailer`], builds the proto
// ///    request, and spawns an outbound gRPC task.
// /// 4. The task's reply (or timeout) feeds back through the command
// ///    channel as [`Command::AppendEntriesOutbound`], which the
// ///    command loop routes into
// ///    `Replication::peer(p).append_result(...)`.
// 
// use tokio::sync::oneshot;
// use raft::TxId;
// use storage::WalTailer;
// 
// /// Reply parked until follower-side WAL durability covers `tx_id`.
// /// The cluster owns the gRPC `oneshot` while the ledger pipeline is
// /// fsyncing the entries that arrived on this AppendEntries; the main
// /// loop drains the parked reply once the ledger's commit watermark
// /// (which collapses with the WAL durability watermark in the current
// /// design — see ADR-0017) covers the parked `last_tx_id`.
// struct ParkedReply {
//     /// The oneshot the gRPC handler is awaiting.
//     reply: oneshot::Sender<proto::AppendEntriesResponse>,
//     /// Term to stamp into the success response.
//     term: u64,
//     /// `last_tx_id` to report — equals the map key.
//     last_tx_id: TxId,
// }
// 
// /// Per-peer leader-side replication state.
// ///
// /// Each peer gets its own `WalTailer` because the tailer cursor is
// /// stateful per-stream — peers replicate at different `next_index`
// /// values, so a single shared tailer would thrash.
// struct PeerReplicator {
//     /// gRPC URL of the peer's `Node` service. `tonic::Channel`-style
//     /// scheme + host + port (e.g. `http://10.0.0.2:50061`).
//     host: String,
//     /// Bookmarked stream over the leader's WAL. Reads bytes for the
//     /// `[from_tx_id..]` range on each replication tick that pulls a
//     /// non-empty `AppendEntriesRequest`.
//     tailer: WalTailer,
// }
// ///
// /// Lock contention with the command loop is minimal — raft calls
// /// are sync and brief; tailer reads happen outside the lock.
// async fn replication_loop(
//     node: Arc<SharedNode>,
//     cmd_tx: mpsc::Sender<Command>,
//     mut peer_replicators: BTreeMap<NodeId, PeerReplicator>,
//     self_id: NodeId,
//     tick_interval: Duration,
//     rpc_timeout: Duration,
//     append_entries_max_bytes: usize,
// ) {
//     info!(
//         "replication_loop: started (self_id={}, peers={}, tick={:?})",
//         self_id,
//         peer_replicators.len(),
//         tick_interval
//     );
//     let mut tick = tokio::time::interval(tick_interval);
//     tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
// 
//     loop {
//         info!("replication_loop: tick wait");
//         tick.tick().await;
//         info!("replication_loop: tick");
//         let now = Instant::now();
// 
//         // Phase 1: enumerate peers (briefly locks the node).
//         let peers: Vec<NodeId> = {
//             let mut n = node.lock().await;
//             if !n.role().is_leader() {
//                 // Not currently a leader — nothing to replicate.
//                 // Keep ticking; the next role transition surfaces
//                 // through `is_leader()` without any explicit
//                 // notification.
//                 info!("replication_loop: not leader; skipping");
//                 return;
//             }
//             info!("replication_loop: leader; replicating to {} peers", n.replication().peers().len());
//             n.replication().peers()
//         };
//         info!("replication_loop: peers: {:?}", peers);
// 
//         // Phase 2: for each peer, pull the next request and ship it.
//         for peer_id in peers {
//             // Pull the request (briefly locks the node). `None` is
//             // the user-visible "skip nodes with no update" path —
//             // the gate lives inside raft's `get_append_range`.
//             let request = {
//                 let mut n = node.lock().await;
//                 match n.replication().peer(peer_id) {
//                     Some(mut p) => p.get_append_range(now),
//                     None => continue,
//                 }
//             };
//             let Some(request) = request else { continue };
// 
//             // Read the WAL byte range. Heartbeats
//             // (`entries.is_empty()`) ship no bytes.
//             let mut wal_bytes: Vec<u8> = Vec::new();
//             let to_tx_id = if request.entries.is_empty() {
//                 // Empty range: from > to is the proto convention.
//                 request.entries.start_tx_id.saturating_sub(1)
//             } else {
//                 let from_tx_id = request.entries.start_tx_id;
//                 let last_tx_id = request
//                     .entries
//                     .last_tx_id()
//                     .expect("non-empty range has a last_tx_id");
//                 let mut buffer = vec![0u8; append_entries_max_bytes];
//                 let written = match peer_replicators.get_mut(&peer_id) {
//                     Some(r) => r.tailer.tail(from_tx_id, &mut buffer) as usize,
//                     None => {
//                         spdlog::warn!(
//                             "replication_loop: peer {} has no replicator entry; skipping",
//                             peer_id
//                         );
//                         continue;
//                     }
//                 };
//                 buffer.truncate(written);
//                 wal_bytes = buffer;
//                 last_tx_id
//             };
// 
//             let proto_req = proto::AppendEntriesRequest {
//                 leader_id: self_id,
//                 term: request.term,
//                 prev_tx_id: request.prev_log_tx_id,
//                 prev_term: request.prev_log_term,
//                 from_tx_id: request.entries.start_tx_id,
//                 to_tx_id,
//                 wal_bytes,
//                 leader_commit_tx_id: request.leader_commit,
//             };
// 
//             // Spawn the outbound RPC. Posts the result back through
//             // the command channel so raft state mutations happen on
//             // the command loop side (single-writer over `RaftNode`'s
//             // mutating APIs from the command-loop task; the
//             // replication loop only ever holds the lock for the
//             // brief read-and-arm via `get_append_range`).
//             let host = match peer_replicators.get(&peer_id) {
//                 Some(r) => r.host.clone(),
//                 None => continue,
//             };
//             let cmd_tx = cmd_tx.clone();
//             tokio::spawn(async move {
//                 let result = send_append_entries_rpc(host, proto_req, rpc_timeout).await;
//                 let _ = cmd_tx
//                     .send(Command::AppendEntriesOutbound { peer_id, result })
//                     .await;
//             });
//         }
//     }
// }

// use std::time::Duration;
// use proto::node::node_client::NodeClient;
// use raft::AppendResult;
// 
// /// Outbound `AppendEntries` over gRPC. Connects to `host`, sends the
// /// proto request with `timeout`, and translates the response into the
// /// raft `AppendResult` shape. Connection errors and tonic timeouts
// /// both map to [`AppendResult::Timeout`] — the leader treats them as
// /// "no information; clear in-flight, leave indexes alone".
// async fn send_append_entries_rpc(
//     host: String,
//     req: proto::AppendEntriesRequest,
//     timeout: Duration,
// ) -> AppendResult {
//     let mut client = match tokio::time::timeout(timeout, NodeClient::connect(host)).await {
//         Ok(Ok(c)) => c,
//         Ok(Err(_)) | Err(_) => return AppendResult::Timeout,
//     };
//     let resp = match tokio::time::timeout(timeout, client.append_entries(req)).await {
//         Ok(Ok(r)) => r.into_inner(),
//         Ok(Err(_)) | Err(_) => return AppendResult::Timeout,
//     };
//     if resp.success {
//         AppendResult::Success {
//             term: resp.term,
//             last_write_id: resp.last_write_id,
//             last_commit_id: resp.last_commit_id,
//         }
//     } else {
//         AppendResult::Reject {
//             term: resp.term,
//             reason: crate::raft_loop::proto_reject_to_raft(resp.reject_reason),
//             last_write_id: resp.last_write_id,
//             last_commit_id: resp.last_commit_id,
//         }
//     }
// }

use std::sync::Arc;
use crate::LedgerSlot;

pub struct ReplicationLoop {
    ledger: Arc<LedgerSlot>,
}

impl ReplicationLoop {
    pub(crate) fn new(ledger: Arc<LedgerSlot>) -> Self {
        Self { ledger }
    }
    pub(super) async fn run_replication_loop(&mut self) {

    }
}