//! Per-peer replication: owns one `WalTailer` cursor, one gRPC client, and
//! the shipping window for a single follower. Its `run()` is the long-lived
//! async task that `PeerManager` spawns.
//!
//! The key state is `(from_tx_id, peer_last_tx)`:
//! - `from_tx_id` is the next tx we still need to ship.
//! - `peer_last_tx` is the high-water mark the follower has acked (stamped
//!   onto every AppendEntries as `prev_tx_id` for Raft-style ordering).

use crate::cluster::config::PeerConfig;
use crate::cluster::proto;
use crate::cluster::proto::node_client::NodeClient;
use crate::ledger::Ledger;
use crate::storage::WalTailer;
use spdlog::{info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};

pub(super) const WAL_RECORD_SIZE: usize = 40;

/// Static replication parameters shared across all peer tasks.
#[derive(Clone, Debug)]
pub struct ReplicationParams {
    pub leader_id: u64,
    pub term: u64,
    pub max_bytes_per_rpc: usize,
    pub rpc_message_size_limit: usize,
    pub poll_interval: Duration,
}

impl ReplicationParams {
    pub fn new(
        leader_id: u64,
        term: u64,
        max_bytes_per_rpc: usize,
        poll_interval: Duration,
    ) -> Self {
        let records_per_rpc = (max_bytes_per_rpc / WAL_RECORD_SIZE).max(1);
        let max_bytes = records_per_rpc * WAL_RECORD_SIZE;
        // Leave protobuf framing headroom (doubled + 4 KiB) so the client
        // doesn't trip tonic's default 4 MiB decode/encode limit.
        let rpc_message_size_limit = max_bytes * 2 + 4 * 1024;
        Self {
            leader_id,
            term,
            max_bytes_per_rpc: max_bytes,
            rpc_message_size_limit,
            poll_interval,
        }
    }
}

/// Per-peer replicator. One per follower.
pub struct PeerReplication {
    peer: PeerConfig,
    ledger: Arc<Ledger>,
    params: ReplicationParams,
    running: Arc<AtomicBool>,
    tailer: WalTailer,
    from_tx_id: u64,
    peer_last_tx: u64,
    /// This peer's last acked `commit_id` (the `last_tx_id` returned by
    /// the follower on its most recent successful `AppendEntries`).
    /// Shared with `PeerManager` for majority tracking.
    last_commit_id: Arc<AtomicU64>,
    /// All peers' `last_commit_id` atomics (including this one). Read by
    /// every task to recompute the majority after its own ack.
    peer_commit_ids: Arc<Vec<Arc<AtomicU64>>>,
    /// Cluster-wide majority commit watermark owned by `PeerManager`.
    /// Written via `fetch_max` so the value is monotonically non-decreasing.
    majority_commit_id: Arc<AtomicU64>,
}

impl PeerReplication {
    pub fn new(
        peer: PeerConfig,
        ledger: Arc<Ledger>,
        params: ReplicationParams,
        running: Arc<AtomicBool>,
        last_commit_id: Arc<AtomicU64>,
        peer_commit_ids: Arc<Vec<Arc<AtomicU64>>>,
        majority_commit_id: Arc<AtomicU64>,
    ) -> Self {
        let tailer = ledger.wal_tailer();
        Self {
            peer,
            ledger,
            params,
            running,
            tailer,
            from_tx_id: 1,
            peer_last_tx: 0,
            last_commit_id,
            peer_commit_ids,
            majority_commit_id,
        }
    }

    /// Long-lived async driver. Runs until `running` is cleared or an
    /// unrecoverable error occurs; otherwise retries transport + reject
    /// failures in place.
    pub async fn run(mut self) {
        info!(
            "replication: starting peer-task for node_id={} ({})",
            self.peer.id, self.peer.node_addr
        );

        let mut client = loop {
            if !self.running.load(Ordering::Relaxed) {
                return;
            }
            match connect(&self.peer.node_addr, self.params.rpc_message_size_limit).await {
                Ok(c) => break c,
                Err(e) => {
                    warn!(
                        "replication: connect to {} failed: {}",
                        self.peer.node_addr, e
                    );
                    sleep(Duration::from_millis(200)).await;
                }
            }
        };

        let mut buf = vec![0u8; self.params.max_bytes_per_rpc];

        while self.running.load(Ordering::Relaxed) {
            let n = self.tailer.tail(self.from_tx_id, &mut buf) as usize;
            if n == 0 {
                sleep(self.params.poll_interval).await;
                continue;
            }

            let shipment_last_tx = tx_id_of_last_record(&buf[..n])
                .expect("tailer returned non-empty bytes with no tx-bearing record");
            let bytes = buf[..n].to_vec();

            if !self
                .ship_until_accepted(&mut client, bytes, shipment_last_tx)
                .await
            {
                return;
            }
        }

        info!("replication: peer-task for node_id={} stopped", self.peer.id);
    }

    /// Retry `AppendEntries` for the given chunk until the peer accepts it
    /// or `running` is cleared. Returns `true` on success, `false` on shutdown.
    async fn ship_until_accepted(
        &mut self,
        client: &mut NodeClient<Channel>,
        bytes: Vec<u8>,
        shipment_last_tx: u64,
    ) -> bool {
        loop {
            if !self.running.load(Ordering::Relaxed) {
                return false;
            }
            let req = proto::AppendEntriesRequest {
                leader_id: self.params.leader_id,
                term: self.params.term,
                prev_tx_id: self.peer_last_tx,
                prev_term: self.params.term,
                from_tx_id: self.from_tx_id,
                to_tx_id: shipment_last_tx,
                wal_bytes: bytes.clone(),
                leader_commit_tx_id: self.ledger.last_commit_id(),
            };

            match client.append_entries(req).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if r.success {
                        self.peer_last_tx = shipment_last_tx;
                        self.from_tx_id = shipment_last_tx + 1;
                        // Publish this peer's latest commit watermark and
                        // recompute the cluster-wide majority. `fetch_max`
                        // keeps `majority_commit_id` monotonic across the
                        // concurrent updates from sibling peer tasks.
                        self.last_commit_id
                            .store(r.last_tx_id, Ordering::Release);
                        let m = majority_of(&self.peer_commit_ids);
                        self.majority_commit_id.fetch_max(m, Ordering::AcqRel);
                        return true;
                    }
                    warn!(
                        "replication: peer {} rejected append (reason={}, last_tx_id={})",
                        self.peer.id, r.reject_reason, r.last_tx_id
                    );
                    sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    warn!(
                        "replication: AppendEntries to peer {} failed (code={:?}): {}",
                        self.peer.id,
                        e.code(),
                        e.message()
                    );
                    sleep(Duration::from_millis(50)).await;
                    if let Ok(c) = connect(
                        &self.peer.node_addr,
                        self.params.rpc_message_size_limit,
                    )
                    .await
                    {
                        *client = c;
                    }
                }
            }
        }
    }
}

/// Compute the cluster's majority commit watermark from per-peer atomics.
///
/// With `F` followers + 1 leader (total `N = F + 1`), a Raft-style majority
/// requires `floor(N/2) + 1` nodes. The leader contributes 1 implicit ack,
/// so we need `floor((F+1)/2)` follower acks. The (F-`required`)-th element
/// of the ascending-sorted peer watermarks is the largest tx_id that at
/// least `required` followers have reached.
pub(crate) fn majority_of(peer_commit_ids: &[Arc<AtomicU64>]) -> u64 {
    let n = peer_commit_ids.len();
    if n == 0 {
        return 0;
    }
    let mut vals: Vec<u64> = peer_commit_ids
        .iter()
        .map(|a| a.load(Ordering::Acquire))
        .collect();
    vals.sort_unstable();
    let required = (n + 1) / 2;
    vals[n - required]
}

async fn connect(
    addr: &str,
    max_message_bytes: usize,
) -> Result<NodeClient<Channel>, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(addr.to_string())?;
    let channel = endpoint.connect().await?;
    Ok(NodeClient::new(channel)
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes))
}

/// tx_id field at offset 8 of the last 40-byte record in `bytes`.
/// Returns `None` when `bytes` is empty or not record-aligned.
fn tx_id_of_last_record(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(WAL_RECORD_SIZE) {
        return None;
    }
    let off = bytes.len() - WAL_RECORD_SIZE + 8;
    Some(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?))
}
