//! Per-peer replication: owns one `WalTailer` cursor, one gRPC client, and
//! the shipping window for a single follower. Its `run()` is the long-lived
//! async task that `Leader` spawns (one per peer).
//!
//! The key state is `(from_tx_id, peer_last_tx)`:
//! - `from_tx_id` is the next tx we still need to ship.
//! - `peer_last_tx` is the high-water mark the follower has acked (stamped
//!   onto every AppendEntries as `prev_tx_id` for Raft-style ordering).

use crate::cluster::Quorum;
use crate::cluster::config::PeerConfig;
use crate::cluster::proto::node as proto;
use crate::cluster::proto::node::node_client::NodeClient;
use crate::ledger::Ledger;
use crate::storage::WalTailer;
use spdlog::{info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Positional id (0-based) used as the `Quorum` slot.
    peer_index: u32,
    params: ReplicationParams,
    running: Arc<AtomicBool>,
    tailer: WalTailer,
    from_tx_id: u64,
    peer_last_tx: u64,
    /// Shared majority-commit tracker owned by `Leader`. The
    /// `leader_commit_tx_id` stamped on every outgoing AppendEntries
    /// reads from here via `majority.get()`.
    majority: Arc<Quorum>,
}

impl PeerReplication {
    pub fn new(
        peer: PeerConfig,
        peer_index: u32,
        ledger: Arc<Ledger>,
        params: ReplicationParams,
        running: Arc<AtomicBool>,
        majority: Arc<Quorum>,
    ) -> Self {
        let tailer = ledger.wal_tailer();
        Self {
            peer,
            peer_index,
            params,
            running,
            tailer,
            from_tx_id: 1,
            peer_last_tx: 0,
            majority,
        }
    }

    /// Long-lived async driver. Runs until `running` is cleared or an
    /// unrecoverable error occurs; otherwise retries transport + reject
    /// failures in place.
    pub async fn run(mut self) {
        info!(
            "replication: starting peer-task for node_id={} ({})",
            self.peer.peer_id, self.peer.host
        );

        let mut client = loop {
            if !self.running.load(Ordering::Relaxed) {
                return;
            }
            match connect(&self.peer.host, self.params.rpc_message_size_limit).await {
                Ok(c) => break c,
                Err(e) => {
                    warn!(
                        "replication: connect to {} failed: {}",
                        self.peer.host, e
                    );
                    sleep(Duration::from_millis(200)).await;
                }
            }
        };

        let mut buf = vec![0u8; self.params.max_bytes_per_rpc];

        while self.running.load(Ordering::Relaxed) {
            let n = self.tailer.tail(self.from_tx_id, &mut buf) as usize;
            if n == 0 {
                // Idle heartbeat: send an empty `AppendEntries` so the
                // follower's fresh `last_commit_id` flows back into our
                // `Quorum` even when the writer has stopped producing.
                // Without this, the majority watermark would freeze at
                // the value the follower returned on the previous
                // non-empty RPC — which is always one batch stale, since
                // `append_wal_entries` queues-then-returns on the follower.
                self.send_heartbeat(&mut client).await;
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

        info!(
            "replication: peer-task for node_id={} stopped",
            self.peer.peer_id
        );
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
                leader_commit_tx_id: self.majority.get(),
            };

            match client.append_entries(req).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if r.success {
                        self.peer_last_tx = shipment_last_tx;
                        self.from_tx_id = shipment_last_tx + 1;
                        // Publish this peer's latest commit watermark and
                        // let the shared tracker recompute the cluster
                        // majority. `advance` is monotonically safe via
                        // `fetch_max` inside `Quorum`.
                        self.majority.advance(self.peer_index, r.last_tx_id);
                        return true;
                    }
                    warn!(
                        "replication: peer {} rejected append (reason={}, last_tx_id={})",
                        self.peer.peer_id, r.reject_reason, r.last_tx_id
                    );
                    sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    warn!(
                        "replication: AppendEntries to peer {} failed (code={:?}): {}",
                        self.peer.peer_id,
                        e.code(),
                        e.message()
                    );
                    sleep(Duration::from_millis(50)).await;
                    if let Ok(c) =
                        connect(&self.peer.host, self.params.rpc_message_size_limit).await
                    {
                        *client = c;
                    }
                }
            }
        }
    }

    /// Send a zero-byte `AppendEntries` to refresh the peer's
    /// `last_commit_id` in our `Quorum`. Failures are swallowed — the
    /// next heartbeat (or real shipment) will retry; the purpose here is
    /// purely observational.
    async fn send_heartbeat(&mut self, client: &mut NodeClient<Channel>) {
        let req = proto::AppendEntriesRequest {
            leader_id: self.params.leader_id,
            term: self.params.term,
            prev_tx_id: self.peer_last_tx,
            prev_term: self.params.term,
            from_tx_id: self.from_tx_id,
            to_tx_id: self.peer_last_tx,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: self.majority.get(),
        };
        match client.append_entries(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                if r.success {
                    self.majority.advance(self.peer_index, r.last_tx_id);
                }
            }
            Err(_) => {
                // Transport hiccup on an idle heartbeat is not worth
                // acting on — the next cycle will reconnect via the
                // normal `ship_until_accepted` path.
            }
        }
    }
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
