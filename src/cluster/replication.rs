//! Leader-side replication: tail own WAL, fan out to followers via AppendEntries.

use crate::cluster::config::PeerConfig;
use crate::cluster::proto;
use crate::cluster::proto::node_client::NodeClient;
use crate::ledger::Ledger;
use spdlog::{info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};

const WAL_RECORD_SIZE: usize = 40;

pub struct Replication {
    ledger: Arc<Ledger>,
    peers: Vec<PeerConfig>,
    leader_id: u64,
    term: u64,
    max_bytes_per_rpc: usize,
    rpc_message_size_limit: usize,
    poll_interval: Duration,
    running: Arc<AtomicBool>,
}

impl Replication {
    pub fn new(
        ledger: Arc<Ledger>,
        peers: Vec<PeerConfig>,
        leader_id: u64,
        term: u64,
        max_bytes_per_rpc: usize,
        poll_interval: Duration,
    ) -> Self {
        let records_per_rpc = (max_bytes_per_rpc / WAL_RECORD_SIZE).max(1);
        let max_bytes = records_per_rpc * WAL_RECORD_SIZE;
        // Leave protobuf framing headroom (doubled + 4 KiB) so the client
        // doesn't trip tonic's default 4 MiB decode/encode limits.
        let rpc_limit = max_bytes * 2 + 4 * 1024;
        Self {
            ledger,
            peers,
            leader_id,
            term,
            max_bytes_per_rpc: max_bytes,
            rpc_message_size_limit: rpc_limit,
            poll_interval,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    /// Spawn one replication task per peer. Returns the join handles.
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::with_capacity(self.peers.len());
        for peer in self.peers {
            let h = tokio::spawn(run_peer(
                peer,
                self.ledger.clone(),
                self.leader_id,
                self.term,
                self.max_bytes_per_rpc,
                self.rpc_message_size_limit,
                self.poll_interval,
                self.running.clone(),
            ));
            handles.push(h);
        }
        handles
    }
}

async fn run_peer(
    peer: PeerConfig,
    ledger: Arc<Ledger>,
    leader_id: u64,
    term: u64,
    max_bytes: usize,
    rpc_limit: usize,
    poll: Duration,
    running: Arc<AtomicBool>,
) {
    info!(
        "replication: starting peer-task for node_id={} ({})",
        peer.id, peer.node_addr
    );

    // Lazy connect; retry forever while running.
    let mut client = loop {
        if !running.load(Ordering::Relaxed) {
            return;
        }
        match connect(&peer.node_addr, rpc_limit).await {
            Ok(c) => break c,
            Err(e) => {
                warn!("replication: connect to {} failed: {}", peer.node_addr, e);
                sleep(Duration::from_millis(200)).await;
            }
        }
    };

    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; max_bytes];

    // Moving window start: first tx_id we still need to ship to this peer.
    // Advances to `shipment_last_tx + 1` after every accepted AppendEntries.
    let mut from_tx_id: u64 = 1;
    // Last tx_id acknowledged by this peer.
    let mut peer_last_tx: u64 = 0;

    while running.load(Ordering::Relaxed) {
        let n = tailer.tail(from_tx_id, &mut buf) as usize;
        if n == 0 {
            sleep(poll).await;
            continue;
        }

        let shipment_last_tx = tx_id_of_last_record(&buf[..n])
            .expect("tailer returned non-empty bytes with no tx-bearing record");
        let bytes = buf[..n].to_vec();

        // Retry the same chunk until the peer accepts it; failures here are
        // transport-level under ADR-015's static-leader model.
        loop {
            if !running.load(Ordering::Relaxed) {
                return;
            }
            let leader_commit = ledger.last_commit_id();
            let req = proto::AppendEntriesRequest {
                leader_id,
                term,
                prev_tx_id: peer_last_tx,
                prev_term: term,
                from_tx_id,
                to_tx_id: shipment_last_tx,
                wal_bytes: bytes.clone(),
                leader_commit_tx_id: leader_commit,
            };

            match client.append_entries(req).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if r.success {
                        peer_last_tx = shipment_last_tx;
                        from_tx_id = shipment_last_tx + 1;
                        break;
                    }
                    warn!(
                        "replication: peer {} rejected append (reason={}, last_tx_id={})",
                        peer.id, r.reject_reason, r.last_tx_id
                    );
                    sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    warn!(
                        "replication: AppendEntries to peer {} failed (code={:?}): {}",
                        peer.id,
                        e.code(),
                        e.message()
                    );
                    sleep(Duration::from_millis(50)).await;
                    if let Ok(c) = connect(&peer.node_addr, rpc_limit).await {
                        client = c;
                    }
                }
            }
        }
    }

    info!("replication: peer-task for node_id={} stopped", peer.id);
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
