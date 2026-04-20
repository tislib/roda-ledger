use crate::cluster::config::ClusterConfig;
use crate::cluster::proto::AppendEntriesRequest;
use crate::cluster::proto::node_client::NodeClient;
use crate::entities::{TxMetadata, WalEntryKind};
use crate::wal::PeerShipper;
use spdlog::{debug, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
struct Peer {
    node_id: u64,
    client: NodeClient<Channel>,
}

pub struct LeaderPeerShipper {
    leader_id: u64,
    term: u64,
    peers: Vec<Peer>,
    handle: Handle,
    semaphore: Arc<Semaphore>,
    last_shipped_tx_id: AtomicU64,
    last_committed_tx_id: Arc<AtomicU64>,
}

impl LeaderPeerShipper {
    pub fn new(
        cluster: &ClusterConfig,
        handle: Handle,
        last_committed_tx_id: Arc<AtomicU64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut peers = Vec::with_capacity(cluster.peers.len());
        for p in &cluster.peers {
            let uri = format!("http://{}", p.address);
            let endpoint = Endpoint::from_shared(uri.clone())
                .map_err(|e| format!("invalid peer uri {}: {}", uri, e))?;
            let channel = endpoint.connect_lazy();
            peers.push(Peer {
                node_id: p.node_id,
                client: NodeClient::new(channel),
            });
        }

        let semaphore = Arc::new(Semaphore::new(3 * peers.len().max(1)));

        Ok(Self {
            leader_id: cluster.node_id,
            term: cluster.current_term,
            peers,
            handle,
            semaphore,
            last_shipped_tx_id: AtomicU64::new(0),
            last_committed_tx_id,
        })
    }

    pub fn semaphore_capacity(&self) -> usize {
        3 * self.peers.len().max(1)
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }
}

impl PeerShipper for LeaderPeerShipper {
    fn ship(&self, buffer: &[u8]) {
        if self.peers.is_empty() {
            return;
        }

        let Some((from_tx_id, to_tx_id)) = scan_tx_id_range(buffer) else {
            return;
        };

        let prev_tx_id = self.last_shipped_tx_id.load(Ordering::Acquire);
        let prev_term = if prev_tx_id == 0 { 0 } else { self.term };
        let leader_commit_tx_id = self.last_committed_tx_id.load(Ordering::Acquire);

        let payload: Arc<[u8]> = Arc::from(buffer);

        for peer in &self.peers {
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(
                        "leader peer shipper: semaphore saturated ({} permits) — \
                         dropping ship to peer {} for tx_ids {}..={}",
                        self.semaphore_capacity(),
                        peer.node_id,
                        from_tx_id,
                        to_tx_id
                    );
                    continue;
                }
            };

            let mut client = peer.client.clone();
            let peer_id = peer.node_id;
            let request = AppendEntriesRequest {
                leader_id: self.leader_id,
                term: self.term,
                prev_tx_id,
                prev_term,
                from_tx_id,
                to_tx_id,
                wal_bytes: payload.to_vec(),
                leader_commit_tx_id,
            };

            self.handle.spawn(async move {
                let _permit = permit;
                match client.append_entries(request).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        if resp.success {
                            debug!(
                                "peer {} acked append_entries up to tx_id={}",
                                peer_id, resp.last_tx_id
                            );
                        } else {
                            warn!(
                                "peer {} rejected append_entries: reject_reason={}, \
                                 follower last_tx_id={}",
                                peer_id, resp.reject_reason, resp.last_tx_id
                            );
                        }
                    }
                    Err(status) => {
                        warn!(
                            "peer {} append_entries rpc error: code={:?}, msg={}",
                            peer_id,
                            status.code(),
                            status.message()
                        );
                    }
                }
            });
        }

        self.last_shipped_tx_id.store(to_tx_id, Ordering::Release);
    }
}

fn scan_tx_id_range(buffer: &[u8]) -> Option<(u64, u64)> {
    if buffer.is_empty() || !buffer.len().is_multiple_of(40) {
        return None;
    }
    let mut from: u64 = 0;
    let mut to: u64 = 0;
    let mut off = 0usize;
    while off + 40 <= buffer.len() {
        if buffer[off] == WalEntryKind::TxMetadata as u8 {
            let m: TxMetadata = bytemuck::pod_read_unaligned(&buffer[off..off + 40]);
            if from == 0 {
                from = m.tx_id;
            }
            to = m.tx_id;
        }
        off += 40;
    }
    if from == 0 { None } else { Some((from, to)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{EntryKind, FailReason, TxEntry};

    fn build_tx_bytes(tx_id: u64) -> Vec<u8> {
        let entry = TxEntry {
            entry_type: WalEntryKind::TxEntry as u8,
            kind: EntryKind::Credit,
            _pad0: [0; 6],
            tx_id,
            account_id: 1,
            amount: 100,
            computed_balance: 100,
        };
        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 1,
            link_count: 0,
            fail_reason: FailReason::NONE,
            crc32c: 0,
            tx_id,
            timestamp: 0,
            user_ref: 0,
            tag: [0; 8],
        };
        let mut d = crc32c::crc32c(bytemuck::bytes_of(&meta));
        d = crc32c::crc32c_append(d, bytemuck::bytes_of(&entry));
        meta.crc32c = d;
        let mut out = Vec::with_capacity(80);
        out.extend_from_slice(bytemuck::bytes_of(&meta));
        out.extend_from_slice(bytemuck::bytes_of(&entry));
        out
    }

    #[test]
    fn scan_empty_buffer() {
        assert_eq!(scan_tx_id_range(&[]), None);
    }

    #[test]
    fn scan_misaligned_buffer() {
        assert_eq!(scan_tx_id_range(&[0u8; 39]), None);
    }

    #[test]
    fn scan_single_tx_yields_same_from_to() {
        let bytes = build_tx_bytes(42);
        assert_eq!(scan_tx_id_range(&bytes), Some((42, 42)));
    }

    #[test]
    fn scan_multi_tx_yields_first_and_last() {
        let mut bytes = build_tx_bytes(10);
        bytes.extend(build_tx_bytes(11));
        bytes.extend(build_tx_bytes(12));
        assert_eq!(scan_tx_id_range(&bytes), Some((10, 12)));
    }

    #[test]
    fn scan_structural_only_returns_none() {
        let mut bytes = vec![0u8; 40];
        bytes[0] = WalEntryKind::SegmentHeader as u8;
        assert_eq!(scan_tx_id_range(&bytes), None);
    }
}
