//! Peer-management layer. One `PeerManager` supervises a fixed set of
//! `PeerReplication`s (ADR-015 is static membership). It owns the only
//! top-level `tokio::spawn` in the replication subsystem; each peer runs
//! inside it as a child sub-task.
//!
//! The manager also owns the cluster-wide **majority commit watermark**:
//! one `AtomicU64` per peer (the follower's last acked `last_tx_id`) plus
//! one aggregate `AtomicU64` that every peer-task recomputes after each
//! successful `AppendEntries` (see `majority_of` in `peer_replication`).

use crate::cluster::config::PeerConfig;
use crate::cluster::peer_replication::{PeerReplication, ReplicationParams};
use crate::ledger::Ledger;
use spdlog::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::task::JoinHandle;

/// Supervises one `PeerReplication` per configured peer.
pub struct PeerManager {
    ledger: Arc<Ledger>,
    peers: Vec<PeerConfig>,
    params: ReplicationParams,
    running: Arc<AtomicBool>,
    /// One `AtomicU64` per peer, positionally aligned with `peers`.
    /// Updated by each peer-task on every accepted `AppendEntries`.
    peer_commit_ids: Arc<Vec<Arc<AtomicU64>>>,
    /// Cluster-wide majority commit watermark. Written via `fetch_max` by
    /// peer tasks after they update their own atomic.
    majority_commit_id: Arc<AtomicU64>,
}

impl PeerManager {
    pub fn new(ledger: Arc<Ledger>, peers: Vec<PeerConfig>, params: ReplicationParams) -> Self {
        let peer_commit_ids: Vec<Arc<AtomicU64>> = peers
            .iter()
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        Self {
            ledger,
            peers,
            params,
            running: Arc::new(AtomicBool::new(true)),
            peer_commit_ids: Arc::new(peer_commit_ids),
            majority_commit_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// External shutdown flag. Set to `false` (via `stop()`) to drain every
    /// peer task on the next poll.
    pub fn running(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    /// Shared handle to the cluster-wide majority commit watermark. The
    /// value is monotonically non-decreasing and reflects the largest
    /// `last_tx_id` that a Raft-style majority of nodes has acked.
    pub fn majority_commit_id(&self) -> Arc<AtomicU64> {
        self.majority_commit_id.clone()
    }

    /// Per-peer commit atomics, positionally aligned with `peers()`. Callers
    /// get read access to individual follower watermarks; writes happen
    /// inside the peer tasks.
    pub fn peer_commit_ids(&self) -> Arc<Vec<Arc<AtomicU64>>> {
        self.peer_commit_ids.clone()
    }

    pub fn peers(&self) -> &[PeerConfig] {
        &self.peers
    }

    /// Stop every peer task started by this manager.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Spawn the manager's supervisor task. Each peer is driven by its own
    /// child sub-task launched inside the supervisor, so only one
    /// `tokio::spawn` is visible at the call site. The returned handle
    /// resolves when every peer sub-task has finished.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    /// Supervisor body: spawn one child task per peer, await them all.
    async fn run(self) {
        if self.peers.is_empty() {
            info!("peer-manager: no peers configured; supervisor exiting");
            return;
        }

        info!(
            "peer-manager: supervising {} peer(s) (leader_id={})",
            self.peers.len(),
            self.params.leader_id
        );

        let mut child_handles: Vec<JoinHandle<()>> = Vec::with_capacity(self.peers.len());
        for (idx, peer) in self.peers.into_iter().enumerate() {
            let replicator = PeerReplication::new(
                peer,
                self.ledger.clone(),
                self.params.clone(),
                self.running.clone(),
                self.peer_commit_ids[idx].clone(),
                self.peer_commit_ids.clone(),
                self.majority_commit_id.clone(),
            );
            child_handles.push(tokio::spawn(async move { replicator.run().await }));
        }

        for h in child_handles {
            let _ = h.await;
        }

        info!("peer-manager: all peer tasks drained");
    }
}
