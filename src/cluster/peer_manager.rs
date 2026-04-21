//! Peer-management layer. One `PeerManager` supervises a fixed set of
//! `PeerReplication`s (ADR-015 is static membership). It owns the only
//! top-level `tokio::spawn` in the replication subsystem; each peer runs
//! inside it as a child sub-task.

use crate::cluster::config::PeerConfig;
use crate::cluster::peer_replication::{PeerReplication, ReplicationParams};
use crate::ledger::Ledger;
use spdlog::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;

/// Supervises one `PeerReplication` per configured peer.
pub struct PeerManager {
    ledger: Arc<Ledger>,
    peers: Vec<PeerConfig>,
    params: ReplicationParams,
    running: Arc<AtomicBool>,
}

impl PeerManager {
    pub fn new(ledger: Arc<Ledger>, peers: Vec<PeerConfig>, params: ReplicationParams) -> Self {
        Self {
            ledger,
            peers,
            params,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// External shutdown flag. Set to `false` (via `stop()`) to drain every
    /// peer task on the next poll.
    pub fn running(&self) -> Arc<AtomicBool> {
        self.running.clone()
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
        for peer in self.peers.into_iter() {
            let replicator = PeerReplication::new(
                peer,
                self.ledger.clone(),
                self.params.clone(),
                self.running.clone(),
            );
            child_handles.push(tokio::spawn(async move { replicator.run().await }));
        }

        for h in child_handles {
            let _ = h.await;
        }

        info!("peer-manager: all peer tasks drained");
    }
}
