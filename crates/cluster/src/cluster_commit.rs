//! `ClusterCommitIndex` — role-agnostic watermark for the highest tx_id
//! known to be quorum-committed across the cluster.
//!
//! Every `Server` holds one (the `Server` is cluster-mode-only). Write
//! paths differ by who owns the server, but all are monotonic `fetch_max`
//! internally:
//!
//! - Leader: `Quorum::advance` mirrors the recomputed majority via
//!   [`set_from_quorum`]. With zero peers, this equals the leader's own
//!   commit progress (Quorum slot 0 is fed by `Ledger::on_commit`).
//! - Follower: [`NodeHandler::append_entries`](super::node_handler::NodeHandler)
//!   clamps `req.leader_commit_tx_id` to its own `last_commit_id` and
//!   publishes via [`set_from_leader`].
//! - Bare `Server` without any cluster wrapper (test harnesses): construct
//!   via [`ClusterCommitIndex::from_ledger`], which seeds from the
//!   ledger's current `last_commit_id` and tracks subsequent commits via
//!   `Ledger::on_commit`.
//!
//! Readers ([`LedgerHandler::wait_for_transaction_level`](super::ledger_handler::LedgerHandler),
//! `get_pipeline_index`) call [`get`].

use ledger::ledger::Ledger;
use spdlog::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ClusterCommitIndex(AtomicU64);

impl ClusterCommitIndex {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(0)))
    }

    /// Build a watermark backed by the ledger's commit stream. Seeds
    /// with `ledger.last_commit_id()` and registers an `on_commit` hook
    /// that advances the watermark on every subsequent commit.
    ///
    /// Use when there is no surrounding cluster (single-node Server
    /// harnesses in tests). On the `Leader` path the `Quorum` already
    /// drives the watermark — do NOT also call this, the ledger's single
    /// `on_commit` slot would conflict.
    pub fn from_ledger(ledger: &Arc<Ledger>) -> Arc<Self> {
        let cci = Self::new();
        cci.set_from_quorum(ledger.last_commit_id());
        let handle = cci.clone();
        // If another subsystem has already claimed the ledger's commit
        // hook (e.g. a Leader was set up against the same ledger), we
        // simply keep the seed value; the user should prefer the cluster
        // bring-up path in that case.
        let _ = ledger.on_commit(Arc::new(move |tx_id| handle.set_from_quorum(tx_id)));
        cci
    }

    /// Read the current quorum-committed watermark.
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Leader-side write: advance to the newly recomputed majority.
    /// Called from `Quorum::advance` after `majority_index.fetch_max`.
    #[inline]
    pub(crate) fn set_from_quorum(&self, majority: u64) {
        let prev = self.0.fetch_max(majority, Ordering::Release);
        if majority > prev {
            debug!(
                "cluster_commit_index: advanced via quorum {} -> {}",
                prev, majority
            );
        }
    }

    /// Follower-side write: advance to the leader-advertised watermark,
    /// already clamped by the caller to the follower's `last_commit_id`.
    #[inline]
    pub(crate) fn set_from_leader(&self, clamped: u64) {
        let prev = self.0.fetch_max(clamped, Ordering::Release);
        if clamped > prev {
            debug!(
                "cluster_commit_index: advanced via leader heartbeat {} -> {}",
                prev, clamped
            );
        }
    }
}
