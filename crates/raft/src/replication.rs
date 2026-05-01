//! Leader-side replication driver, exposed to the cluster via
//! [`RaftNode::replication`](crate::node::RaftNode::replication).
//!
//! ADR-0017 §"Driver call pattern": the cluster — not raft — owns
//! the per-peer loop that sends `AppendEntries` RPCs. Raft owns the
//! decision *what* to send (the next range, the §5.3 prev-log
//! header, the leader-commit watermark) and *how to react* to the
//! reply (advance / regress per-peer indexes, run the quorum,
//! step down on a higher term).
//!
//! Cluster usage pattern (one task per peer):
//!
//! ```ignore
//! loop {
//!     // 1. Pull the next request — None means "no longer leader",
//!     //    exit the loop.
//!     let req = match node.replication().peer(peer_id) {
//!         Some(mut p) => p.get_append_range(now),
//!         None => break,
//!     };
//!     let Some(req) = req else { break };
//!
//!     // 2. Send the RPC. Cluster's responsibility — the cluster
//!     //    chooses its own retry / sleep cadence.
//!     let result = send_grpc(req).await;
//!
//!     // 3. Feed the outcome back. Updates `next_index` /
//!     //    `match_index`, may advance `cluster_commit_index`,
//!     //    handles step-down on `TermBehind`.
//!     node.replication()
//!         .peer(peer_id)
//!         .unwrap()
//!         .append_result(now, result);
//! }
//! ```
//!
//! All counters live on `RaftNode` (inside `LeaderState`); the
//! [`Replication`] / [`PeerReplication`] types are zero-state borrow
//! views.

use std::time::Instant;

use crate::log_entry::LogEntryRange;
use crate::node::RaftNode;
use crate::persistence::Persistence;
use crate::types::{NodeId, RejectReason, Term, TxId};

/// Wire payload for an `AppendEntries` RPC. `entries.is_empty()`
/// is a heartbeat — the AE still ships
/// `(prev_log_tx_id, prev_log_term, leader_commit)` for the
/// follower's §5.3 / commit-watermark propagation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendEntriesRequest {
    pub to: NodeId,
    pub term: Term,
    pub prev_log_tx_id: TxId,
    pub prev_log_term: Term,
    pub entries: LogEntryRange,
    pub leader_commit: TxId,
}

/// Outcome of an `AppendEntries` RPC. Built by the cluster from
/// the wire response (or a local RPC timeout) and fed back via
/// [`PeerReplication::append_result`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendResult {
    /// Peer accepted and durably wrote up to `last_write_id`,
    /// committed up to `last_commit_id`. `term` is the peer's
    /// reported term — if it exceeds the leader's current term,
    /// the leader steps down.
    Success {
        term: Term,
        last_write_id: TxId,
        last_commit_id: TxId,
    },
    /// Peer rejected. `reason` drives the recovery strategy:
    ///
    /// - [`RejectReason::LogMismatch`] — leader walks `next_index`
    ///   back one (clamped at 1) and the cluster's next iteration
    ///   builds a fresh request with the lower `prev_log_*`.
    /// - [`RejectReason::TermBehind`] — peer's term is strictly
    ///   higher; leader observes the term and transitions to
    ///   follower.
    Reject {
        term: Term,
        reason: RejectReason,
        last_write_id: TxId,
        last_commit_id: TxId,
    },
    /// RPC fired but no reply within the cluster's deadline. The
    /// state machine clears `in_flight` and leaves `next_index` /
    /// `match_index` unchanged — the next iteration of the
    /// per-peer loop builds a fresh request.
    Timeout,
}

/// Top-level handle for leader-side replication. Acquired via
/// [`RaftNode::replication`](crate::node::RaftNode::replication).
pub struct Replication<'a, P: Persistence> {
    node: &'a mut RaftNode<P>,
}

/// Per-peer replication handle. All mutations route to
/// `RaftNode::LeaderState`; the handle holds no state of its own.
pub struct PeerReplication<'a, P: Persistence> {
    node: &'a mut RaftNode<P>,
    peer_id: NodeId,
}

impl<'a, P: Persistence> Replication<'a, P> {
    pub(crate) fn new(node: &'a mut RaftNode<P>) -> Self {
        Self { node }
    }

    /// Whether this node is currently a leader. The cluster's
    /// per-peer loop should exit when this returns `false`.
    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    /// Snapshot list of peer ids excluding `self_id`. Owned `Vec`
    /// so the caller can iterate without holding a borrow on
    /// `Replication`.
    pub fn peers(&self) -> Vec<NodeId> {
        let self_id = self.node.self_id();
        self.node
            .peers()
            .iter()
            .copied()
            .filter(|&id| id != self_id)
            .collect()
    }

    /// Per-peer view. Returns `None` if `peer_id` is not a
    /// configured peer of this cluster (or is `self_id`).
    /// Consumes `self` — re-acquire via `node.replication()` to
    /// access another peer.
    pub fn peer(self, peer_id: NodeId) -> Option<PeerReplication<'a, P>> {
        if peer_id == self.node.self_id() {
            return None;
        }
        if !self.node.peers().contains(&peer_id) {
            return None;
        }
        Some(PeerReplication {
            node: self.node,
            peer_id,
        })
    }
}

impl<'a, P: Persistence> PeerReplication<'a, P> {
    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    /// Read-only: the next tx_id this leader will ship to the
    /// peer. `next_index - 1` corresponds to the AE's
    /// `prev_log_tx_id`.
    pub fn next_index(&self) -> TxId {
        self.node.replication_peer_next_index(self.peer_id)
    }

    /// Read-only: peer's reported durable end. Drives the
    /// cluster-wide quorum.
    pub fn match_index(&self) -> TxId {
        self.node.replication_peer_match_index(self.peer_id)
    }

    /// Build the next `AppendEntries` request for this peer and
    /// mark the RPC as in-flight. Returns `None` if this node is
    /// no longer the leader, or if `peer_id` was removed from the
    /// cluster.
    ///
    /// The returned range covers `[next_index ..
    /// local_write_index]`, capped by the configured
    /// `max_entries_per_append` and stopping at the first term
    /// boundary (multi-term catch-up takes multiple RPCs). An
    /// empty range is a heartbeat — the AE still ships
    /// `(prev_log_tx_id, prev_log_term, leader_commit)` for
    /// follower-side §5.3 / commit-watermark propagation.
    ///
    /// The cluster driver is expected to send this RPC and feed
    /// the outcome back via [`Self::append_result`].
    pub fn get_append_range(&mut self, now: Instant) -> Option<AppendEntriesRequest> {
        self.node.replication_get_append_range(self.peer_id, now)
    }

    /// Feed the outcome of an `AppendEntries` RPC back into the
    /// state machine. Updates per-peer `next_index` /
    /// `match_index`, clears `in_flight`, and may advance
    /// `cluster_commit_index` (in-place; observable via
    /// [`RaftNode::cluster_commit_index`](crate::node::RaftNode::cluster_commit_index)).
    pub fn append_result(&mut self, now: Instant, result: AppendResult) {
        self.node
            .replication_append_result(self.peer_id, now, result);
    }
}
