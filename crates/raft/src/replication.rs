//! Leader-side replication driver, exposed to the cluster via
//! [`RaftNode::replication`](crate::node::RaftNode::replication).
//!
//! ADR-0017 §"Driver call pattern": the cluster — not raft — owns
//! the per-peer loop that sends `AppendEntries` RPCs. Raft owns the
//! decision *what* to send (the next range, the leader-commit
//! watermark) and *how to react* to the reply (advance / regress
//! per-peer indexes, run the quorum, step down on a higher term).

use std::time::Instant;

use crate::log_entry::LogEntryRange;
use crate::node::RaftNode;
use crate::persistence::Persistence;
use crate::types::{NodeId, RejectReason, Term, TxId};

/// In-stream replication payload. `entries.is_empty()` is a
/// heartbeat — still carries `leader_commit` so the follower can
/// advance its cluster-commit view.
///
/// Per the handshake-only validation model, this RPC does not
/// carry the §5.3 anchor — that is validated once at stream setup
/// via the handshake, and the follower trusts subsequent updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendEntriesRequest {
    pub to: NodeId,
    pub entries: LogEntryRange,
    pub leader_commit: TxId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendResult {
    Success {
        term: Term,
        last_commit_id: TxId,
    },
    Reject {
        term: Term,
        reason: RejectReason,
        last_commit_id: TxId,
    },
    Timeout,
}

pub struct Replication<'a, P: Persistence> {
    node: &'a mut RaftNode<P>,
}

pub struct PeerReplication<'a, P: Persistence> {
    node: &'a mut RaftNode<P>,
    peer_id: NodeId,
}

impl<'a, P: Persistence> Replication<'a, P> {
    pub(crate) fn new(node: &'a mut RaftNode<P>) -> Self {
        Self { node }
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    pub fn peers(&self) -> Vec<NodeId> {
        let self_id = self.node.self_id();
        self.node
            .peers()
            .iter()
            .copied()
            .filter(|&id| id != self_id)
            .collect()
    }

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

    pub fn next_index(&self) -> TxId {
        self.node.replication_peer_next_index(self.peer_id)
    }

    pub fn match_index(&self) -> TxId {
        self.node.replication_peer_match_index(self.peer_id)
    }

    pub fn get_append_range(&mut self, now: Instant) -> Option<AppendEntriesRequest> {
        self.node.replication_get_append_range(self.peer_id, now)
    }

    pub fn append_result(&mut self, now: Instant, result: AppendResult) {
        self.node
            .replication_append_result(self.peer_id, now, result);
    }
}
