//! Follower role-specific state.
//!
//! The follower's only Raft-relevant memory beyond the durable
//! `(current_term, voted_for)` is which node it currently believes is
//! leader — used to ignore stale `AppendEntries` from a previous
//! leader's late RPCs. The election timer lives on `RaftNode`
//! directly because Initializing also runs one.
//!
//! `pending_leader_commit` holds the most recent `leader_commit`
//! observed by `validate_append_entries_request` whose entries the
//! cluster driver is still durably persisting. It is consumed inside
//! `RaftNode::advance(write, commit)` once the cluster reports
//! durability — clamped to the freshly-updated `local_commit_index`
//! and propagated into `cluster_commit_index` (Raft §5.3 follower
//! commit rule). Cleared along with the rest of the FollowerState on
//! transition out of Follower.

use crate::types::{NodeId, TxId};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FollowerState {
    /// `Some(id)` after a valid `AppendEntries` from `id`. Cleared
    /// only on transition out of Follower, not on heartbeat timeout
    /// — until the next leader speaks, the previous leader id
    /// remains the best guess for routing client redirects.
    pub leader_id: Option<NodeId>,
    /// `leader_commit` carried by the most recent
    /// `validate_append_entries_request` whose entries the cluster is
    /// still durably persisting. Drained inside `RaftNode::advance`
    /// after `local_commit_index` updates, where it is clamped to the
    /// new `local_commit_index` and propagated into
    /// `cluster_commit_index` (Raft §5.3 follower commit rule).
    /// Last-writer-wins on overwrite — a second validate before the
    /// first's durability ack supersedes the earlier value.
    pub pending_leader_commit: Option<TxId>,
}

impl FollowerState {
    pub fn new() -> Self {
        Self {
            leader_id: None,
            pending_leader_commit: None,
        }
    }

    pub fn with_leader(leader_id: NodeId) -> Self {
        Self {
            leader_id: Some(leader_id),
            pending_leader_commit: None,
        }
    }

    pub fn observe_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_leader_records_id() {
        let mut s = FollowerState::new();
        assert_eq!(s.leader_id, None);
        s.observe_leader(7);
        assert_eq!(s.leader_id, Some(7));
        s.observe_leader(11);
        assert_eq!(s.leader_id, Some(11));
    }
}
