//! Follower role-specific state.
//!
//! The follower's only Raft-relevant memory beyond the durable
//! `(current_term, voted_for)` is which node it currently believes is
//! leader — used to ignore stale `AppendEntries` from a previous
//! leader's late RPCs. The election timer lives on `RaftNode`
//! directly because Initializing also runs one.

use crate::types::NodeId;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FollowerState {
    /// `Some(id)` after a valid `AppendEntries` from `id`. Cleared
    /// only on transition out of Follower, not on heartbeat timeout
    /// — until the next leader speaks, the previous leader id
    /// remains the best guess for routing client redirects.
    pub leader_id: Option<NodeId>,
}

impl FollowerState {
    pub fn new() -> Self {
        Self { leader_id: None }
    }

    pub fn with_leader(leader_id: NodeId) -> Self {
        Self {
            leader_id: Some(leader_id),
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
