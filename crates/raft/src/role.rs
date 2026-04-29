//! Public role discriminant.
//!
//! The state machine carries strictly more state than this enum
//! exposes (per-peer replication progress, votes received, etc. — see
//! `follower.rs`, `candidate.rs`, `leader.rs`), but only the role
//! discriminant is part of the public read API. ADR-0017 §"Read-only
//! query API": "Anything beyond [role/term/commit] is internal to
//! raft."

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Role {
    /// Pre-election bring-up. No leader known, election timer armed.
    /// On timeout transitions to Candidate. Distinct from Follower
    /// because there's no leader to track yet — it lets the gRPC
    /// boundary distinguish "I never saw a leader" from "I had one
    /// and lost it".
    Initializing,
    /// Reads heartbeats from a leader. Optional `leader_id` lives in
    /// `FollowerState`. On election timeout transitions to Candidate.
    Follower,
    /// Running an election. Owns `votes_received` and per-peer
    /// in-flight `RequestVote` deadlines.
    Candidate,
    /// Replicates entries to peers, owns `next_index`/`match_index`
    /// per peer, drives heartbeats. No election timer.
    Leader,
}

impl Role {
    #[inline]
    pub fn is_leader(self) -> bool {
        matches!(self, Role::Leader)
    }

    #[inline]
    pub fn is_follower(self) -> bool {
        matches!(self, Role::Follower)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicates_match_variants() {
        assert!(Role::Leader.is_leader());
        assert!(!Role::Follower.is_leader());
        assert!(Role::Follower.is_follower());
        assert!(!Role::Candidate.is_follower());
    }
}
