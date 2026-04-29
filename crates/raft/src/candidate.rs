//! Candidate role-specific state.
//!
//! On Initializing/Follower → Candidate transition the library
//! constructs a `CandidateState`, durably bumps the term + self-votes
//! via `Vote::vote(new_term, self_id)`, and emits one
//! `Action::SendRequestVote` per other peer. As replies arrive they
//! land in `votes_received`; once `votes_received.len() >= majority`
//! the candidate transitions to Leader and the durable term boundary
//! is committed via `Term::commit_term` — Raft's "Election Safety"
//! property fix from ADR-0017 §"Required Invariants".

use std::collections::HashSet;
use std::time::Instant;

use crate::types::NodeId;

#[derive(Clone, Debug)]
pub struct CandidateState {
    /// Term we are running this election in. Stamped onto every
    /// outgoing `RequestVote` so a peer's reply can be matched
    /// against the round it answers.
    pub election_term: u64,
    /// Set of nodes (including ourselves) that have granted us a
    /// vote in `election_term`. Inserts only.
    votes_received: HashSet<NodeId>,
    /// Per-peer expiry of the in-flight `RequestVote`. On `Tick`
    /// expired entries are treated as no-vote replies (the slot is
    /// removed; the peer is simply not counted toward the majority).
    in_flight: std::collections::HashMap<NodeId, Instant>,
}

impl CandidateState {
    pub fn new(election_term: u64, self_id: NodeId) -> Self {
        let mut votes_received = HashSet::new();
        votes_received.insert(self_id);
        Self {
            election_term,
            votes_received,
            in_flight: std::collections::HashMap::new(),
        }
    }

    pub fn record_in_flight(&mut self, peer: NodeId, expires_at: Instant) {
        self.in_flight.insert(peer, expires_at);
    }

    /// Drop the in-flight entry for `peer`, returning whether one was
    /// present. Used both on receiving a reply and on RPC-timeout
    /// expiry.
    pub fn complete_in_flight(&mut self, peer: NodeId) -> bool {
        self.in_flight.remove(&peer).is_some()
    }

    /// Returns `true` if the grant pushed us over the majority
    /// threshold for the first time. Idempotent on repeated grants
    /// from the same peer.
    pub fn record_grant(&mut self, peer: NodeId, majority: usize) -> bool {
        let pre = self.votes_received.len();
        self.votes_received.insert(peer);
        let post = self.votes_received.len();
        post >= majority && pre < majority
    }

    pub fn votes_received(&self) -> usize {
        self.votes_received.len()
    }

    /// Soonest expiry among in-flight `RequestVote` RPCs, or `None`
    /// if there are no outstanding RPCs. The state machine uses this
    /// to decide whether a `Tick` should synthesise a timeout.
    pub fn next_rpc_expiry(&self) -> Option<Instant> {
        self.in_flight.values().min().copied()
    }

    /// All peers whose RPC has expired by `now`, in deterministic
    /// (sorted) order. The state machine pops these and treats them
    /// as no-vote replies.
    pub fn drain_expired(&mut self, now: Instant) -> Vec<NodeId> {
        let mut expired: Vec<NodeId> = self
            .in_flight
            .iter()
            .filter(|(_, exp)| **exp <= now)
            .map(|(p, _)| *p)
            .collect();
        expired.sort_unstable();
        for p in &expired {
            self.in_flight.remove(p);
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn self_vote_seeds_vote_set() {
        let s = CandidateState::new(1, 7);
        assert_eq!(s.election_term, 1);
        assert_eq!(s.votes_received(), 1);
    }

    #[test]
    fn record_grant_reaches_majority_on_threshold_crossing() {
        // 5-node cluster, majority = 3; need 2 more grants beyond self.
        let mut s = CandidateState::new(1, 1);
        assert!(!s.record_grant(2, 3));
        // The crossing returns true *exactly* on the first grant
        // that reaches the threshold.
        assert!(s.record_grant(3, 3));
        // Subsequent grants past the threshold do not re-trigger.
        assert!(!s.record_grant(4, 3));
    }

    #[test]
    fn record_grant_is_idempotent_on_repeated_peer() {
        let mut s = CandidateState::new(1, 1);
        assert!(!s.record_grant(2, 3));
        assert!(!s.record_grant(2, 3));
        assert_eq!(s.votes_received(), 2);
    }

    #[test]
    fn next_rpc_expiry_returns_soonest() {
        let mut s = CandidateState::new(1, 1);
        let now = t0();
        s.record_in_flight(2, now + Duration::from_millis(50));
        s.record_in_flight(3, now + Duration::from_millis(20));
        s.record_in_flight(4, now + Duration::from_millis(100));
        assert_eq!(s.next_rpc_expiry(), Some(now + Duration::from_millis(20)));
    }

    #[test]
    fn drain_expired_removes_only_due_entries() {
        let mut s = CandidateState::new(1, 1);
        let now = t0();
        s.record_in_flight(2, now + Duration::from_millis(50));
        s.record_in_flight(3, now + Duration::from_millis(150));
        let expired = s.drain_expired(now + Duration::from_millis(100));
        assert_eq!(expired, vec![2]);
        assert!(s.in_flight.contains_key(&3));
    }

    #[test]
    fn complete_in_flight_returns_presence() {
        let mut s = CandidateState::new(1, 1);
        let now = t0();
        s.record_in_flight(2, now + Duration::from_millis(50));
        assert!(s.complete_in_flight(2));
        assert!(!s.complete_in_flight(2));
    }
}
