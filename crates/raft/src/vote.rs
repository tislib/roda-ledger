//! Pure in-memory vote-state model.
//!
//! ADR-0017: the library performs no I/O. The driver owns disk-side
//! persistence; this module models only `(current_term, voted_for)`.
//! Mutators update memory and the caller (`RaftNode`) emits a
//! matching `Action::PersistVote` so the driver can persist.

use crate::types::{NodeId, Term};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoteError {
    /// `candidate_id == 0` is reserved for "no vote".
    InvalidCandidate,
    /// Strict regression: incoming term is less than current.
    Regression { current: Term, incoming: Term },
}

pub struct Vote {
    current_term: Term,
    /// `0` = no vote granted in `current_term`.
    voted_for: u64,
}

impl Vote {
    pub fn new(current_term: Term, voted_for: u64) -> Self {
        Self {
            current_term,
            voted_for,
        }
    }

    pub fn empty() -> Self {
        Self::new(0, 0)
    }

    #[inline]
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// `Some(node_id)` iff a vote was granted in `current_term`,
    /// `None` otherwise. `voted_for == 0` is the wire encoding of "no
    /// vote".
    #[inline]
    pub fn voted_for(&self) -> Option<NodeId> {
        match self.voted_for {
            0 => None,
            n => Some(n),
        }
    }

    /// Raft §5.4.1: grant a vote for `candidate_id` in `term`.
    ///
    /// Returns `Ok(true)` on grant (or idempotent re-grant), `Ok(false)`
    /// when refused (already voted for somebody else this term, or
    /// `term < current_term`). `candidate_id == 0` is rejected as
    /// malformed input.
    pub fn vote(&mut self, term: Term, candidate_id: NodeId) -> Result<bool, VoteError> {
        if candidate_id == 0 {
            return Err(VoteError::InvalidCandidate);
        }
        if term < self.current_term {
            return Ok(false);
        }
        if term == self.current_term {
            if self.voted_for == candidate_id {
                return Ok(true);
            }
            if self.voted_for != 0 {
                return Ok(false);
            }
            // No vote yet in this term — fall through and grant.
        }
        self.current_term = term;
        self.voted_for = candidate_id;
        Ok(true)
    }

    /// Record a strictly higher `term` observed via inbound RPC,
    /// clearing the vote (so the next vote in this term is allowed).
    /// Idempotent on equal term, errors on strict regression.
    pub fn observe_term(&mut self, term: Term) -> Result<(), VoteError> {
        if term == self.current_term {
            return Ok(());
        }
        if term < self.current_term {
            return Err(VoteError::Regression {
                current: self.current_term,
                incoming: term,
            });
        }
        self.current_term = term;
        self.voted_for = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_starts_unvoted_at_term_zero() {
        let vote = Vote::empty();
        assert_eq!(vote.current_term(), 0);
        assert_eq!(vote.voted_for(), None);
    }

    #[test]
    fn first_vote_in_term_is_granted() {
        let mut vote = Vote::empty();
        assert!(vote.vote(1, 7).unwrap());
        assert_eq!(vote.current_term(), 1);
        assert_eq!(vote.voted_for(), Some(7));
    }

    #[test]
    fn same_term_same_candidate_is_idempotent() {
        let mut vote = Vote::empty();
        assert!(vote.vote(3, 9).unwrap());
        assert!(vote.vote(3, 9).unwrap());
        assert_eq!(vote.voted_for(), Some(9));
    }

    #[test]
    fn same_term_different_candidate_is_refused() {
        let mut vote = Vote::empty();
        assert!(vote.vote(3, 9).unwrap());
        assert!(!vote.vote(3, 11).unwrap());
        assert_eq!(vote.voted_for(), Some(9));
    }

    #[test]
    fn older_term_vote_request_is_refused() {
        let mut vote = Vote::empty();
        assert!(vote.vote(5, 1).unwrap());
        assert!(!vote.vote(4, 2).unwrap());
        assert_eq!(vote.current_term(), 5);
        assert_eq!(vote.voted_for(), Some(1));
    }

    #[test]
    fn higher_term_vote_advances_term() {
        let mut vote = Vote::empty();
        assert!(vote.vote(1, 1).unwrap());
        assert!(vote.vote(2, 7).unwrap());
        assert_eq!(vote.current_term(), 2);
        assert_eq!(vote.voted_for(), Some(7));
    }

    #[test]
    fn observe_higher_term_clears_voted_for() {
        let mut vote = Vote::empty();
        assert!(vote.vote(1, 1).unwrap());
        vote.observe_term(2).unwrap();
        assert_eq!(vote.current_term(), 2);
        assert_eq!(vote.voted_for(), None);
        assert!(vote.vote(2, 5).unwrap());
        assert_eq!(vote.voted_for(), Some(5));
    }

    #[test]
    fn observe_same_term_is_idempotent() {
        let mut vote = Vote::empty();
        assert!(vote.vote(3, 4).unwrap());
        vote.observe_term(3).unwrap();
        assert_eq!(vote.voted_for(), Some(4));
    }

    #[test]
    fn observe_lower_term_is_rejected() {
        let mut vote = Vote::empty();
        assert!(vote.vote(5, 1).unwrap());
        let err = vote.observe_term(4).unwrap_err();
        assert!(matches!(err, VoteError::Regression { .. }));
    }

    #[test]
    fn zero_candidate_id_is_rejected() {
        let mut vote = Vote::empty();
        let err = vote.vote(1, 0).unwrap_err();
        assert!(matches!(err, VoteError::InvalidCandidate));
    }

    #[test]
    fn new_can_seed_durable_state() {
        let vote = Vote::new(7, 12);
        assert_eq!(vote.current_term(), 7);
        assert_eq!(vote.voted_for(), Some(12));
    }
}
