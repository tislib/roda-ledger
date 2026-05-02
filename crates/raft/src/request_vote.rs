//! Wire payload types for the direct-method `RequestVote` path.
//!
//! [`RaftNode::request_vote`](crate::node::RaftNode::request_vote)
//! is the voter-side entry-point: the cluster driver calls in
//! synchronously and gets a [`RequestVoteReply`] back. There is no
//! event/action equivalent — `RequestVote` is exclusively direct-
//! method.

use crate::types::{NodeId, Term, TxId};

/// Inbound `RequestVote` from a candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestVoteRequest {
    pub from: NodeId,
    pub term: Term,
    pub last_tx_id: TxId,
    pub last_term: Term,
}

/// Voter-side decision. The driver stamps `term` from
/// `current_term()` (which may have advanced inside the call if the
/// candidate's term was strictly higher) and `granted` from the
/// `Persistence::vote` outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestVoteReply {
    pub term: Term,
    pub granted: bool,
}
