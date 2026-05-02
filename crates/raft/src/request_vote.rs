//! Wire payload types for the direct-method `RequestVote` path.
//!
//! [`RaftNode::request_vote`](crate::node::RaftNode::request_vote)
//! lets the cluster driver call into the voter side synchronously,
//! returning a [`RequestVoteReply`] directly — same shape and semantics
//! as feeding `Event::RequestVoteRequest` through `step` and reading
//! the resulting `Action::SendRequestVoteReply`, but without the
//! action-stream round-trip. The legacy event/action path is
//! unchanged and remains the canonical entry-point for the simulator
//! and any consumer that wants to drive raft purely through `step`.

use crate::types::{NodeId, Term, TxId};

/// Inbound `RequestVote` from a candidate. Mirrors the field set of
/// `Event::RequestVoteRequest` so the cluster can build either shape
/// from the same wire decode.
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
