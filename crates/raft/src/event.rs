//! Inputs to `RaftNode::step`. Everything the state machine reacts
//! to arrives as one of these.
//!
//! Wall-clock time is **not** carried on the variants — `step` takes
//! a `now: Instant` parameter that's authoritative for the call.
//! `Event::Tick` is the explicit "wake up and re-check" signal the
//! driver fires when an `Action::SetWakeup` deadline arrives.
//!
//! Durable persistence of term/vote logs is mediated synchronously
//! through the [`crate::Persistence`] trait — there is no
//! `Event::*Persisted` ack. When a trait write returns `Ok(_)`, the
//! library treats the change as durable and proceeds.
//!
//! Inbound RPCs do **not** flow through this enum:
//!
//! - Follower-side `AppendEntries`: the cluster driver calls
//!   `RaftNode::validate_append_entries_request` and performs the
//!   WAL I/O / `advance(write, commit)` itself.
//! - Leader-side `AppendEntries` reply: the cluster driver feeds it
//!   into `RaftNode::replication().peer(id).append_result(now, result)`.
//! - Voter-side `RequestVote`: the cluster driver calls
//!   `RaftNode::request_vote` and gets a `RequestVoteReply`
//!   synchronously.
//!
//! All three are direct methods. Only `Tick` and the candidate's
//! own `RequestVoteReply` (the side waiting for the synchronous
//! voter response to come back over the wire) flow through `step`.

use crate::types::{NodeId, Term};

#[derive(Clone, Debug)]
pub enum Event {
    /// Wake up and re-check timers. The driver fires `Tick` when an
    /// `Action::SetWakeup` deadline arrives.
    Tick,

    /// Reply to a `RequestVote` we sent.
    RequestVoteReply {
        from: NodeId,
        term: Term,
        granted: bool,
    },
}
