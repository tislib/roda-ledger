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
//! `AppendEntries` does **not** flow through this enum:
//!
//! - Follower path: the cluster driver calls
//!   `RaftNode::validate_append_entries_request` and performs the
//!   WAL I/O / `advance(write, commit)` itself.
//! - Leader path: the cluster driver feeds replies into
//!   `RaftNode::replication().peer(id).append_result(now, result)`.
//!
//! Both AE-related paths are direct methods, not events.

use crate::types::{NodeId, Term, TxId};

#[derive(Clone, Debug)]
pub enum Event {
    /// Wake up and re-check timers. The driver fires `Tick` when an
    /// `Action::SetWakeup` deadline arrives.
    Tick,

    /// Inbound `RequestVote` from a candidate.
    RequestVoteRequest {
        from: NodeId,
        term: Term,
        last_tx_id: TxId,
        last_term: Term,
    },

    /// Reply to a `RequestVote` we sent.
    RequestVoteReply {
        from: NodeId,
        term: Term,
        granted: bool,
    },
}
