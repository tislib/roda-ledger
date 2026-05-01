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
//! Inbound `AppendEntries` does **not** flow through this enum: the
//! follower path uses the direct-method
//! `RaftNode::validate_append_entries_request` (with the cluster
//! driver performing WAL I/O and `advance(write, commit)` itself).
//! The leader's reply observation lives here as
//! `Event::AppendEntriesReply`.

use crate::types::{NodeId, RejectReason, Term, TxId};

#[derive(Clone, Debug)]
pub enum Event {
    /// Wake up and re-check timers. The driver fires `Tick` when an
    /// `Action::SetWakeup` deadline arrives.
    Tick,

    /// Reply to an `AppendEntries` we sent.
    ///
    /// Two watermarks: `last_commit_id` is the follower's durably-
    /// committed end (drives `match_index` and the cluster quorum);
    /// `last_write_id` is the follower's accepted/written end (drives
    /// the replication window — `next_index`). Invariant:
    /// `last_write_id >= last_commit_id`. See ADR-0017 §"AE reply:
    /// write vs commit watermark".
    AppendEntriesReply {
        from: NodeId,
        term: Term,
        success: bool,
        last_commit_id: TxId,
        last_write_id: TxId,
        reject_reason: Option<RejectReason>,
    },

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
