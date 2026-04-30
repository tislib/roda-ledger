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
//! Payload bytes do not flow through this enum (or through
//! `Action`). The driver moves payloads between storage and the wire
//! on its own; `LogEntryRange` carries only `(start_tx_id, count,
//! term)` — the meta needed for §5.3 prev_log_term matching and
//! durable term-log updates.

use crate::log_entry::LogEntryRange;
use crate::types::{NodeId, RejectReason, Term, TxId};

#[derive(Clone, Debug)]
pub enum Event {
    /// Wake up and re-check timers. The driver fires `Tick` when an
    /// `Action::SetWakeup` deadline arrives.
    Tick,

    /// Inbound `AppendEntries` from a peer. `entries` is a single
    /// same-term contiguous range — heartbeat is `LogEntryRange::empty()`.
    AppendEntriesRequest {
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: LogEntryRange,
        leader_commit: TxId,
    },

    /// Reply to an `AppendEntries` we sent.
    AppendEntriesReply {
        from: NodeId,
        term: Term,
        success: bool,
        last_tx_id: TxId,
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

    /// Leader-only: ledger reports it durably committed an entry
    /// locally. Bridge from the ledger's on-commit hook into raft.
    /// Advances the leader's own slot in match_index, feeding into
    /// cluster_commit_index.
    LocalCommitAdvanced { tx_id: TxId },

    /// Leader-only: driver acknowledges that entries up to `tx_id`
    /// are durably written to the leader's raft log and may be
    /// replicated. Distinct from `LocalCommitAdvanced` — that one
    /// fires when the ledger commits an entry locally and feeds the
    /// leader's quorum self-slot. `LocalWriteAdvanced` only bounds
    /// the AE replication window via `last_written`; it does not
    /// touch `local_log_index`, the quorum, or `cluster_commit_index`.
    LocalWriteAdvanced { tx_id: TxId },

    /// Driver acknowledges the most recent `Action::AppendLog` for
    /// the follower path. `tx_id` is the highest tx_id in the
    /// just-appended range.
    LogAppendComplete { tx_id: TxId },

    /// Driver acknowledges the most recent `Action::TruncateLog`.
    LogTruncateComplete { up_to: TxId },
}
