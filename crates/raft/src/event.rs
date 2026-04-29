//! Inputs to `RaftNode::step`. Everything the state machine reacts to
//! arrives as one of these.
//!
//! Wall-clock time is **not** carried on the variants — `step` takes
//! a `now: Instant` parameter that's authoritative for the call.
//! `Event::Tick` is the explicit "wake up and re-check" signal the
//! driver fires when an `Action::SetWakeup` deadline arrives; all
//! other events ride the same `now` as the call that delivered
//! them.
//!
//! Payload bytes do not flow through this enum (or through
//! `Action`). The driver moves payloads between storage and the wire
//! on its own; `LogEntryMeta` carries only the `(tx_id, term)` pair
//! Raft needs for §5.3 prev_log_term matching.

use crate::log_entry::LogEntryMeta;
use crate::types::{NodeId, RejectReason, Term, TxId};

#[derive(Clone, Debug)]
pub enum Event {
    /// Wake up and re-check timers. The driver fires `Tick` when an
    /// `Action::SetWakeup` deadline arrives.
    Tick,

    /// Inbound `AppendEntries` from a peer.
    AppendEntriesRequest {
        from: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: Vec<LogEntryMeta>,
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
    /// locally. ADR-0017 §"Asymmetric write paths" — the on-commit
    /// hook bridges from the leader's ledger into raft, so the
    /// leader's slot in `Quorum::match_index` advances.
    LocalCommitAdvanced { tx_id: TxId },

    /// Driver acknowledges the most recent `Action::AppendLog` for
    /// the follower path. After this fires, the follower's
    /// `commit_index` (local) advances to `tx_id`.
    LogAppendComplete { tx_id: TxId },

    /// Driver acknowledges the most recent `Action::TruncateLog`.
    /// The follower's local-log view is now `<= up_to`.
    LogTruncateComplete { up_to: TxId },

    /// Driver acknowledges that `Action::PersistTerm { term, start_tx_id }`
    /// is durable on disk. The library treats these as
    /// observability hooks: it has already updated its in-memory
    /// model when emitting the action; the ack confirms the durable
    /// write completed and lets the driver round-trip its state
    /// asynchronously without lying about durability.
    TermPersisted { term: Term, start_tx_id: TxId },

    /// Driver acknowledges `Action::PersistVote`.
    VotePersisted { term: Term, voted_for: NodeId },
}
