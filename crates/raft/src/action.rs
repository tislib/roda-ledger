//! Outputs from `RaftNode::step`. The driver reads the action list
//! and executes each one in order, then feeds completion events back
//! through the next `step`.
//!
//! ADR-0017 §"Architectural Boundary": the library does not perform
//! I/O. Durable writes to the term and vote logs go through the
//! synchronous [`crate::Persistence`] trait that `RaftNode` is
//! parameterised over — they do **not** appear in this enum.
//! Everything below is a side effect the *driver* must perform
//! (sending wire RPCs, appending the entry log, advancing apply
//! gates, etc.).
//!
//! The library only ever talks about *ranges* — never individual
//! entries — so `SendAppendEntries` and `AppendLog` both carry a
//! single [`LogEntryRange`] (a contiguous, same-term chunk). The
//! driver expands the range when reading from / writing to its WAL.

use std::time::Instant;

use crate::log_entry::LogEntryRange;
use crate::role::Role;
use crate::types::{NodeId, Term, TxId};

#[derive(Clone, Debug)]
pub enum Action {
    // ── outbound RPCs ───────────────────────────────────────────────────
    /// `entries` is a single same-term contiguous range.
    /// `LogEntryRange::empty()` is a heartbeat.
    SendAppendEntries {
        to: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: LogEntryRange,
        leader_commit: TxId,
    },
    SendAppendEntriesReply {
        to: NodeId,
        term: Term,
        success: bool,
        last_tx_id: TxId,
    },
    SendRequestVote {
        to: NodeId,
        term: Term,
        last_tx_id: TxId,
        last_term: Term,
    },
    SendRequestVoteReply {
        to: NodeId,
        term: Term,
        granted: bool,
    },

    // ── follower log directives ─────────────────────────────────────────
    /// Drop entry-log records with `tx_id > after_tx_id`. The library
    /// has already truncated its term-log mirror via the
    /// `Persistence` trait; this action is for the *entry* log the
    /// driver owns. The driver acks via `Event::LogTruncateComplete`.
    TruncateLog { after_tx_id: TxId },
    /// Append the entries described by `range` to the entry log.
    /// The driver already has the payloads (they came in on the
    /// inbound RPC); this action tells it to commit metadata +
    /// payloads durably and ack via `Event::LogAppendComplete` with
    /// the highest tx_id in the range.
    AppendLog { range: LogEntryRange },

    // ── apply pipeline gate ─────────────────────────────────────────────
    /// The cluster commit watermark advanced to `tx_id`. The driver
    /// gates the ledger's apply pipeline on this signal — ADR-0017
    /// §"Apply-on-commit, not apply-on-durable".
    AdvanceClusterCommit { tx_id: TxId },

    // ── role observability ──────────────────────────────────────────────
    /// Notify the driver of a role transition. Mirrors the public
    /// `role()` query for cases where the driver wants an event-driven
    /// notification (gRPC handler swaps, supervisor signals).
    BecomeRole { role: Role, term: Term },

    // ── timer management ────────────────────────────────────────────────
    /// Schedule the next `Event::Tick` at `at`. The library re-emits
    /// this on every transition that changes the soonest deadline.
    SetWakeup { at: Instant },
}
