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

use std::time::Instant;

use crate::log_entry::LogEntryMeta;
use crate::role::Role;
use crate::types::{NodeId, Term, TxId};

#[derive(Clone, Debug)]
pub enum Action {
    // ── outbound RPCs ───────────────────────────────────────────────────
    SendAppendEntries {
        to: NodeId,
        term: Term,
        prev_log_tx_id: TxId,
        prev_log_term: Term,
        entries: Vec<LogEntryMeta>,
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
    /// Append the entry whose metadata matches `(tx_id, term)`. The
    /// driver already has the payload (it received it as part of the
    /// inbound RPC); raft tells it to commit the metadata + payload
    /// pair durably and then ack via `Event::LogAppendComplete`.
    AppendLog { tx_id: TxId, term: Term },

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
