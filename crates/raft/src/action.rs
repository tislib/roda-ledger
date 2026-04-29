//! Outputs from `RaftNode::step`. The driver reads the action list,
//! executes each one in order (`Persist*` writes durably first, then
//! `Send*`), and feeds completion events back through the next
//! `step`.
//!
//! ADR-0017 §"Architectural Boundary": the library does not perform
//! I/O. Every side effect — including durable writes to the term and
//! vote logs raft logically owns — is described here; the driver
//! does the actual work.

use std::time::Instant;

use crate::log_entry::LogEntryMeta;
use crate::role::Role;
use crate::types::{NodeId, Term, TxId};

#[derive(Clone, Debug)]
pub enum Action {
    // ── durable writes the driver routes to the term / vote logs ────────
    /// Append `(term, start_tx_id)` to the term log. The driver
    /// writes durably (e.g. `storage::TermStorage`), then feeds back
    /// `Event::TermPersisted` so the library knows the on-disk state
    /// caught up. The library has already updated its in-memory
    /// model by the time it emits this action — the ack is purely
    /// "the durable view matches the in-memory view".
    PersistTerm { term: Term, start_tx_id: TxId },
    /// Append `(term, voted_for)` to the vote log. `voted_for == 0`
    /// is the wire encoding of "no vote granted in this term".
    /// Acked via `Event::VotePersisted`.
    PersistVote { term: Term, voted_for: NodeId },

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
    /// Drop log entries with `tx_id > after_tx_id`. The driver
    /// truncates **both** the entry log and its term-log mirror to
    /// keep the §5.3 invariant (paired truncation). Once durable it
    /// feeds back `Event::LogTruncateComplete`.
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
    /// The driver re-arms its `tokio::time::sleep_until`.
    SetWakeup { at: Instant },
}
