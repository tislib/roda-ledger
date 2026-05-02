//! Outputs from `RaftNode::step`. The driver reads the action list
//! and executes each one in order, then feeds completion events back
//! through the next `step`.
//!
//! ADR-0017 §"Architectural Boundary": the library does not perform
//! I/O. Durable writes to the term and vote logs go through the
//! synchronous [`crate::Persistence`] trait that `RaftNode` is
//! parameterised over — they do **not** appear in this enum.
//! Everything below is a side effect the *driver* must perform
//! (sending wire RPCs, scheduling wakeups, etc.).
//!
//! Follower-side AppendEntries handling does **not** flow through
//! `step` / `Action` — see `RaftNode::validate_append_entries_request`
//! and the `AppendEntriesDecision` returned by it for the direct-
//! method path. The driver applies the decision's WAL directives
//! (truncate / append) itself, then calls
//! `RaftNode::advance(write, commit)` once the entries are durable.
//! The wire reply is built from the post-advance getters.
//!
//! Leader-side AppendEntries dispatch does **not** flow through
//! `Action` either — see `RaftNode::replication` /
//! [`crate::replication::PeerReplication::get_append_range`] for the
//! direct-method pull path. The cluster owns the per-peer loop;
//! raft owns the per-peer state and decides what to ship.
//!
//! Voter-side RequestVote handling is also direct-method (see
//! `RaftNode::request_vote`); the only `Action` involved is the
//! candidate's outbound [`Action::SendRequestVote`] — the wire reply
//! comes back through `Event::RequestVoteReply`.

use std::time::Instant;

use crate::role::Role;
use crate::types::{NodeId, Term, TxId};

#[derive(Clone, Debug)]
pub enum Action {
    // ── outbound RPCs ───────────────────────────────────────────────────
    /// Outbound `RequestVote` from a candidate. The driver sends it
    /// over the wire to `to`, awaits the reply, and feeds the result
    /// back as `Event::RequestVoteReply`. The voter side is direct-
    /// method (`RaftNode::request_vote`), so there is no
    /// `SendRequestVoteReply` action — the driver builds the wire
    /// reply from the synchronous return value.
    SendRequestVote {
        to: NodeId,
        term: Term,
        last_tx_id: TxId,
        last_term: Term,
    },

    // ── role observability ──────────────────────────────────────────────
    /// Notify the driver of a role transition. Mirrors the public
    /// `role()` query for cases where the driver wants an event-driven
    /// notification (gRPC handler swaps, supervisor signals).
    BecomeRole { role: Role, term: Term },

    // ── timer management ────────────────────────────────────────────────
    /// Schedule the next `Event::Tick` at `at`. The library re-emits
    /// this on every transition that changes the soonest deadline.
    SetWakeup { at: Instant },

    // ── fatal failures ──────────────────────────────────────────────────
    /// Unrecoverable failure (e.g. a `Persistence` write returned
    /// `Err`). The library has frozen its state machine: subsequent
    /// `step()` calls return an empty action vec. The driver MUST
    /// shut the node down on receipt — continuing risks committing
    /// non-durable entries or violating §5.3 / §5.4 because the
    /// in-memory state and the on-disk state may have diverged.
    ///
    /// `reason` is a fixed `&'static str` from a closed set of failure
    /// sites; it is intended for logging, not for branching driver
    /// logic. Promote to `Box<str>` if dynamic context is ever needed.
    FatalError { reason: &'static str },
}
