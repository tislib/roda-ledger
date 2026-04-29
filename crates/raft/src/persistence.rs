//! Durable state contract for the raft library.
//!
//! The library does not perform I/O. Every durable write goes through
//! this trait. The driver supplies an implementation that wraps the
//! real `Term` / `Vote` files; tests supply an in-memory fake.
//!
//! ## Contract
//!
//! When a write method returns `Ok(_)`, the state change is durable
//! on the underlying medium. `commit_term`, `observe_term`,
//! `truncate_term_after`, `vote`, `observe_vote_term` all fsync (or
//! equivalent) before returning. The library relies on this to
//! maintain the invariant "raft's in-memory decisions reflect only
//! durably-persisted state."
//!
//! Read methods are cheap, side-effect-free queries. They are called
//! per-RPC (not per-instruction); no caching is required at the
//! library level.
//!
//! ## Atomicity boundary
//!
//! Writes are atomic at the level of a single method call. A crash
//! during `commit_term` either leaves the term log unchanged (call
//! returned `Err`) or contains the new record (call returned
//! `Ok(true)`).
//!
//! `truncate_term_after` rewrites the term log file. Implementations
//! MUST use rename-based replacement (write tmp file, fsync tmp,
//! rename, fsync parent dir) so a crash mid-truncate leaves the
//! original file intact.
//!
//! ## Term log vs vote log
//!
//! These are two separate durable streams. `current_term()` returns
//! the *term log's* current term — i.e. the term whose first entry
//! lives at `last_term_record().start_tx_id`. The vote log carries
//! its own per-term `voted_for` slot, kept in sync with the term log
//! via `observe_vote_term`. The library calls both `observe_term`
//! and `observe_vote_term` whenever it observes a higher term via
//! inbound RPC, so an implementation that only updates one is broken.

use std::io;

use crate::types::{NodeId, Term as TermNum, TxId};

/// One record in the term log: which term started at which `tx_id`.
/// Re-exported as a public type so trait implementors don't depend on
/// `storage::TermRecord` directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TermRecord {
    pub term: TermNum,
    pub start_tx_id: TxId,
}

/// Durable state every Raft node holds. Implementations are owned by
/// the driver and handed to `RaftNode::new`.
pub trait Persistence {
    // ── term log ────────────────────────────────────────────────────────

    /// Highest term currently recorded in the term log.
    fn current_term(&self) -> TermNum;

    /// Most recent term-log record, or `None` on a fresh install.
    fn last_term_record(&self) -> Option<TermRecord>;

    /// Term that covered `tx_id`: the record with the largest
    /// `start_tx_id <= tx_id`. `None` if `tx_id` predates every
    /// record (or the term log is empty).
    fn term_at_tx(&self, tx_id: TxId) -> Option<TermRecord>;

    /// Election-win commit. Atomically asserts `current_term + 1 ==
    /// expected` and persists the new boundary.
    ///
    /// - `Ok(true)`: write succeeded, `current_term` is now `expected`.
    /// - `Ok(false)`: `current_term` already advanced past `expected`
    ///   (a concurrent observer got there first); caller should treat
    ///   the election as lost.
    /// - `Err(InvalidInput)`: `expected` is more than one term ahead
    ///   of current (programming error).
    /// - `Err(_)`: I/O failure.
    fn commit_term(&mut self, expected: TermNum, start_tx_id: TxId) -> io::Result<bool>;

    /// Follower path: record a strictly-higher term observed via
    /// inbound RPC. Idempotent on equal term, rejects strict
    /// regressions with `InvalidInput`.
    fn observe_term(&mut self, term: TermNum, start_tx_id: TxId) -> io::Result<()>;

    /// Drop term records whose `start_tx_id > tx_id`. Atomic via
    /// rename-based file replacement. Pairs with the log-suffix
    /// truncation Raft §5.3 demands when a follower's log diverges.
    fn truncate_term_after(&mut self, tx_id: TxId) -> io::Result<()>;

    // ── vote log ────────────────────────────────────────────────────────

    /// Term currently recorded in the vote log. May lead the term
    /// log when a candidate has self-voted at term `N` but not yet
    /// won (term log advances only on election win, via `commit_term`).
    /// The library reads this together with `current_term()` to
    /// compute "what term am I in" — the candidate uses
    /// `max(current_term, vote_term) + 1` for the next election.
    fn vote_term(&self) -> TermNum;

    /// `Some(node_id)` iff a vote was granted in the current
    /// vote-log term, `None` otherwise.
    fn voted_for(&self) -> Option<NodeId>;

    /// Raft §5.4.1: durably grant a vote for `candidate_id` in `term`.
    ///
    /// - `Ok(true)`: granted (or idempotent re-grant of a same-candidate
    ///   vote in the same term).
    /// - `Ok(false)`: refused (already voted for someone else this
    ///   term, or `term < vote-log term`).
    /// - `Err(InvalidInput)`: `candidate_id == 0`.
    /// - `Err(_)`: I/O failure.
    fn vote(&mut self, term: TermNum, candidate_id: NodeId) -> io::Result<bool>;

    /// Record a strictly-higher term observed via inbound RPC,
    /// clearing the vote slot. Idempotent on equal term, rejects
    /// strict regressions with `InvalidInput`.
    fn observe_vote_term(&mut self, term: TermNum) -> io::Result<()>;
}
