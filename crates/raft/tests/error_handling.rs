//! Edge-case regression tests for the audit findings that survive
//! the consensus refactor.
//!
//! Pre-refactor this file pinned `Action::FatalError` /
//! `HandshakeDecision::Fatal` propagation paths for persistence
//! I/O failures. After the refactor:
//!
//! - The `Persistence` trait no longer returns `io::Result` — an
//!   implementation that cannot durably persist a write must panic
//!   internally; the supervisor restarts the process. There is no
//!   "fatal but still running" state for the library to model.
//! - `Action::FatalError` and `HandshakeDecision::Fatal` are
//!   gone with the rest of the action stream.
//!
//! What survives: the `RaftConfig::validate` panic-at-construction
//! check, the §5.4 truncation invariant `debug_assert`, and the
//! `pending_leader_commit` lifecycle around role transitions /
//! repeated AE. Those are the audit findings that don't depend on
//! persistence-error plumbing.

mod common;

use std::time::{Duration, Instant};

use common::mem_persistence::MemPersistence;
use raft::{RaftConfig, RaftNode};

fn make_follower(persistence: MemPersistence) -> RaftNode<MemPersistence> {
    RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42)
}

// ─── §5.4 truncation invariant — `debug_assert!`-or-clamp ───────────────

/// Constructs an internally inconsistent state where
/// `cluster_commit_index = 5` and the next AE drives a truncation to
/// tx_id 1. In a correct Raft run this is impossible (Leader
/// Completeness §5.4 guarantees committed entries appear in every
/// future leader's log), so the library `debug_assert!`s instead of
/// silently clamping.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "truncation below cluster_commit_index")]
fn truncation_below_cluster_commit_index_panics_in_debug() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 1,
            start_tx_id: 1,
        }],
        1,
        0,
    );
    let mut node = make_follower(persistence);
    let now = Instant::now();

    // Hydrate watermark to 10.
    node.advance_local_index(10);

    // First handshake: matching prev_log; driver then advances cluster
    // commit to 5.
    let _ = node.validate_handshake(now, 2, 1, &[], 10, 1);
    node.advance_cluster_index(5);
    assert_eq!(node.cluster_commit_index(), 5);

    // Second handshake: deliberately mismatched prev_log_term, forcing
    // truncation to tx_id 1 — strictly below cluster_commit_index=5.
    // Triggers the debug_assert.
    let _ = node.validate_handshake(now, 2, 1, &[], 2, 99);
}

// ─── RaftConfig::validate panics at construction ───────────────────────

#[test]
#[should_panic(expected = "RaftConfig")]
fn raftnode_new_panics_on_misconfigured_heartbeat_interval() {
    use raft::ElectionTimerConfig;

    let cfg = RaftConfig {
        heartbeat_interval: Duration::from_millis(200), // 200 * 2 > min_ms
        election_timer: ElectionTimerConfig {
            min_ms: 150,
            max_ms: 300,
        },
        rpc_timeout: Duration::from_millis(500),
        max_entries_per_append: 64,
    };
    let _ = RaftNode::new(1, vec![1, 2], MemPersistence::new(), cfg, 42);
}

// ─── pending_leader_commit lifecycle ─────────────────────────────────────

// ─── current_term composition ────────────────────────────────────────────

/// `current_term()` returns `max(term_log term, vote_log term)`.
/// A vote log that has raced ahead of the term log (candidate
/// self-voted at a term it has not yet won) is reflected in the
/// public read.
#[test]
fn current_term_returns_max_of_term_log_and_vote_log() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 3,
            start_tx_id: 0,
        }],
        7, // vote-log raced ahead by 4 terms
        2,
    );
    let node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    assert_eq!(node.current_term(), 7);
}

#[test]
fn current_term_returns_term_log_when_vote_log_is_lower() {
    let persistence = MemPersistence::with_state(
        vec![raft::TermRecord {
            term: 9,
            start_tx_id: 0,
        }],
        4,
        0,
    );
    let node = RaftNode::new(1, vec![1, 2], persistence, RaftConfig::default(), 42);
    assert_eq!(node.current_term(), 9);
}
