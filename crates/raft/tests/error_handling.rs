//! Edge-case regression tests for the audit findings that survive
//! the consensus refactor.
//!
//! Pre-refactor this file pinned `Action::FatalError` /
//! `AppendEntriesDecision::Fatal` propagation paths for persistence
//! I/O failures. After the refactor:
//!
//! - The `Persistence` trait no longer returns `io::Result` — an
//!   implementation that cannot durably persist a write must panic
//!   internally; the supervisor restarts the process. There is no
//!   "fatal but still running" state for the library to model.
//! - `Action::FatalError` and `AppendEntriesDecision::Fatal` are
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
use raft::{AppendEntriesDecision, LogEntryRange, RaftConfig, RaftNode};

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

    // Hydrate watermarks to 10.
    node.advance_write_index(10);
    node.advance_commit_index(10);

    // First AE: matching prev_log to advance cluster_commit_index to 5.
    let _ = node.validate_append_entries_request(now, 2, 1, 10, 1, LogEntryRange::empty(), 5);
    assert_eq!(node.cluster_commit_index(), 5);

    // Second AE: deliberately mismatched prev_log_term, forcing
    // truncation to tx_id 1 — strictly below cluster_commit_index=5.
    // Triggers the debug_assert.
    let _ = node.validate_append_entries_request(now, 2, 1, 2, 99, LogEntryRange::empty(), 5);
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

/// A `Follower → Candidate` transition (election timeout fires while
/// the follower's last-validated AE still holds a `pending_leader_commit`
/// awaiting durability) discards the staged value along with the rest
/// of the FollowerState. The cluster's eventual `advance` MUST NOT
/// bump `cluster_commit_index` from the stale leader_commit — there is
/// no follower state to consult anymore.
#[test]
fn pending_leader_commit_lost_on_follower_to_candidate_transition() {
    let mut node = make_follower(MemPersistence::new());
    let now = Instant::now();

    // First validate stages `leader_commit=3` waiting on durability.
    let decision =
        node.validate_append_entries_request(now, 2, 1, 0, 0, LogEntryRange::new(1, 3, 1), 3);
    assert_eq!(
        decision,
        AppendEntriesDecision::Accept {
            append: Some(LogEntryRange::new(1, 3, 1)),
        }
    );
    assert_eq!(node.cluster_commit_index(), 0);

    // Election timeout fires → Follower → Candidate. The transition
    // discards FollowerState including `pending_leader_commit`.
    common::drive_tick(&mut node, now + Duration::from_secs(60));
    assert_eq!(node.role(), raft::Role::Candidate);

    // Cluster (which does not know about the role change) finally
    // reports durability. cluster_commit must stay put — no stale
    // follower state to drain.
    node.advance_write_index(3);
    node.advance_commit_index(3);
    assert_eq!(
        node.cluster_commit_index(),
        0,
        "candidate must not pick up the stale leader_commit"
    );
}

/// Two `validate_append_entries_request` calls arrive in rapid
/// succession before the cluster acks durability via `advance`. The
/// second `leader_commit` overwrites the first.
#[test]
fn pending_leader_commit_overwritten_when_second_ae_arrives_before_advance() {
    let mut node = make_follower(MemPersistence::new());
    let now = Instant::now();

    // First AE: prev=0, entries 1..3, leader_commit=2.
    let _ = node.validate_append_entries_request(now, 2, 1, 0, 0, LogEntryRange::new(1, 3, 1), 2);

    // Second AE before any advance: same prev=0 but an extended
    // range 1..5, leader_commit=5.
    let _ = node.validate_append_entries_request(now, 2, 1, 0, 0, LogEntryRange::new(1, 5, 1), 5);

    // Cluster acks durability up to 5. Drain inside `advance` picks
    // up the second AE's leader_commit (5), not the first's (2).
    node.advance_write_index(5);
    node.advance_commit_index(5);
    assert_eq!(
        node.cluster_commit_index(),
        5,
        "second AE's leader_commit must overwrite the first"
    );
}

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
