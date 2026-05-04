//! Single-node and degenerate-cluster behaviour. Tests run against
//! `RaftNode<MemPersistence>` directly (no simulator) where possible
//! — these are unit-scope checks on the state machine in isolation.

mod common;

use std::time::{Duration, Instant};

use common::mem_persistence::MemPersistence;
use raft::{RaftConfig, RaftNode, Role};

fn fresh(self_id: u64, peers: Vec<u64>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

/// `peers` must contain `self_id`; an empty list trivially fails
/// that check.
#[test]
#[should_panic(expected = "peers list must contain self_id")]
fn construction_with_empty_peer_list_panics() {
    let _ = RaftNode::new(
        1,
        Vec::<u64>::new(),
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    );
}

/// Single-node cluster self-elects on the first tick that crosses
/// the election timeout window.
#[test]
fn single_node_self_elects_on_first_timeout_tick() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    assert_eq!(node.role(), Role::Initializing);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.current_term(), 1);
}

/// Single-node leader commits its own writes immediately — quorum is 1.
#[test]
fn single_node_leader_commits_own_writes_immediately() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());

    // `advance` is silent; observe the cluster_commit advance via
    // the getter (ADR-0017 §"Driver call pattern").
    node.advance_write_index(5);
    node.advance_commit_index(5);
    assert_eq!(node.commit_index(), 5);
    assert_eq!(node.write_index(), 5);
    assert_eq!(node.cluster_commit_index(), 5);
}

/// Without any events, a node makes no transitions — the library
/// does not measure time internally.
#[test]
fn no_events_means_no_transitions() {
    let node = fresh(1, vec![1, 2, 3]);
    assert_eq!(node.role(), Role::Initializing);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.cluster_commit_index(), 0);
    assert_eq!(node.voted_for(), None);
}

/// Two-node cluster: a candidate that gets no peer votes stays
/// Candidate and bumps term on every election timeout.
#[test]
fn two_node_cluster_without_peer_replies_keeps_bumping_term() {
    let mut node = fresh(1, vec![1, 2]);
    let mut now = Instant::now();
    common::drive_tick(&mut node, now);

    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    assert_eq!(node.role(), Role::Candidate);
    let term_after_first = node.current_term();
    assert!(term_after_first >= 1);

    now += Duration::from_secs(60);
    common::drive_tick(&mut node, now);
    let term_after_second = node.current_term();
    assert!(
        term_after_second > term_after_first,
        "expected term to monotonically advance through failed candidacies (was {}, now {})",
        term_after_first,
        term_after_second
    );
    assert!(!node.role().is_leader());
}

// ── advance() behavior tests (split-watermark refactor) ────────────────

/// `advance_*` is monotonic — a regressed argument is silently
/// ignored. Mirrors `Quorum`'s monotonicity invariant for
/// `cluster_commit_index`.
#[test]
fn advance_is_monotonic() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());

    // Write monotonicity: regressed write argument is ignored. Tested
    // before any commit advance so the new `advance_write_index`
    // debug_assert (`local_commit_index <= local_write_index`) holds.
    node.advance_write_index(5);
    assert_eq!(node.write_index(), 5);
    node.advance_write_index(2);
    assert_eq!(node.write_index(), 5, "write must not regress");

    // Commit monotonicity: regressed commit argument is ignored.
    node.advance_commit_index(3);
    assert_eq!(node.commit_index(), 3);
    node.advance_commit_index(1);
    assert_eq!(node.commit_index(), 3, "commit must not regress");
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "advance: commit_index=5 > write_index=3")]
fn advance_write_panics_below_commit_in_debug() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    // Drive write/commit to 5, then attempt to regress write below
    // commit — `advance_write_index` must panic on the invariant
    // `local_commit_index <= local_write_index`.
    node.advance_write_index(5);
    node.advance_commit_index(5);
    node.advance_write_index(3);
}

/// `advance` only updates `cluster_commit_index` on a leader. On a
/// non-leader (Initializing / Follower / Candidate) the watermark
/// stays put — only the local fields move.
#[test]
fn advance_updates_cluster_commit_on_leader_only() {
    // Initializing follower: cluster_commit must NOT advance.
    let mut follower = fresh(1, vec![1, 2, 3]);
    follower.advance_write_index(5);
    follower.advance_commit_index(5);
    assert_eq!(follower.write_index(), 5);
    assert_eq!(follower.commit_index(), 5);
    assert_eq!(
        follower.cluster_commit_index(),
        0,
        "non-leader advance must not lift cluster_commit"
    );

    // Single-node leader: cluster_commit lifts to local_commit
    // immediately (quorum=1, no peers to wait on).
    let mut leader = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut leader, t0);
    common::drive_tick(&mut leader, t0 + Duration::from_secs(60));
    assert!(leader.role().is_leader());
    leader.advance_write_index(5);
    leader.advance_commit_index(5);
    assert_eq!(leader.cluster_commit_index(), 5);
}

/// `advance` is a pure state mutator — it returns nothing and changes
/// no future timer behaviour beyond what role/quorum already require.
/// The cluster_commit advance is observed via the getter (no
/// dedicated action; ADR-0017 §"Driver call pattern").
#[test]
fn advance_returns_unit() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());

    // The method is `() -> ()` — no actions returned. We assert on
    // the post-condition observable via getters.
    node.advance_write_index(5);
    node.advance_commit_index(5);
    assert_eq!(node.cluster_commit_index(), 5);
}
