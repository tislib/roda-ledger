//! Single-node and degenerate-cluster behaviour. Tests run against
//! `RaftNode<MemPersistence>` directly (no simulator) where possible
//! — these are unit-scope checks on the state machine in isolation.

mod common;

use std::time::{Duration, Instant};

use common::mem_persistence::MemPersistence;
use raft::{Action, Event, RaftConfig, RaftNode, Role};

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
    let _ = node.step(t0, Event::Tick);
    assert_eq!(node.role(), Role::Initializing);
    let actions = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.current_term(), 1);
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::BecomeRole {
            role: Role::Leader,
            ..
        }
    )));
}

/// Single-node leader commits its own writes immediately — quorum is 1.
#[test]
fn single_node_leader_commits_own_writes_immediately() {
    let mut node = fresh(1, vec![1]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());

    let actions = node.step(
        t0 + Duration::from_secs(61),
        Event::LocalCommitAdvanced { tx_id: 5 },
    );
    assert_eq!(node.commit_index(), 5);
    assert_eq!(node.cluster_commit_index(), 5);
    let advances: Vec<u64> = actions
        .iter()
        .filter_map(|a| match a {
            Action::AdvanceClusterCommit { tx_id } => Some(*tx_id),
            _ => None,
        })
        .collect();
    assert_eq!(advances, vec![5]);
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
    let _ = node.step(now, Event::Tick);

    now += Duration::from_secs(60);
    let _ = node.step(now, Event::Tick);
    assert_eq!(node.role(), Role::Candidate);
    let term_after_first = node.current_term();
    assert!(term_after_first >= 1);

    now += Duration::from_secs(60);
    let _ = node.step(now, Event::Tick);
    let term_after_second = node.current_term();
    assert!(
        term_after_second > term_after_first,
        "expected term to monotonically advance through failed candidacies (was {}, now {})",
        term_after_first,
        term_after_second
    );
    assert!(!node.role().is_leader());
}
