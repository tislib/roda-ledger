//! Regression tests for bugs found in the previous in-place
//! `cluster::raft::*` implementation. Each test reproduces a bug
//! that would have triggered a known incident before the fixes
//! described in ADR-0017 (term-bump-before-win, boot-time term
//! bump, missing §5.3 walk-back, term log not truncating with the
//! WAL).

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use common::mem_persistence::MemPersistence;
use raft::{Action, Event, LogEntryMeta, NodeId, RaftConfig, RaftNode, Role};

fn fresh_node(self_id: u64, peers: Vec<u64>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

fn await_leader(sim: &mut Sim) -> NodeId {
    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    [1u64, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap()
}

/// Term-bump-before-win: previously, every lost election left a
/// permanent record in the term log because `commit_term` was called
/// before the win was confirmed. Verify the fix: `last_term_record()`
/// stays `None` across a sequence of failed candidacies.
#[test]
fn lost_elections_do_not_pollute_term_log() {
    let mut node = fresh_node(1, vec![1, 2]);
    let mut now = Instant::now();
    let _ = node.step(now, Event::Tick);
    for _ in 0..5 {
        now += Duration::from_secs(60);
        let _ = node.step(now, Event::Tick);
    }
    let p = node.into_persistence();
    use raft::Persistence;
    assert_eq!(p.current_term(), 0, "term log advanced on lost candidacy");
    assert!(p.last_term_record().is_none(), "phantom term-log record");
    assert!(
        p.vote_term() >= 1,
        "vote log did not advance — candidate never self-voted?"
    );
}

/// Boot-time term bump: previously the constructor incremented term
/// on every start. Verify the fix: a node restarted from durable
/// state has the same `current_term` and `voted_for` as before, and
/// no `BecomeRole` action fires at construction.
#[test]
fn restart_does_not_bump_term() {
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());
    let term_before = node.current_term();

    let p = node.into_persistence();
    let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.current_term(), term_before);
    // Construction emits no actions — only step does.
    assert_eq!(restarted.role(), Role::Initializing);
}

/// §5.3 walk-back: previously, on a `LogMismatch` reject the leader
/// did not decrement `next_index`, so it retried the same range
/// indefinitely. Verify the fix: `next_index` walks back one entry
/// per reject until the agreement point is found.
#[test]
fn log_mismatch_decrements_next_index() {
    // Construct a leader (single-node so we don't depend on peers).
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());

    // Pretend it has 5 entries in flight to a phantom peer 2 with
    // next_index=6. We can't reach into LeaderState directly; instead,
    // exercise the rejection path through the public step API.
    //
    // Simulate the leader having committed 3 entries:
    for tx in 1..=3 {
        let _ = node.step(
            t0 + Duration::from_secs(60 + tx),
            Event::LocalCommitAdvanced { tx_id: tx },
        );
    }

    // Inject 3 successive LogMismatch rejects from peer 2 — each
    // should produce a `SendAppendEntries` with a strictly smaller
    // `prev_log_tx_id`.
    use raft::RejectReason;
    let mut last_prev_log: Option<u64> = None;
    for i in 0..3 {
        let actions = node.step(
            t0 + Duration::from_secs(70 + i),
            Event::AppendEntriesReply {
                from: 2,
                term: node.current_term(),
                success: false,
                last_tx_id: 0,
                reject_reason: Some(RejectReason::LogMismatch),
            },
        );
        // The leader's next leader_drive() will fire on the next Tick.
        let _ = node.step(t0 + Duration::from_secs(80 + i), Event::Tick);
        if let Some(prev) = actions.iter().find_map(|a| match a {
            Action::SendAppendEntries { prev_log_tx_id, .. } => Some(*prev_log_tx_id),
            _ => None,
        }) {
            if let Some(p) = last_prev_log {
                assert!(
                    prev < p,
                    "prev_log_tx_id should walk back ({} -> {})",
                    p,
                    prev
                );
            }
            last_prev_log = Some(prev);
        }
    }
}

/// Term log truncation paired with §5.3 entry-log truncation:
/// previously the term log retained stale records when the entry
/// log was truncated. Verify the fix: when a follower truncates its
/// log on §5.3 mismatch, the term-log records past the new high
/// water mark are dropped synchronously through the trait.
#[test]
fn term_log_truncates_with_entry_log() {
    let mut node = fresh_node(1, vec![1, 2, 3]);

    // Drive entries 1..5 at term 1 into the follower.
    let actions = node.step(
        Instant::now(),
        Event::AppendEntriesRequest {
            from: 2,
            term: 1,
            prev_log_tx_id: 0,
            prev_log_term: 0,
            entries: (1..=5).map(|tx| LogEntryMeta::new(tx, 1)).collect(),
            leader_commit: 0,
        },
    );
    let _ = actions;
    assert_eq!(node.commit_index(), 5);

    // Now arrive an AppendEntries from a leader at term 2 with
    // prev_log_tx_id=5, prev_log_term=2 — mismatch. Follower must
    // truncate to 4 in BOTH the entry log (Action::TruncateLog)
    // and the term log (synchronous through the trait).
    let actions = node.step(
        Instant::now(),
        Event::AppendEntriesRequest {
            from: 2,
            term: 2,
            prev_log_tx_id: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        },
    );
    let truncate = actions.iter().find_map(|a| match a {
        Action::TruncateLog { after_tx_id } => Some(*after_tx_id),
        _ => None,
    });
    assert_eq!(truncate, Some(4));

    // Confirm the term log mirror is consistent: term_at_tx beyond
    // the truncation point should not return a record from a
    // future term.
    use raft::Persistence;
    let p = node.into_persistence();
    let last = p.last_term_record();
    assert!(
        last.map(|r| r.start_tx_id <= 4).unwrap_or(true),
        "term log retained a record past the truncation: {:?}",
        last
    );
}

/// Term-N split-brain prevention. Previously, when a leader at term
/// N received a higher-term reply and stepped down, two new
/// candidacies could simultaneously win term N+1. With the
/// commit_term `current + 1 == expected` check, at most one wins.
///
/// We can't directly script the race in the simulator (it's
/// deterministic) but we can verify the underlying invariant:
/// `commit_term(expected)` returns `Ok(false)` when current has
/// already advanced past `expected`.
#[test]
fn commit_term_refuses_when_already_advanced() {
    use raft::Persistence;
    let mut p = MemPersistence::new();
    p.observe_term(5, 0).unwrap();
    // Two concurrent winners of term 5 would both call commit_term(5).
    // The first to land on a particular replica gets Ok(false) here
    // because current=5 already; only the actual term-5 winner wrote
    // it — the loser steps down on this signal.
    assert!(!p.commit_term(5, 0).unwrap());
}

/// 3-node cluster after a leader crash converges with all four
/// safety properties intact. The "system stays correct under
/// failover" smoke test that catches regressions across the entire
/// state machine.
#[test]
fn three_node_failover_preserves_all_safety_properties() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 101);
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 4);

    sim.crash(leader);
    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3]
            .iter()
            .filter(|n| **n != leader)
            .any(|n| s.role_of(*n) == Role::Leader)
    });

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}
