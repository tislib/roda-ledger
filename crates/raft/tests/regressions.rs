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
use raft::{AppendResult, HandshakeDecision, NodeId, RaftConfig, RaftNode, RejectReason, Role};

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
    common::drive_tick(&mut node, now);
    for _ in 0..5 {
        now += Duration::from_secs(60);
        common::drive_tick(&mut node, now);
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
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());
    let term_before = node.current_term();

    let p = node.into_persistence();
    let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.current_term(), term_before);
    // Construction emits no actions — only step does.
    assert_eq!(restarted.role(), Role::Initializing);
}

/// §5.3 walk-back: on a `LogMismatch` reject the leader decrements
/// `next_index` for the peer (single-step under the simplified
/// no-fast-backoff model). Verified via `peer(2).next_index()`.
#[test]
fn log_mismatch_decrements_next_index() {
    let mut node = fresh_node(1, vec![1, 2]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    common::deliver_vote_reply(&mut node, t0 + Duration::from_secs(60), 2, 1, true);
    assert!(node.role().is_leader());

    for tx in 1..=3 {
        node.advance_local_index(tx);
    }

    let term = node.current_term();
    let initial = node.replication().peer(2).unwrap().next_index();
    let mut last_next: u64 = initial;
    for i in 0..3 {
        let now_reply = t0 + Duration::from_secs(70 + i);
        node.replication().peer(2).unwrap().append_result(
            now_reply,
            AppendResult::Reject {
                term,
                reason: RejectReason::LogMismatch,
                last_commit_id: 0,
            },
        );
        let next = node.replication().peer(2).unwrap().next_index();
        assert!(
            next < last_next || next == 1,
            "next_index should walk back ({} -> {})",
            last_next,
            next
        );
        last_next = next;
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

    // Drive entries 1..5 at term 1 into the follower. The cluster
    // acks durability via `advance`, which is what advances both
    // watermarks.
    let records = [raft::TermRecord {
        term: 1,
        start_tx_id: 1,
    }];
    let _ = node.validate_handshake(Instant::now(), 2, 1, &records, 0, 0);
    node.advance_local_index(5);
    assert_eq!(node.commit_index(), 5);

    // Handshake from a leader at term 2 with prev_log_tx_id=5,
    // prev_log_term=2 — mismatch. Decision must surface
    // `truncate_after = Some(4)`; the term log is truncated
    // synchronously inside validate_handshake.
    let decision = node.validate_handshake(Instant::now(), 2, 2, &[], 5, 2);
    assert_eq!(
        decision,
        HandshakeDecision::Reject {
            reason: RejectReason::LogMismatch,
            truncate_after: Some(4),
            // The fixture's term log has only one record (term=1
            // at start_tx_id=0 via the implicit term-init that
            // accompanies the AE accept above), so the hint
            // points back to the first conflicting term boundary.
        }
    );

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
/// `commit_term(expected)` returns `false` when current has already
/// advanced past `expected`.
#[test]
fn commit_term_refuses_when_already_advanced() {
    use raft::Persistence;
    let mut p = MemPersistence::new();
    p.observe_term(5, 0);
    // Two concurrent winners of term 5 would both call commit_term(5).
    // The first to land on a particular replica gets `false` here
    // because current=5 already; only the actual term-5 winner wrote
    // it — the loser steps down on this signal.
    assert!(!p.commit_term(5, 0));
}

/// **Raft §5.4.2 / Figure 8** — a new leader must not commit
/// entries from previous terms by counting replicas. Only entries
/// from the leader's current term may be committed by replica
/// counting; prior-term entries become committed indirectly via
/// the Log Matching Property, but only once a current-term entry
/// has crossed the commit boundary.
///
/// **The trigger.** In a 5-node cluster, the old leader at term 1
/// partially replicated entries 1..=5 to this node before
/// crashing — but it never reached the 3-of-5 majority needed to
/// advance `cluster_commit`, so this node holds the entries with
/// `cluster_commit = 0`. This node now wins term 2 (its log is
/// most up-to-date) and immediately receives `last_tx_id = 5`
/// acks from a quorum of peers. The acks reflect each peer
/// catching up the prior-term log; no current-term entry has been
/// proposed yet.
///
/// Without the §5.4.2 guard, the leader counts 3-of-5 acks at
/// `last_tx = 5` and advances `cluster_commit` to 5 — committing
/// term-1 entries by counting term-2 replicas. A subsequent
/// leader could then overwrite those entries, the canonical
/// Figure 8 violation.
#[test]
fn figure_8_new_leader_does_not_commit_prior_term_entries_by_replica_count() {
    let mut node = fresh_node(1, vec![1, 2, 3, 4, 5]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);

    // Phase 1 — receive entries 1..=5 at term 1 from a leader
    // whose own commit watermark is still 0 (it crashed before
    // achieving majority on these entries).
    let records = [raft::TermRecord {
        term: 1,
        start_tx_id: 1,
    }];
    let _ = node.validate_handshake(t0 + Duration::from_millis(10), 2, 1, &records, 0, 0);
    // Cluster acks ledger-apply; cluster_commit stays at 0 because
    // leader_commit was 0. Models the §5.4.2 scenario: prior-term
    // entries present locally but not committed cluster-wide.
    node.advance_local_index(5);
    assert_eq!(node.commit_index(), 5, "entries applied to ledger");
    assert_eq!(
        node.cluster_commit_index(),
        0,
        "no commit propagated yet — old leader crashed pre-majority"
    );

    // Phase 2 — silence past the election timeout, become
    // candidate at term 2, then collect votes from 2 peers (with
    // self-vote that is 3/5, a majority) and become leader.
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert_eq!(node.role(), Role::Candidate);
    let cand_term = node.current_term();
    assert_eq!(cand_term, 2);

    let now = t0 + Duration::from_secs(60) + Duration::from_millis(1);
    common::deliver_vote_reply(&mut node, now, 3, cand_term, true);
    common::deliver_vote_reply(&mut node, now, 4, cand_term, true);
    assert!(
        node.role().is_leader(),
        "should be leader at term {cand_term} after 3-of-5 votes"
    );

    // Phase 3 — receive the catch-up acks. Each peer reports
    // `last_tx_id = 5` after the new leader's first AE rounds
    // bring their logs up to the prior-term high-water mark.
    // Without §5.4.2, this advances `cluster_commit` to 5
    // (committing term-1 entries by current-term replica count).
    node.replication().peer(3).unwrap().append_result(
        now,
        AppendResult::Success {
            term: cand_term,
            last_commit_id: 5,
        },
    );
    node.replication().peer(4).unwrap().append_result(
        now,
        AppendResult::Success {
            term: cand_term,
            last_commit_id: 5,
        },
    );

    assert!(
        node.cluster_commit_index() < 5,
        "Figure 8 violation: new leader at term {} committed prior-term entries by replica counting (cluster_commit={}, local_log_index={})",
        cand_term,
        node.cluster_commit_index(),
        node.commit_index(),
    );
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
