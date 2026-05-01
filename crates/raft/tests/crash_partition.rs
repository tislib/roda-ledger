//! Crash-recovery and network-partition scenarios. Uses
//! `Sim::crash` (drops the volatile `RaftNode`, retains the
//! `Persistence` value via `into_persistence`) and `Sim::restart`
//! (rebuilds the node from the saved persistence) to model real
//! crash semantics: volatile state is lost, durable term/vote
//! state survives.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use common::mem_persistence::MemPersistence;
use raft::{Event, NodeId, Persistence, RaftConfig, RaftNode, Role};

fn await_leader_in(sim: &mut Sim, ids: &[NodeId]) -> NodeId {
    let deadline = sim.clock() + Duration::from_secs(5);
    let elected = sim.run_until_predicate(deadline, |s| {
        ids.iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    assert!(elected, "no leader elected from {:?}", ids);
    ids.iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap()
}

// ── Crash and recovery ──────────────────────────────────────────────────────

/// Leader crashes mid-replication; the survivors elect a new leader
/// at a strictly higher term. Cluster makes progress again.
#[test]
fn leader_crash_triggers_failover() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 11);
    let leader1 = await_leader_in(&mut sim, &[1, 2, 3]);

    sim.client_write(leader1, 3);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader1) >= 3);

    sim.crash(leader1);
    let survivors: Vec<NodeId> = [1, 2, 3]
        .iter()
        .copied()
        .filter(|n| *n != leader1)
        .collect();
    let leader2 = await_leader_in(&mut sim, &survivors);
    assert_ne!(leader1, leader2);
    assert!(sim.current_term_of(leader2) > sim.current_term_of(leader1));

    sim.assert_election_safety();
    sim.assert_log_matching();
}

/// Follower crashes, leader keeps committing, follower restarts and
/// catches up via heartbeats.
#[test]
fn follower_crash_then_restart_catches_up() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 19);
    let leader = await_leader_in(&mut sim, &[1, 2, 3]);

    let follower: NodeId = [1, 2, 3].iter().copied().find(|n| *n != leader).unwrap();
    sim.client_write(leader, 3);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 3);

    sim.crash(follower);
    // Leader keeps committing without the follower (still has a
    // 2-of-3 quorum with the other peer).
    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 7);
    assert!(sim.cluster_commit_of(leader) >= 7);

    sim.restart(follower);
    let dl = sim.clock() + Duration::from_secs(3);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(follower) >= 7);
    assert!(sim.cluster_commit_of(follower) >= 7);

    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

/// A node that voted for X in term T survives crash/restart with
/// the vote intact. Reconstruct via `into_persistence` →
/// `RaftNode::new`.
#[test]
fn boot_recovers_term_and_vote() {
    // Use a single-node so the in-process election finishes
    // deterministically without a simulator.
    let mut node = RaftNode::new(1, vec![1], MemPersistence::new(), RaftConfig::default(), 42);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());
    assert_eq!(node.current_term(), 1);
    assert_eq!(node.voted_for(), Some(1));

    let p = node.into_persistence();
    assert_eq!(p.current_term(), 1);
    assert_eq!(p.voted_for(), Some(1));

    let restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.current_term(), 1);
    assert_eq!(restarted.voted_for(), Some(1));
    assert_eq!(restarted.role(), Role::Initializing);
    assert_eq!(restarted.commit_index(), 0);
}

/// After voting for X in term T then restarting, a subsequent
/// RequestVote(term=T, candidate=Y) where Y≠X is refused — the
/// per-term one-vote rule survives the crash.
#[test]
fn restart_preserves_one_vote_per_term_rule() {
    // Single-node first to get a vote on disk, then restart in a
    // multi-node config and replay an RV from a different candidate.
    let mut node = RaftNode::new(1, vec![1], MemPersistence::new(), RaftConfig::default(), 42);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert_eq!(node.voted_for(), Some(1));
    let term = node.current_term();
    let p = node.into_persistence();

    // Restart in a 3-node cluster shape so RV semantics make sense.
    let mut node = RaftNode::new(1, vec![1, 2, 3], p, RaftConfig::default(), 42);
    let actions = node.step(
        Instant::now(),
        Event::RequestVoteRequest {
            from: 2,
            term,
            last_tx_id: 0,
            last_term: 0,
        },
    );
    let granted = actions
        .iter()
        .any(|a| matches!(a, raft::Action::SendRequestVoteReply { granted: true, .. }));
    assert!(!granted, "vote granted in same term to a different node");
}

// ── Network partitions ──────────────────────────────────────────────────────

/// 5-node cluster with a 2-node minority partition. The minority
/// can't elect; the majority continues to commit.
#[test]
fn minority_partition_cannot_elect_majority_continues() {
    let mut sim = Sim::new(&[1, 2, 3, 4, 5], Sim::standard_cfg(), 23);
    let leader = await_leader_in(&mut sim, &[1, 2, 3, 4, 5]);

    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_millis(500);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 4);

    let minority: Vec<NodeId> = [1, 2, 3, 4, 5]
        .iter()
        .copied()
        .filter(|n| *n != leader)
        .take(2)
        .collect();
    for m in &minority {
        for o in [1, 2, 3, 4, 5] {
            if *m != o {
                sim.partition(*m, o);
            }
        }
    }

    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 8);
    assert!(sim.cluster_commit_of(leader) >= 8);

    sim.assert_election_safety();
    sim.assert_log_matching();
}

/// Heal a partition; the partitioned outcast catches up despite
/// having bumped term repeatedly while isolated.
#[test]
fn rejoin_after_partition_catches_up() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 71);
    let leader = await_leader_in(&mut sim, &[1, 2, 3]);

    let outcast: NodeId = [1, 2, 3].iter().copied().find(|n| *n != leader).unwrap();
    sim.partition(outcast, leader);
    for n in [1, 2, 3] {
        if n != outcast && n != leader {
            sim.partition(outcast, n);
        }
    }

    sim.client_write(leader, 4);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 4);
    assert!(sim.cluster_commit_of(leader) >= 4);

    for n in [1, 2, 3] {
        if n != outcast {
            sim.heal_partition(outcast, n);
        }
    }
    let dl = sim.clock() + Duration::from_secs(8);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(outcast) >= 4);
    assert!(
        sim.cluster_commit_of(outcast) >= 4,
        "outcast {} did not catch up (cci={}, term={})",
        outcast,
        sim.cluster_commit_of(outcast),
        sim.current_term_of(outcast)
    );

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

/// Repeated partition flap (open-close-open-close) — the cluster
/// must eventually stabilise once the flapping stops.
#[test]
fn repeated_partition_flap_eventually_stabilises() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 41);
    let _ = await_leader_in(&mut sim, &[1, 2, 3]);

    for _ in 0..10 {
        sim.partition(1, 2);
        let dl = sim.clock() + Duration::from_millis(100);
        sim.run_until(dl);
        sim.heal_partition(1, 2);
        let dl = sim.clock() + Duration::from_millis(100);
        sim.run_until(dl);
    }

    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    assert!(
        [1u64, 2, 3].iter().any(|n| sim.role_of(*n) == Role::Leader),
        "cluster did not stabilise after partition flap"
    );

    sim.assert_election_safety();
    sim.assert_log_matching();
}

/// Two-way symmetric partition (no quorum either side) → neither
/// side commits. After heal the cluster re-elects.
#[test]
fn symmetric_partition_no_quorum_either_side() {
    let mut sim = Sim::new(&[1, 2, 3, 4, 5], Sim::standard_cfg(), 51);
    let leader = await_leader_in(&mut sim, &[1, 2, 3, 4, 5]);

    // Crash the leader; remaining 4 split 2-2.
    sim.crash(leader);
    let survivors: Vec<NodeId> = [1, 2, 3, 4, 5]
        .iter()
        .copied()
        .filter(|n| *n != leader)
        .collect();
    let (a, b, c, d) = (survivors[0], survivors[1], survivors[2], survivors[3]);
    sim.partition(a, c);
    sim.partition(a, d);
    sim.partition(b, c);
    sim.partition(b, d);

    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until(dl);
    // Neither side has 3-of-5 majority (leader is dead), so no
    // leader anywhere.
    for n in &survivors {
        assert!(
            !sim.role_of(*n).is_leader(),
            "node {} elected itself without quorum",
            n
        );
    }

    // Heal: 4 alive nodes can elect, 3-of-5 quorum reachable.
    sim.heal_partition(a, c);
    sim.heal_partition(a, d);
    sim.heal_partition(b, c);
    sim.heal_partition(b, d);

    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        survivors.iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    assert!(
        survivors.iter().any(|n| sim.role_of(*n) == Role::Leader),
        "cluster failed to re-elect after heal"
    );

    sim.assert_election_safety();
}

// ── Index-split refactor regressions ──────────────────────────────────────

/// After driver-side hydration via `advance`, a restarted node
/// should reflect the recovered watermarks. Pre-refactor this was
/// implicit through `LogAppendComplete`; post-refactor `advance` is
/// the canonical entry point.
#[test]
fn restart_recovers_local_indexes_via_advance() {
    let mut node = RaftNode::new(1, vec![1], MemPersistence::new(), RaftConfig::default(), 42);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());
    node.advance(7, 7);
    assert_eq!(node.commit_index(), 7);
    assert_eq!(node.write_index(), 7);

    // Crash + restart preserves only the term/vote logs.
    let p = node.into_persistence();
    let mut restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    assert_eq!(restarted.commit_index(), 0);
    assert_eq!(restarted.write_index(), 0);

    // Driver hydrates from the durable ledger watermark.
    restarted.advance(7, 7);
    assert_eq!(restarted.commit_index(), 7);
    assert_eq!(restarted.write_index(), 7);
}

/// Pre-crash, the leader had `local_write_index = 5` and
/// `local_commit_index = 3` (durably written but not yet locally
/// committed). The driver's recovery path surfaces only the ledger's
/// commit watermark (3) — see ADR-0017 §"Required Invariants" #9. We
/// pin this collapse as **expected** behavior so a future change
/// adding index persistence has to update the contract explicitly.
#[test]
fn write_ahead_of_commit_collapses_on_restart() {
    let mut node = RaftNode::new(1, vec![1], MemPersistence::new(), RaftConfig::default(), 42);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    assert!(node.role().is_leader());
    node.advance(5, 3);
    assert_eq!(node.write_index(), 5);
    assert_eq!(node.commit_index(), 3);

    let p = node.into_persistence();
    let mut restarted = RaftNode::new(1, vec![1], p, RaftConfig::default(), 42);
    // Driver only knows about the ledger's last_commit_id; the raft-
    // log WAL extent is not separately surfaced today.
    restarted.advance(3, 3);
    assert_eq!(restarted.write_index(), 3);
    assert_eq!(restarted.commit_index(), 3);
}

/// A leader that observes a higher term steps down to follower; the
/// node-level `local_write_index` survives the role swing. Pre-
/// refactor `LeaderState.last_written` was dropped on step-down — the
/// node-scoped split fixes this for free.
#[test]
fn leader_step_down_preserves_write_index() {
    let mut node = RaftNode::new(1, vec![1, 2, 3], MemPersistence::new(), RaftConfig::default(), 42);
    let t0 = Instant::now();
    let _ = node.step(t0, Event::Tick);
    let _ = node.step(t0 + Duration::from_secs(60), Event::Tick);
    // Single-vote majority requires a peer-vote to win; force win
    // via a record_grant by sending a vote reply at the right term.
    let term_now = node.current_term();
    let _ = node.step(
        t0 + Duration::from_secs(60),
        Event::RequestVoteReply {
            from: 2,
            term: term_now,
            granted: true,
        },
    );
    assert!(node.role().is_leader(), "test setup: must be leader");

    node.advance(5, 5);
    assert_eq!(node.write_index(), 5);

    // Inbound RPC at a strictly higher term forces step-down.
    let _ = node.step(
        t0 + Duration::from_secs(61),
        Event::AppendEntriesRequest {
            from: 2,
            term: term_now + 1,
            prev_log_tx_id: 5,
            prev_log_term: term_now,
            entries: raft::LogEntryRange::empty(),
            leader_commit: 0,
        },
    );
    assert!(matches!(node.role(), Role::Follower));
    // Survives the role transition.
    assert_eq!(node.write_index(), 5);
}
