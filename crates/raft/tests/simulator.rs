//! End-to-end scenarios for the deterministic raft simulator.
//!
//! ADR-0017 §"Test Strategy" — these are the "happy path elects, leader
//! crashes, partition heals" scenarios. The harness lives in
//! `tests/common/mod.rs`; safety property tests using `proptest` live in
//! `tests/safety_properties.rs`.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use raft::{NodeId, Role};

#[test]
fn three_node_cluster_elects_a_leader_and_replicates() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    let elected = sim.run_until_predicate(deadline, |s| {
        [1, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    assert!(elected, "no leader elected within 5s of virtual time");

    let leader = [1, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();

    sim.client_write(leader, 5);
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 5);

    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until(deadline);
    for n in [1, 2, 3] {
        assert!(
            sim.cluster_commit_of(n) >= 5,
            "node {} has cluster_commit {} (expected >= 5)",
            n,
            sim.cluster_commit_of(n)
        );
    }

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

#[test]
fn single_node_cluster_self_elects_and_commits() {
    let mut sim = Sim::new(&[1], Sim::standard_cfg(), 7);
    let deadline = Instant::now() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| s.role_of(1) == Role::Leader);
    assert_eq!(sim.role_of(1), Role::Leader);

    sim.client_write(1, 3);
    let deadline = sim.clock() + Duration::from_millis(500);
    sim.run_until(deadline);
    assert_eq!(sim.cluster_commit_of(1), 3);
    sim.assert_election_safety();
}

#[test]
fn leader_failover_after_crash() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 11);
    let deadline = Instant::now() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| {
        [1, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    let leader1 = [1, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();

    sim.client_write(leader1, 3);
    let deadline = sim.clock() + Duration::from_millis(500);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader1) >= 3);

    sim.crash(leader1);

    let survivors: Vec<NodeId> = [1, 2, 3]
        .iter()
        .copied()
        .filter(|n| *n != leader1)
        .collect();
    let deadline = sim.clock() + Duration::from_secs(3);
    let new_leader_elected = sim.run_until_predicate(deadline, |s| {
        survivors.iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    assert!(
        new_leader_elected,
        "no failover leader after crashing {}",
        leader1
    );

    let leader2 = survivors
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();
    assert_ne!(leader1, leader2);
    assert!(sim.current_term_of(leader2) > sim.current_term_of(leader1));

    sim.assert_election_safety();
    sim.assert_log_matching();
}

#[test]
fn partitioned_minority_does_not_disrupt_majority() {
    let mut sim = Sim::new(&[1, 2, 3, 4, 5], Sim::standard_cfg(), 23);
    let deadline = Instant::now() + Duration::from_secs(3);
    sim.run_until_predicate(deadline, |s| {
        [1, 2, 3, 4, 5]
            .iter()
            .any(|n| s.role_of(*n) == Role::Leader)
    });
    let leader = [1, 2, 3, 4, 5]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();

    sim.client_write(leader, 4);
    let deadline = sim.clock() + Duration::from_millis(500);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 4);

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
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 8);
    assert!(sim.cluster_commit_of(leader) >= 8);

    sim.assert_election_safety();
    sim.assert_log_matching();
}

#[test]
fn read_api_consistency_with_action_stream() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 41);
    let deadline = Instant::now() + Duration::from_secs(3);
    sim.run_until_predicate(deadline, |s| {
        [1, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    let leader = [1, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();
    let term_at_election = sim.current_term_of(leader);
    sim.client_write(leader, 2);
    let deadline = sim.clock() + Duration::from_millis(500);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 2);
    assert!(sim.role_of(leader).is_leader());
    assert!(sim.current_term_of(leader) >= term_at_election);
    assert_eq!(sim.local_commit_of(leader), 2);
    assert_eq!(sim.cluster_commit_of(leader), 2);
}

#[test]
fn rejoin_with_divergent_log_truncates_to_leader() {
    // Set up: 3-node cluster, partition node 3 from the others, let 1+2
    // continue to commit. When 3 rejoins, its empty/stale log must be
    // brought into agreement via §5.3 prev_log_term truncation +
    // backfill.
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 71);
    let deadline = Instant::now() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| {
        [1, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });
    let leader = [1, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
        .unwrap();

    // Identify a non-leader to partition.
    let outcast: NodeId = [1, 2, 3].iter().copied().find(|n| *n != leader).unwrap();
    sim.partition(outcast, leader);
    for n in [1, 2, 3] {
        if n != outcast && n != leader {
            sim.partition(outcast, n);
        }
    }

    // Leader replicates 4 entries to the remaining quorum.
    sim.client_write(leader, 4);
    let deadline = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(leader) >= 4);
    assert!(sim.cluster_commit_of(leader) >= 4);

    // Heal the partition. The outcast must catch up. ADR-0017 §"Non-Goals":
    // partitioned nodes that bumped term repeatedly disrupt the cluster on
    // rejoin (no pre-vote in v1), so convergence can require several
    // election rounds — give it plenty of virtual time.
    for n in [1, 2, 3] {
        if n != outcast {
            sim.heal_partition(outcast, n);
        }
    }
    let deadline = sim.clock() + Duration::from_secs(8);
    sim.run_until_predicate(deadline, |s| s.cluster_commit_of(outcast) >= 4);
    assert!(
        sim.cluster_commit_of(outcast) >= 4,
        "outcast {} did not catch up after rejoin (cluster_commit={}, term={})",
        outcast,
        sim.cluster_commit_of(outcast),
        sim.current_term_of(outcast)
    );

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}
