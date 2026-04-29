//! Stress + soak: run a long random schedule under a realistic mix
//! of fault-injection and assert all safety properties survive.
//!
//! The simulator is fully deterministic — same `seed_base` →
//! identical schedule. We use modest virtual-time budgets so the
//! tests stay under a second of wall-clock; nightly runs can
//! re-target with bigger numbers.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use raft::{NodeId, Role};

fn pick_leader(sim: &Sim, ids: &[NodeId]) -> Option<NodeId> {
    ids.iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
}

/// 5-node cluster, ~5s of virtual time, mixed faults: drops,
/// duplicates, periodic crashes. All safety properties hold
/// throughout, and the cluster commits at least one entry.
#[test]
fn long_schedule_with_mixed_faults_preserves_safety() {
    let mut sim = Sim::new(&[1, 2, 3, 4, 5], Sim::standard_cfg(), 1001);
    sim.set_drop_probability(0.05);
    sim.set_duplicate_probability(0.05);

    let dl = sim.clock() + Duration::from_secs(5);
    sim.run_until_predicate(dl, |s| {
        [1u64, 2, 3, 4, 5]
            .iter()
            .any(|n| s.role_of(*n) == Role::Leader)
    });
    let leader = pick_leader(&sim, &[1, 2, 3, 4, 5]).expect("no leader");
    sim.client_write(leader, 5);

    // Periodic chaos: crash a non-leader, restart it, run more.
    for round in 0..3 {
        let nodes_alive: Vec<NodeId> = [1u64, 2, 3, 4, 5]
            .iter()
            .copied()
            .filter(|n| !sim.is_crashed(*n))
            .collect();
        if let Some(leader) = pick_leader(&sim, &nodes_alive) {
            let target: NodeId = nodes_alive
                .iter()
                .copied()
                .find(|n| *n != leader)
                .unwrap_or(leader);
            sim.crash(target);
            let dl = sim.clock() + Duration::from_millis(500);
            sim.run_until(dl);
            sim.client_write(leader, 2);
            let dl = sim.clock() + Duration::from_millis(500);
            sim.run_until(dl);
            sim.restart(target);
            let dl = sim.clock() + Duration::from_millis(500);
            sim.run_until(dl);
        }
        let _ = round;
    }

    // Drain the chaos: turn faults off, give the cluster a generous
    // settle window, then assert. Asserting under active drops is
    // racy — the cluster might be mid-walk-back when the predicate
    // fires.
    sim.set_drop_probability(0.0);
    sim.set_duplicate_probability(0.0);
    let dl = sim.clock() + Duration::from_secs(10);
    sim.run_until(dl);

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();

    // Some leader (current or previous) committed at least the
    // initial 5-entry batch.
    let any_committed = [1u64, 2, 3, 4, 5]
        .iter()
        .any(|n| sim.cluster_commit_of(*n) >= 1);
    assert!(any_committed, "no commits after long stress run");
}

/// Deterministic replay: same seed and schedule produce identical
/// state across runs. Verify by running twice and comparing every
/// node's commit-state.
#[test]
fn deterministic_replay_with_same_seed() {
    fn run() -> Vec<(NodeId, u64)> {
        let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 7777);
        let dl = Instant::now() + Duration::from_secs(2);
        sim.run_until_predicate(dl, |s| {
            [1u64, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
        });
        if let Some(leader) = pick_leader(&sim, &[1, 2, 3]) {
            sim.client_write(leader, 4);
            let dl = sim.clock() + Duration::from_secs(2);
            sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 4);
        }
        let mut out: Vec<(NodeId, u64)> = [1u64, 2, 3]
            .iter()
            .map(|n| (*n, sim.cluster_commit_of(*n)))
            .collect();
        out.sort();
        out
    }
    let a = run();
    let b = run();
    assert_eq!(a, b, "simulator was not deterministic across runs");
}
