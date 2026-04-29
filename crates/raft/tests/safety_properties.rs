//! §5.4 safety property tests using `proptest`.
//!
//! ADR-0017 §"Test Strategy": "property tests in `crates/raft/tests/`:
//! §5.4 safety properties (Election Safety, Log Matching, Leader
//! Completeness, State Machine Safety) verified across randomized
//! event schedules using proptest". The harness in `tests/common/`
//! drives an in-memory cluster; this file shrinks across schedules
//! and seeds and reuses the harness's `assert_*` invariants.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use proptest::prelude::*;
use raft::{NodeId, Role};

/// Driver for a randomized cluster schedule. Picks a leader, drives
/// `K` rounds of (client_writes, optional crash) and asserts §5.4
/// invariants every time. Crashes are restarted next round so the
/// cluster cannot stall permanently. Three-node cluster keeps
/// majority at 2 so crashing one node still has a quorum.
fn drive(seed: u64, ops: Vec<Op>) {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), seed);
    let deadline = Instant::now() + Duration::from_secs(5);
    sim.run_until_predicate(deadline, |s| {
        [1u64, 2, 3].iter().any(|n| s.role_of(*n) == Role::Leader)
    });

    for op in ops {
        match op {
            Op::Write(n) => {
                let leader = current_leader(&sim);
                if let Some(l) = leader {
                    sim.client_write(l, n.max(1) as usize);
                    let dl = sim.clock() + Duration::from_secs(2);
                    sim.run_until_predicate(dl, |_| false);
                }
            }
            Op::CrashLeader => {
                if let Some(l) = current_leader(&sim) {
                    sim.crash(l);
                    let dl = sim.clock() + Duration::from_secs(3);
                    sim.run_until_predicate(dl, |s| {
                        [1u64, 2, 3]
                            .iter()
                            .filter(|n| **n != l)
                            .any(|n| s.role_of(*n) == Role::Leader)
                    });
                    sim.restart(l);
                    let dl = sim.clock() + Duration::from_secs(2);
                    sim.run_until(dl);
                }
            }
            Op::Partition(a, b) => {
                let na = (a as NodeId % 3) + 1;
                let nb = (b as NodeId % 3) + 1;
                if na != nb {
                    sim.partition(na, nb);
                    let dl = sim.clock() + Duration::from_secs(1);
                    sim.run_until(dl);
                    sim.heal_partition(na, nb);
                    let dl = sim.clock() + Duration::from_secs(1);
                    sim.run_until(dl);
                }
            }
        }

        // After every operation, every safety property must hold.
        sim.assert_election_safety();
        sim.assert_log_matching();
        sim.assert_state_machine_safety();
    }
}

fn current_leader(sim: &Sim) -> Option<NodeId> {
    [1u64, 2, 3]
        .iter()
        .copied()
        .find(|n| sim.role_of(*n) == Role::Leader)
}

#[derive(Clone, Debug)]
enum Op {
    /// Client write of N entries via the current leader.
    Write(u8),
    /// Crash the current leader and wait for failover.
    CrashLeader,
    /// Partition `(a, b)` for one second of virtual time, then heal.
    Partition(u8, u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u8..6).prop_map(Op::Write),
        1 => Just(Op::CrashLeader),
        2 => (0u8..3, 0u8..3).prop_map(|(a, b)| Op::Partition(a, b)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_shrink_iters: 32,
        // Each case spins up a full simulator + drives multiple
        // seconds of virtual time; 16 is enough to find regressions
        // without burning CI minutes.
        ..ProptestConfig::default()
    })]

    /// Election Safety + Log Matching + State Machine Safety hold
    /// under arbitrary schedules of writes, crash/restarts, and
    /// transient partitions.
    #[test]
    fn safety_under_random_schedule(seed in any::<u64>(), ops in proptest::collection::vec(op_strategy(), 1..6)) {
        drive(seed, ops);
    }
}
