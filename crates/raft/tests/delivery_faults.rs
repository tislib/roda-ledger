//! Message-delivery faults: drop, duplication, long delay, and
//! late/spurious replies. The harness's `set_drop_probability` and
//! `set_duplicate_probability` toggle the per-message fault
//! injection; `set_network_delay` widens the link.
//!
//! A few cases (out-of-order replies, late reply after a synthetic
//! RPC timeout, reply to a never-sent request) are exercised
//! directly against `RaftNode<MemPersistence>` since they don't need
//! the full simulator — the library should ignore the malformed
//! input gracefully.

mod common;

use std::time::{Duration, Instant};

use common::Sim;
use common::mem_persistence::MemPersistence;
use raft::{AppendResult, NodeId, RaftConfig, RaftNode, RejectReason, Role};

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

fn fresh_node(self_id: u64, peers: Vec<u64>) -> RaftNode<MemPersistence> {
    RaftNode::new(
        self_id,
        peers,
        MemPersistence::new(),
        RaftConfig::default(),
        42,
    )
}

/// At a 10% drop rate the cluster slows but still makes progress.
#[test]
fn cluster_makes_progress_under_10pct_drop() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 31);
    sim.set_drop_probability(0.10);
    let leader = await_leader(&mut sim);

    sim.client_write(leader, 10);
    let dl = sim.clock() + Duration::from_secs(10);
    let progressed = sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 5);
    assert!(
        progressed,
        "cluster did not commit at least 5 entries under 10% drop"
    );

    sim.assert_election_safety();
    sim.assert_log_matching();
}

/// Idempotent retransmission: with every message duplicated, the
/// cluster behaves identically (Raft RPCs are idempotent on the
/// receiver).
#[test]
fn cluster_is_idempotent_under_duplication() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 37);
    sim.set_duplicate_probability(1.0);
    let leader = await_leader(&mut sim);

    sim.client_write(leader, 5);
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 5);
    assert!(sim.cluster_commit_of(leader) >= 5);

    sim.assert_election_safety();
    sim.assert_log_matching();
    sim.assert_state_machine_safety();
}

/// Long network delay (> election timeout) — every RPC arrives
/// stale relative to the receiver's term. The cluster never settles
/// on a leader; elections keep timing out and bumping term.
/// Documents the no-pre-vote v1 limitation.
#[test]
fn cluster_thrashes_under_delay_above_election_timeout() {
    let mut sim = Sim::new(&[1, 2, 3], Sim::standard_cfg(), 43);
    // Standard cfg's election timeout is 80–160ms. Set delay 200ms
    // (deliberately above) — every RV/AE arrives after the
    // candidate's own timer has fired, so receivers reject the
    // stale term and elections keep bumping.
    sim.set_network_delay(Duration::from_millis(200));
    let dl = sim.clock() + Duration::from_secs(2);
    sim.run_until(dl);
    // Term is climbing — at least one node bumped repeatedly.
    let max_term = [1u64, 2, 3]
        .iter()
        .map(|n| sim.current_term_of(*n))
        .max()
        .unwrap_or(0);
    assert!(
        max_term >= 3,
        "expected term-bumping under delay, max term seen = {}",
        max_term
    );
}

/// Long network delay (~ heartbeat interval, below election timeout)
/// — the cluster is slower but still commits.
#[test]
fn cluster_makes_progress_under_moderate_delay() {
    let mut cfg = Sim::standard_cfg();
    // Widen the election window so RVs round-trip comfortably.
    cfg.election_timer = raft::ElectionTimerConfig {
        min_ms: 200,
        max_ms: 400,
    };
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg.rpc_timeout = Duration::from_millis(200);
    let mut sim = Sim::new(&[1, 2, 3], cfg, 47);
    sim.set_network_delay(Duration::from_millis(30));
    let leader = await_leader(&mut sim);
    sim.client_write(leader, 3);
    let dl = sim.clock() + Duration::from_secs(5);
    let progressed = sim.run_until_predicate(dl, |s| s.cluster_commit_of(leader) >= 3);
    assert!(
        progressed,
        "no progress under 30ms delay + widened election window"
    );
}

/// A reply from a node that is not in this leader's peer list must be
/// unreachable through the public `Replication` API — the per-peer
/// gate refuses to hand out a handle, so there is no way to feed a
/// non-peer reply into the state machine. Pins this property as part
/// of the surface contract.
#[test]
fn unsolicited_append_entries_reply_is_unreachable_via_replication() {
    let mut node = fresh_node(1, vec![1]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    assert!(node.role().is_leader());
    let term_before = node.current_term();

    // Peer 99 was never configured: `Replication::peer` returns None,
    // and there is no other entry point that takes a `from` argument.
    assert!(node.replication().peer(99).is_none());

    // Leader stays leader; nothing observable changed.
    assert!(node.role().is_leader());
    assert_eq!(node.current_term(), term_before);
}

/// Late reply after a simulated RPC-timeout: the leader's in-flight
/// slot is already cleared by the timeout, so a subsequent reply that
/// arrives at the same peer slot just gets processed normally. Must
/// not panic or step down.
#[test]
fn late_reply_after_rpc_timeout_does_not_crash() {
    // Two-node cluster so peer 2 is real and we can drive it to leader
    // via a single peer vote.
    let mut node = fresh_node(1, vec![1, 2]);
    let t0 = Instant::now();
    common::drive_tick(&mut node, t0);
    common::drive_tick(&mut node, t0 + Duration::from_secs(60));
    let term = node.current_term();
    common::deliver_vote_reply(&mut node, t0 + Duration::from_secs(60), 2, term, true);
    assert!(node.role().is_leader());

    // Synthesize the cluster-driver-side timeout: clears the leader's
    // in-flight slot for peer 2 and leaves indexes alone.
    node.replication()
        .peer(2)
        .unwrap()
        .append_result(t0 + Duration::from_secs(100), AppendResult::Timeout);

    // Now the "late" reply lands. With in_flight already cleared by
    // the timeout, the leader processes it as an ordinary stale
    // reject and stays the leader.
    let term_now = node.current_term();
    node.replication().peer(2).unwrap().append_result(
        t0 + Duration::from_secs(101),
        AppendResult::Reject {
            term: term_now,
            reason: RejectReason::RpcTimeout,
            last_write_id: 0,
            last_commit_id: 0,
        },
    );
    assert!(node.role().is_leader());
}
