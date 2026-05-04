//! Safety & client-facing durability invariants.
//!
//! End-to-end tests against a real `ClusterTestingControl`:
//!
//! - `idempotent_retry_across_failover`
//! - `acked_cluster_commit_survives_full_restart`
//! - `rotational_restart_resilience`
//! - `no_leader_blocks_writes`
//! - `mid_election_cluster_commit_not_lost`


use ::proto::ledger::WaitLevel;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl, ClusterTestingError};
use storage::entities::{FailReason, SYSTEM_ACCOUNT_ID};
use std::time::Duration;
use tokio::time::sleep;

const ACCOUNT: u64 = 7;
const AMOUNT: u64 = 100;

fn duplicate_code() -> u32 {
    FailReason::DUPLICATE.as_u8() as u32
}

/// Wait until *some* node in the cluster reports `Leader` via Ping. The
/// supervisor's role driver may need a few ticks after a node restart to
/// resettle; the harness's `wait_for_leader` already handles "no leader yet"
/// by polling until timeout.
async fn wait_leader(ctl: &ClusterTestingControl, timeout: Duration) -> usize {
    ctl.wait_for_leader(timeout)
        .await
        .expect("leader did not appear within timeout")
}

// ─────────────────────────────────────────────────────────────────────────
//
// Idempotent retry across failover.
//
// 1. Submit deposits with `user_ref ∈ 1..=100` under leader L1, every op
//    waiting at `ClusterCommit`. All 100 land.
// 2. Kill L1.
// 3. New leader L2 emerges. Client reconnects.
// 4. Resubmit `user_ref ∈ 1..=200` against L2. The first 100 are dedup
//    duplicates; the next 100 are new.
// 5. Final balance equals 200 × AMOUNT — no doubles, no losses, no torn
//    state. `system + account == 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn idempotent_retry_across_failover() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_idempotent".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader1_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    let l1_node_id = ctl.node_id(leader1_idx).expect("node_id");
    eprintln!("[idempotent] leader1 = node {}", l1_node_id);

    // Phase 1: submit user_refs 1..=100 under L1, ClusterCommit-acked.
    let half: u64 = 100;
    let total: u64 = 200;
    let first_half: Vec<(u64, u64, u64)> = (1..=half).map(|ur| (ACCOUNT, AMOUNT, ur)).collect();

    let r1 = ctl
        .deposit_batch_and_wait(&first_half, WaitLevel::ClusterCommit)
        .await
        .expect("first half ack under L1");
    assert_eq!(r1.len() as u64, half);
    for r in &r1 {
        assert_eq!(r.fail_reason, 0, "phase-1 deposit must succeed");
    }

    let bal_pre_kill = (half as i64) * (AMOUNT as i64);
    ctl.require_balance(ACCOUNT, bal_pre_kill).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    // Phase 2: kill L1.
    ctl.stop_node(leader1_idx).await.expect("stop L1");

    // Phase 3: a different node must win the election.
    let leader2_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    let l2_node_id = ctl.node_id(leader2_idx).expect("L2 node_id");
    assert_ne!(
        l2_node_id, l1_node_id,
        "killed node cannot also be the new leader"
    );
    eprintln!("[idempotent] leader2 = node {}", l2_node_id);

    // Phase 4: retry the FULL 200-op batch. user_refs 1..=100 are
    // duplicates (already in dedup cache, replicated to L2's WAL);
    // 101..=200 are fresh.
    let full_batch: Vec<(u64, u64, u64)> = (1..=total).map(|ur| (ACCOUNT, AMOUNT, ur)).collect();

    let r2 = ctl
        .deposit_batch_and_wait(&full_batch, WaitLevel::ClusterCommit)
        .await
        .expect("retry batch under L2");
    assert_eq!(r2.len() as u64, total);

    let dup = r2
        .iter()
        .filter(|r| r.fail_reason == duplicate_code())
        .count();
    let ok = r2.iter().filter(|r| r.fail_reason == 0).count();
    assert_eq!(
        dup, half as usize,
        "first {} retried user_refs must dedup",
        half
    );
    assert_eq!(
        ok, half as usize,
        "last {} user_refs are new and must succeed",
        half
    );

    // Phase 5: final balance reflects ONE application of the 200 deposits.
    let bal_final = (total as i64) * (AMOUNT as i64);
    ctl.require_balance(ACCOUNT, bal_final).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    // Cross-node check: surviving follower must agree.
    let follower_idx = ctl
        .first_follower_index()
        .await
        .expect("there must still be a follower");
    ctl.require_balance_on(follower_idx, ACCOUNT, bal_final)
        .await;

    ctl.stop_all().await;
}

// ─────────────────────────────────────────────────────────────────────────
//
// Balance staleness across failover.
//
// When a follower is promoted to leader, its `Transactor::balances`
// must reflect every replicated tx — otherwise validation on new ops
// uses out-of-date state and silently mis-decides
// (e.g. `INSUFFICIENT_FUNDS` on a transfer that should succeed).
//
// 1. 3-node cluster. Fund account A with 1000 on leader L1, ack
//    ClusterCommit (so the deposit is on majority of WALs).
// 2. Kill L1.
// 3. New leader L2 emerges (it was a follower; received tx 1 via
//    replication; its Transactor MUST have applied it to balances).
// 4. On L2, attempt `transfer(A → B, 600)` and ack ClusterCommit.
//    Without the routing fix, L2's stale `balances[A] == 0` rejects
//    this with INSUFFICIENT_FUNDS. With the fix, the transfer
//    succeeds; final A=400, B=600.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn balance_state_in_sync_after_failover() {
    const ACC_A: u64 = 71;
    const ACC_B: u64 = 72;

    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_balance_failover".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader1_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    let l1_node_id = ctl.node_id(leader1_idx).expect("node_id");

    // Fund A with 1000 on the leader; ack ClusterCommit so it's on
    // majority of WALs (and replicated to whichever survivor wins).
    let r = ctl
        .deposit_and_wait(ACC_A, 1000, /*user_ref=*/ 1, WaitLevel::ClusterCommit)
        .await
        .expect("fund A");
    assert_eq!(r.fail_reason, 0);
    ctl.require_balance(ACC_A, 1000).await;

    // Kill L1.
    ctl.stop_node(leader1_idx).await.expect("stop L1");

    // New leader emerges from the followers — its Transactor's
    // `balances[A]` is what we're testing.
    let leader2_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    assert_ne!(
        ctl.node_id(leader2_idx).unwrap(),
        l1_node_id,
        "killed leader cannot be the new one"
    );

    // Transfer 600 from A to B on the new leader. The new leader's
    // Transactor must validate against an up-to-date `balances[A]`.
    let r = ctl
        .transfer_and_wait(
            ACC_A,
            ACC_B,
            600,
            /*user_ref=*/ 2,
            WaitLevel::ClusterCommit,
        )
        .await
        .expect("transfer A→B");
    assert_eq!(
        r.fail_reason, 0,
        "new leader must not reject transfer due to stale balance: \
         got fail_reason={}",
        r.fail_reason
    );

    ctl.require_balance(ACC_A, 400).await;
    ctl.require_balance(ACC_B, 600).await;
    ctl.require_zero_sum(&[ACC_A, ACC_B, SYSTEM_ACCOUNT_ID])
        .await;

    // Cross-node convergence: surviving follower agrees.
    let follower_idx = ctl
        .first_follower_index()
        .await
        .expect("a non-leader survives");
    ctl.require_balance_on(follower_idx, ACC_A, 400).await;
    ctl.require_balance_on(follower_idx, ACC_B, 600).await;

    ctl.stop_all().await;
}

// ─────────────────────────────────────────────────────────────────────────
//
// Every `WaitLevel::ClusterCommit`-acked tx survives a cold restart of
// the entire cluster. After all three nodes go down and come back up
// against the same data dirs, every recorded tx_id is still committed,
// balances are unchanged, no acked write disappears.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn acked_cluster_commit_survives_full_restart() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_full_restart".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let _leader_idx = wait_leader(&ctl, Duration::from_secs(10)).await;

    // Submit N=20 deposits, each individually `submit_and_wait(ClusterCommit)`.
    // Doing them one-at-a-time means each assertion ("acked => durable") is
    // unambiguous. The harness tracks `last_tx_id` internally so the
    // post-restart `require_*` reads block on the new leader's snapshot
    // catch-up.
    let n: u64 = 20;
    let expected_balance = (n as i64) * (AMOUNT as i64);
    let mut acked_tx_ids: Vec<u64> = Vec::with_capacity(n as usize);
    for ur in 1..=n {
        let r = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit_and_wait");
        assert_eq!(r.fail_reason, 0);
        assert!(r.tx_id > 0);
        acked_tx_ids.push(r.tx_id);
    }

    ctl.require_balance(ACCOUNT, expected_balance).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    // Cold-restart the entire cluster on the same data dirs.
    for i in 0..ctl.len() {
        ctl.stop_node(i).await.expect("stop");
    }
    for i in 0..ctl.len() {
        ctl.start_node(i).await.expect("start");
    }

    let _new_leader_idx = wait_leader(&ctl, Duration::from_secs(15)).await;

    // Every acked tx_id must still be Committed (or further) on the
    // new leader once it has caught up to last_tx_id.
    for tx_id in &acked_tx_ids {
        ctl.require_transaction_committed(*tx_id).await;
    }

    // Balance must be exactly preserved.
    ctl.require_balance(ACCOUNT, expected_balance).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    ctl.stop_all().await;
}

// ─────────────────────────────────────────────────────────────────────────
//
// Rotational restart resilience.
//
// Bring up a 3-node cluster. Submit a baseline of `ClusterCommit`-acked
// writes. Now restart each node in turn (followers first, then the
// leader). After every restart, verify the cluster still has a leader,
// every pre-recorded tx is still Committed, and the balance is unchanged.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn rotational_restart_resilience() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_rotational".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let _initial_leader = wait_leader(&ctl, Duration::from_secs(10)).await;

    // Baseline: 30 deposits, ClusterCommit-acked individually, recorded.
    let baseline_count: u64 = 30;
    let baseline_balance = (baseline_count as i64) * (AMOUNT as i64);
    let mut baseline_ids: Vec<u64> = Vec::with_capacity(baseline_count as usize);
    for ur in 1..=baseline_count {
        let r = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("baseline deposit");
        assert_eq!(r.fail_reason, 0);
        baseline_ids.push(r.tx_id);
    }
    ctl.require_balance(ACCOUNT, baseline_balance).await;

    // Restart every slot once, in slot-order. After each, the cluster must
    // have a leader within the election-timer budget AND the baseline must
    // still be intact across whichever node currently leads.
    for i in 0..ctl.len() {
        let node_id = ctl.node_id(i).expect("node_id");
        eprintln!("[rotational] restarting slot {} (node_id={})", i, node_id);
        ctl.stop_node(i).await.expect("stop");
        ctl.start_node(i).await.expect("start");

        // The cluster (with at most 1 node down) must keep a leader.
        let _new_leader = wait_leader(&ctl, Duration::from_secs(10)).await;

        // Every baseline tx must still be Committed (or further) once
        // the leader has caught up to last_tx_id.
        for tx_id in &baseline_ids {
            ctl.require_transaction_committed(*tx_id).await;
        }

        ctl.require_balance(ACCOUNT, baseline_balance).await;
        ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;
    }

    // After the rotation completes, all three nodes must agree on the
    // baseline once their commit indices have caught up.
    for i in 0..ctl.len() {
        ctl.require_balance_on(i, ACCOUNT, baseline_balance).await;
    }

    ctl.stop_all().await;
}

// ─────────────────────────────────────────────────────────────────────────
//
// When no leader exists, writes are blocked.
//
// 1. Bring up a 3-node cluster, observe a leader.
// 2. Stop ALL three nodes.
// 3. Restart only one: it boots into Initializing, cycles
//    Initializing → Candidate → Initializing because it cannot reach
//    quorum (1 of 3, majority needs 2).
// 4. Every `submit_operation` must return `FailedPrecondition` ("node is
//    not a leader"). No tx ever lands in any WAL.
// 5. Start a second node so quorum (2 of 3) becomes reachable; election
//    settles a leader; the very next submit succeeds.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn no_leader_blocks_writes() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_no_leader".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    // Baseline: cluster came up and elected a leader.
    let _initial_leader = wait_leader(&ctl, Duration::from_secs(10)).await;

    // Take everything down, clear the harness's cached leader.
    for i in 0..ctl.len() {
        ctl.stop_node(i).await.expect("stop");
    }

    // Start ONLY node 0. Quorum = 2 of 3; this node is alone, cannot win
    // any election round. Role oscillates Initializing ↔ Candidate, never
    // Leader. The wait_for_leader call below gives the supervisor enough
    // time to settle into that oscillation.
    ctl.start_node(0).await.expect("start node 0");

    // `wait_for_leader` should TIME OUT — no node is leader.
    match ctl.wait_for_leader(Duration::from_millis(800)).await {
        Err(ClusterTestingError::NoLeader { .. }) => {}
        other => panic!(
            "expected NoLeader, got {:?} (a leader appeared in a no-quorum cluster!)",
            other.map(|i| ctl.node_id(i).unwrap_or(0))
        ),
    }

    // Submit must fail with FailedPrecondition. Try a few times across
    // the Initializing/Candidate oscillation — every attempt must reject.
    // Negative-path: bypasses the harness's leader-routing retry loop.
    for attempt in 0..5 {
        ctl
            .deposit_and_wait_no_retry(ACCOUNT, AMOUNT, attempt + 1, WaitLevel::Computed)
            .await
            .expect_err("submit must fail when no leader");
        sleep(Duration::from_millis(100)).await;
    }

    // The lone node must have NO committed user transactions yet (the
    // term log will still record genesis terms, but no Deposit landed).
    // Verify via pipeline index — commit should be 1. (a new leader election produces single transaction)
    let idx = ctl.pipeline_index_on(0).await.expect("pipeline index");
    assert!(
        idx.commit <= 1,
        "no submission should have produced a commit while no leader existed"
    );

    // Bring node 1 back online — quorum (2 of 3) is now reachable.
    ctl.start_node(1).await.expect("start node 1");

    // An election should now resolve to a leader.
    let new_leader_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    eprintln!(
        "[no_leader] leader after quorum restored = node {}",
        ctl.node_id(new_leader_idx).unwrap()
    );

    // Submit succeeds against the new leader.
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 999, WaitLevel::ClusterCommit)
        .await
        .expect("submit succeeds once leader exists");
    assert_eq!(r.fail_reason, 0);
    assert!(r.tx_id > 0);

    ctl.require_balance(ACCOUNT, AMOUNT as i64).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    ctl.stop_all().await;
}

// ─────────────────────────────────────────────────────────────────────────
//
// During re-election, every previously cluster-committed tx is preserved.
//
// 1. 3-node cluster.
// 2. Submit N deposits, each individually waiting at ClusterCommit.
//    Snapshot every (tx_id, balance) tuple.
// 3. Kill the leader — the cluster transitions through an election
//    window during which one of the survivors becomes leader.
// 4. After the new leader stabilises, the snapshot tx_ids must all
//    still be Committed, the balance unchanged, and the zero-sum
//    invariant intact on every surviving node.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore]
async fn mid_election_cluster_commit_not_lost() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "safety_mid_election".to_string(),
        replication_poll_ms: 5,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    let leader_node_id = ctl.node_id(leader_idx).expect("node_id");

    // Submit N deposits, each ClusterCommit-acked. Snapshot each tx_id.
    let n: u64 = 25;
    let pre_balance = (n as i64) * (AMOUNT as i64);
    let mut acked: Vec<u64> = Vec::with_capacity(n as usize);
    for ur in 1..=n {
        let r = ctl
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("ClusterCommit deposit");
        assert_eq!(r.fail_reason, 0);
        acked.push(r.tx_id);
    }
    ctl.require_balance(ACCOUNT, pre_balance).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    // Pull the trigger on the leader.
    ctl.stop_node(leader_idx).await.expect("stop leader");

    // A different node must take over.
    let new_leader_idx = wait_leader(&ctl, Duration::from_secs(10)).await;
    let new_leader_node_id = ctl.node_id(new_leader_idx).expect("node_id");
    assert_ne!(
        new_leader_node_id, leader_node_id,
        "killed node cannot also be the new leader"
    );

    // Every acked tx must still be Committed (or further) on the new
    // leader once it has caught up to last_tx_id.
    for tx_id in &acked {
        ctl.require_transaction_committed(*tx_id).await;
    }

    ctl.require_balance(ACCOUNT, pre_balance).await;
    ctl.require_zero_sum(&[ACCOUNT, SYSTEM_ACCOUNT_ID]).await;

    // The remaining follower (the one that wasn't elected) should also
    // converge on the same balance once its commit catches up.
    let follower_idx = ctl
        .first_follower_index()
        .await
        .expect("there must still be a non-leader running node");
    ctl.require_balance_on(follower_idx, ACCOUNT, pre_balance)
        .await;

    ctl.stop_all().await;
}
