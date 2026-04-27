//! Lagged single-phase replication, AppendEntries semantics, lag
//! observability.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger::WaitLevel;
use roda_ledger::cluster::proto::node as proto;
use roda_ledger::cluster::proto::node::node_server::Node;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, Role};
use roda_ledger::entities::SYSTEM_ACCOUNT_ID;
use roda_ledger::ledger::Ledger;
use roda_ledger::transaction::Operation;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tonic::Request;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

// ── Setup helpers ---------------------------------------------------------

async fn bare_follower() -> (
    ClusterTestingControl,
    Arc<Ledger>,
    Arc<roda_ledger::cluster::Term>,
    roda_ledger::cluster::NodeHandler,
) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let ledger = ctl.ledger(0).unwrap();
    let term = ctl.term(0).unwrap();
    let handler = ctl.node_handler(0, 2).expect("handler");
    (ctl, ledger, term, handler)
}

async fn wait_committed(ledger: &Ledger, tx_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while ledger.last_commit_id() < tx_id {
        if Instant::now() > deadline {
            panic!("commit timeout");
        }
        sleep(Duration::from_millis(2)).await;
    }
}

fn make_request(
    term: u64,
    prev_tx_id: u64,
    prev_term: u64,
    leader_commit: u64,
) -> proto::AppendEntriesRequest {
    proto::AppendEntriesRequest {
        leader_id: 1,
        term,
        prev_tx_id,
        prev_term,
        from_tx_id: prev_tx_id + 1,
        to_tx_id: prev_tx_id,
        wal_bytes: Vec::new(),
        leader_commit_tx_id: leader_commit,
    }
}

// ── End-to-end replication ------------------------------------------------

/// Leader → follower replication catches the follower up to leader's
/// commit. Balances on both nodes match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_replicates_to_follower() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 2,
        append_entries_max_bytes: 256 * 1024,
        ..ClusterTestingConfig::cluster(2)
    })
    .await
    .expect("start");

    let _leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.client().leader().clone();
    let follower_client = ctl.client().next_follower().await.expect("follower index");

    // Follower rejects writes.
    let err = follower_client
        .deposit(ACCOUNT, AMOUNT, 0)
        .await
        .expect_err("follower rejects writes");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Drive 200 deposits on the leader.
    let total = 200u64;
    let deposits: Vec<(u64, u64, u64)> = (0..total).map(|_| (ACCOUNT, AMOUNT, 0)).collect();
    let r = leader_client
        .deposit_batch_and_wait(&deposits, WaitLevel::Committed)
        .await
        .expect("batch");
    for x in &r {
        assert_eq!(x.fail_reason, 0);
    }

    // Wait for follower to catch up.
    ctl.wait_for(Duration::from_secs(60), "follower commit", || {
        let fc = follower_client.clone();
        async move {
            fc.get_pipeline_index()
                .await
                .map(|i| i.commit >= total)
                .unwrap_or(false)
        }
    })
    .await
    .expect("catch up");

    // Balances match across nodes.
    let leader_bal = leader_client.get_balance(ACCOUNT).await.unwrap().balance;
    let follower_bal = follower_client.get_balance(ACCOUNT).await.unwrap().balance;
    assert_eq!(leader_bal, follower_bal);
    assert_eq!(leader_bal, (total * AMOUNT) as i64);
    let sys = follower_client
        .get_balance(SYSTEM_ACCOUNT_ID)
        .await
        .unwrap()
        .balance;
    assert_eq!(leader_bal + sys, 0);

    ctl.stop_all().await;
}

/// Idle heartbeats keep `Quorum::get()` advancing after the writer
/// stops. Without heartbeats the watermark would freeze one batch
/// behind the leader's actual commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_heartbeats_close_stale_by_one_gap() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 5,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.client().leader().clone();

    // Submit a few txs.
    for ur in 1..=10u64 {
        leader_client
            .deposit_and_wait(ACCOUNT, AMOUNT, ur, WaitLevel::ClusterCommit)
            .await
            .expect("deposit");
    }

    // Stop submitting. Quorum should converge to leader's last_commit_id
    // within a few heartbeat intervals.
    let supervisor = ctl.handles(leader_idx).unwrap();
    let quorum = supervisor.quorum().expect("quorum");
    let last_commit = ctl.ledger(leader_idx).unwrap().last_commit_id();
    ctl.wait_for(
        Duration::from_secs(2),
        "quorum catches up via heartbeats",
        || {
            let q = quorum.clone();
            async move { q.get() >= last_commit }
        },
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Quorum::get()={} did not catch leader's last_commit_id={} via heartbeats",
            quorum.get(),
            last_commit
        )
    });
    assert!(quorum.get() >= last_commit);
}

// ── AppendEntries — direct-handler scenarios ------------------------------

/// `prev_tx_id == 0` sentinel is accepted unconditionally — first
/// RPC of a fresh follower with no prior log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_tx_id_zero_sentinel_accepted() {
    let (_ctl, ledger, _term, handler) = bare_follower().await;
    let tx = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 7,
        user_ref: 0,
    });
    wait_committed(&ledger, tx).await;

    let resp = handler
        .append_entries(Request::new(make_request(1, 0, 0, 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.reject_reason, proto::RejectReason::RejectNone as u32);
    assert!(handler.take_divergence_watermark().is_none());
}

/// Matching `(prev_tx_id, prev_term)` accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_log_match_accepted() {
    let (_ctl, ledger, term, handler) = bare_follower().await;
    term.new_term(1).unwrap();
    let _ = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 5,
        user_ref: 0,
    });
    let tx2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 5,
        user_ref: 0,
    });
    wait_committed(&ledger, tx2).await;

    let resp = handler
        .append_entries(Request::new(make_request(1, 1, 1, 2)))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
}

/// `prev_tx_id` beyond our last commit → RejectPrevMismatch (gap).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_tx_id_gap_rejected() {
    let (_ctl, ledger, term, handler) = bare_follower().await;
    term.new_term(1).unwrap();
    let tx = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    wait_committed(&ledger, tx).await;

    let resp = handler
        .append_entries(Request::new(make_request(1, 50, 1, 5)))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectPrevMismatch as u32
    );
}

/// Stale-term `AppendEntries` (req.term < current_term) is rejected
/// with `RejectTermStale` and the response carries our higher term.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_term_append_entries_rejected_with_our_term() {
    let (_ctl, _ledger, term, handler) = bare_follower().await;

    // Make our term 7.
    term.observe(7, 0).unwrap();

    let resp = handler
        .append_entries(Request::new(make_request(3, 0, 0, 0)))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
    assert_eq!(resp.term, 7);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectTermStale as u32
    );
}

/// Higher-term `AppendEntries` durably advances our term log
/// (`Term::observe`) before applying entries. Vote log is NOT updated
/// by AE — only by `RequestVote` paths (per `node_handler.rs`).
/// Reopening the on-disk Term confirms durability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn higher_term_append_entries_durably_observed() {
    let (ctl, _ledger, term, handler) = bare_follower().await;

    // Pre: term 1.
    term.new_term(0).unwrap();
    assert_eq!(term.get_current_term(), 1);

    // Send AE with term 5 — durably observed.
    let resp = handler
        .append_entries(Request::new(make_request(5, 0, 0, 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(term.get_current_term(), 5);

    // Reopen Term — durable. (Vote layer is independently maintained;
    // AE doesn't update it.)
    let dir = ctl.data_dir(0).unwrap().to_string_lossy().into_owned();
    let t = roda_ledger::cluster::Term::open_in_dir(&dir).unwrap();
    assert_eq!(t.get_current_term(), 5);
}

/// `AppendEntries` to a Leader → `RejectNotFollower`, ledger untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_entries_to_leader_rejected() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Leader))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 1).expect("handler");

    let resp = handler
        .append_entries(Request::new(make_request(1, 0, 0, 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectNotFollower as u32
    );
}

/// After a successful AppendEntries that observes a higher term, the
/// follower's role flag transitions Initializing → Follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settle_as_follower_on_valid_append_entries() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Initializing))
        .await
        .expect("bare start");
    let handler = ctl.node_handler(0, 2).expect("handler");
    let role = ctl.role_flag(0).expect("role");
    assert_eq!(role.get(), Role::Initializing);

    let resp = handler
        .append_entries(Request::new(make_request(1, 0, 0, 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(role.get(), Role::Follower);
}

// ── Lag observability ----------------------------------------------------

/// Leader's `cluster_commit_index` advances on followers as quorum acks
/// flow back. Single-node cluster ⇒ leader is its own quorum, so
/// cluster_commit advances purely from on-commit hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_commit_index_advances() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let client = ctl.client().node(0).clone();

    let r = client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("ClusterCommit ack on single-node cluster");
    assert_eq!(r.fail_reason, 0);
}

/// Follower reads expose its (lagging) `commit` and `cluster_commit_index`
/// fields. After a write on the leader and a heartbeat, they catch up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_reads_reflect_lagging_state() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig {
        replication_poll_ms: 5,
        ..ClusterTestingConfig::cluster(3)
    })
    .await
    .expect("start");

    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let leader_client = ctl.client().leader().clone();

    let r = leader_client
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .expect("deposit");
    assert!(r.tx_id > 0);

    let follower_client = ctl.client().next_follower().await.expect("follower idx");
    // Follower must eventually report commit ≥ tx_id.
    ctl.wait_for(
        Duration::from_secs(10),
        "follower commit catches up",
        || {
            let fc = follower_client.clone();
            let target = r.tx_id;
            async move {
                fc.get_pipeline_index()
                    .await
                    .map(|idx| idx.commit >= target)
                    .unwrap_or(false)
            }
        },
    )
    .await
    .expect("follower commit");
}
