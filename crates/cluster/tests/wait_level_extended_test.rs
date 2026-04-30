//! Wait levels and read semantics on cluster.

use ::proto::ledger as proto;
use ::proto::ledger::WaitLevel;
use ::proto::ledger::ledger_server::Ledger as LedgerSvc;
use client::RetryConfig;
use cluster::{LedgerHandler, Role, Term};
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use ledger::ledger::Ledger;
use ledger::transaction::Operation;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::{TermRecord, TermStorage};
use tempfile::TempDir;
use tokio::time::sleep;
use tonic::Request;

const ACCOUNT: u64 = 1;
const AMOUNT: u64 = 100;

async fn setup_bare_leader() -> (ClusterTestingControl, Arc<Ledger>, Arc<Term>, LedgerHandler) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Leader))
        .await
        .expect("bare start");
    let ledger = ctl.ledger(0).unwrap();
    let term = ctl.term(0).unwrap();
    let handler = ctl.ledger_handler(0).expect("handler");
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

// ── Wait levels via cluster client API -----------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_level_computed_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Computed)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
    assert!(r.tx_id > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_level_committed_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Committed)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_level_snapshot_on_cluster() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(1))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::Snapshot)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
    ctl.require_balance(ACCOUNT, AMOUNT as i64).await;
}

/// `WaitLevel::ClusterCommit` requires snapshot ≥ tx, commit ≥ tx,
/// AND `cluster_commit_index ≥ tx`. End-to-end ack on a 3-node
/// cluster proves the wait succeeds when all three advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_level_cluster_commit_requires_full_advance() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let r = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();
    assert_eq!(r.fail_reason, 0);
}

/// `WaitLevel::ClusterCommit` blocks (and eventually times out) when no
/// quorum is reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_level_cluster_commit_blocks_without_quorum() {
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let leader_idx = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Ack one tx.
    ctl.deposit_and_wait(ACCOUNT, AMOUNT, 1, WaitLevel::ClusterCommit)
        .await
        .unwrap();

    // Stop both followers.
    for i in 0..ctl.len() {
        if i == leader_idx {
            continue;
        }
        ctl.stop_node(i).await.expect("stop");
    }

    let result = ctl
        .deposit_and_wait(ACCOUNT, AMOUNT, 2, WaitLevel::ClusterCommit)
        .await;
    assert!(result.is_err());
}

// ── Direct LedgerHandler wait semantics (term fence + NotFound) ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_unknown_tx_returns_not_found_immediately() {
    let (_ctl, _ledger, _term, handler) = setup_bare_leader().await;
    let started = Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        handler.wait_for_transaction(Request::new(proto::WaitForTransactionRequest {
            transaction_id: 99_999,
            term: 0,
            wait_level: proto::WaitLevel::Committed as i32,
        })),
    )
    .await
    .expect("must return promptly")
    .unwrap()
    .into_inner();
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(resp.outcome, proto::WaitOutcome::NotFound as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_with_wrong_term_returns_term_mismatch_with_actual_term() {
    let (_ctl, ledger, term, handler) = setup_bare_leader().await;
    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;
    term.new_term(tx1 + 1).unwrap();

    let resp = handler
        .wait_for_transaction(Request::new(proto::WaitForTransactionRequest {
            transaction_id: tx1,
            term: 5,
            wait_level: proto::WaitLevel::Committed as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome, proto::WaitOutcome::TermMismatch as i32);
    assert_eq!(resp.term, 1);
    assert_eq!(resp.term_start_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_reached_carries_actual_term() {
    let (_ctl, ledger, term, handler) = setup_bare_leader().await;
    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    let resp = handler
        .wait_for_transaction(Request::new(proto::WaitForTransactionRequest {
            transaction_id: tx1,
            term: 0,
            wait_level: proto::WaitLevel::Committed as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome, proto::WaitOutcome::Reached as i32);
    assert_eq!(resp.term, 1);
    assert_eq!(resp.term_start_tx_id, 0);
}

// ── GetTransactionStatus -------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_returns_tx_not_found_for_unknown_tx() {
    let (_ctl, _ledger, _term, handler) = setup_bare_leader().await;
    let resp = handler
        .get_transaction_status(Request::new(proto::GetStatusRequest {
            transaction_id: 42,
            term: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, proto::TransactionStatus::TxNotFound as i32);
    assert!(!resp.term_mismatch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_returns_term_mismatch_with_actual_term() {
    let (_ctl, ledger, term, handler) = setup_bare_leader().await;
    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;
    term.new_term(tx1 + 1).unwrap();

    let resp = handler
        .get_transaction_status(Request::new(proto::GetStatusRequest {
            transaction_id: tx1,
            term: 7,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, proto::TransactionStatus::TxNotFound as i32);
    assert!(resp.term_mismatch);
    assert_eq!(resp.term, 1);
    assert_eq!(resp.term_start_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_with_correct_term_succeeds() {
    let (_ctl, ledger, term, handler) = setup_bare_leader().await;
    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;
    let resp = handler
        .get_transaction_status(Request::new(proto::GetStatusRequest {
            transaction_id: tx1,
            term: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(resp.status, proto::TransactionStatus::TxNotFound as i32);
    assert!(!resp.term_mismatch);
}

// ── Submit semantics on non-leader roles ---------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_on_follower_returns_failed_precondition() {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::cluster(3))
        .await
        .expect("start");
    let _ = ctl.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let follower_idx = ctl.first_follower_index().await.expect("follower idx");
    let err = ctl
        .raw_client_for_slot(follower_idx)
        .expect("raw follower client")
        .deposit(ACCOUNT, AMOUNT, 0)
        .await
        .expect_err("must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// `term.log` storage layer rehydrates current term from the last
/// record on reopen. Cluster-level: ledger boots, terms are bumped,
/// reopen sees current_term match.
#[test]
fn term_storage_recovers_current_after_reopen() {
    let td = TempDir::new().unwrap();
    let dir = td.path().to_string_lossy().into_owned();

    let term = Term::open_in_dir(&dir).unwrap();
    term.new_term(0).unwrap();
    term.new_term(10).unwrap();
    term.new_term(20).unwrap();
    assert_eq!(term.get_current_term(), 3);
    drop(term);

    let reopened = Term::open_in_dir(&dir).unwrap();
    assert_eq!(reopened.get_current_term(), 3);
    assert_eq!(
        reopened.last_record(),
        Some(TermRecord {
            term: 3,
            start_tx_id: 20,
        })
    );

    let mut storage = TermStorage::open(&dir).unwrap();
    let mut collected = Vec::new();
    storage.scan(|r| collected.push(r)).unwrap();
    assert_eq!(collected.len(), 3);
}
