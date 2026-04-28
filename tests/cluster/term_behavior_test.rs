//! Term / NOT_FOUND behaviour — unit-ish integration tests hitting the
//! `LedgerHandler` directly (no network) and the durable term log.
//!
//! These exercise the four user-visible contracts:
//!
//! 1. `Ledger::get_transaction_status` returns `NotFound` for tx ids
//!    that were never sequenced (`0` or beyond `last_sequenced_id`).
//! 2. `GetTransactionStatus` surfaces `TX_NOT_FOUND` with `term_mismatch`
//!    set + the covering term and `term_start_tx_id` when the caller's
//!    expected term is wrong.
//! 3. `GetTransactionStatus` returns `TX_NOT_FOUND` for unknown tx
//!    without flipping `term_mismatch`.
//! 4. `WaitForTransaction` short-circuits to `NOT_FOUND` on unknown tx
//!    and `TERM_MISMATCH` on term fence failure.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::ledger as proto;
use roda_ledger::cluster::proto::ledger::ledger_server::Ledger as LedgerSvc;
use roda_ledger::cluster::{
    ClusterTestingConfig, ClusterTestingControl, LedgerHandler, Role, Term,
};
use roda_ledger::ledger::Ledger;
use roda_ledger::storage::{TermRecord, TermStorage};
use roda_ledger::transaction::Operation;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tonic::Request;

// ── helpers ────────────────────────────────────────────────────────────────

/// Build a single-slot Bare-mode harness with role pinned to Leader
/// (writable RPCs proceed) and return `(ctl, ledger, term,
/// handler)`. The harness owns lifetime of the data dir.
async fn setup() -> (ClusterTestingControl, Arc<Ledger>, Arc<Term>, LedgerHandler) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Leader))
        .await
        .expect("bare start");
    let ledger = ctl.ledger(0).expect("ledger");
    let term = ctl.term(0).expect("term");
    let handler = ctl.ledger_handler(0).expect("ledger_handler");
    (ctl, ledger, term, handler)
}

async fn wait_committed(ledger: &Ledger, tx_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while ledger.last_commit_id() < tx_id {
        if Instant::now() >= deadline {
            panic!("tx {} not committed within 5s", tx_id);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

// ── 1. Ledger::get_transaction_status NotFound ─────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ledger_get_transaction_status_returns_not_found_for_never_sequenced() {
    let (_ctl, ledger, _term, _handler) = setup().await;

    use roda_ledger::transaction::TransactionStatus as LStatus;
    // tx_id 0 is never assigned by the sequencer.
    assert!(matches!(
        ledger.get_transaction_status(0),
        LStatus::NotFound
    ));
    // Nothing submitted yet → any positive tx_id is unknown.
    assert!(matches!(
        ledger.get_transaction_status(1),
        LStatus::NotFound
    ));
    assert!(matches!(
        ledger.get_transaction_status(9_999),
        LStatus::NotFound
    ));

    // After a submit, the returned tx_id must be known.
    let tx_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx_id).await;
    assert!(!matches!(
        ledger.get_transaction_status(tx_id),
        LStatus::NotFound
    ));
    // One past the last sequenced id is still unknown.
    assert!(matches!(
        ledger.get_transaction_status(tx_id + 1),
        LStatus::NotFound
    ));
}

// ── 2. GetTransactionStatus RPC ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_rpc_returns_tx_not_found_for_unknown_tx() {
    let (_ctl, _ledger, _term, handler) = setup().await;

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
    // No term known for an unknown tx.
    assert_eq!(resp.term, 0);
    assert_eq!(resp.term_start_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_rpc_returns_tx_not_found_on_term_mismatch_with_actual_term() {
    let (_ctl, ledger, term, handler) = setup().await;

    // Open term 1 @ tx=0, submit a tx that lands in term 1, then open
    // term 2 @ tx=2 so term 1 has a known start+end.
    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    term.new_term(tx1 + 1).unwrap(); // term 2 starts right after tx1

    // Query tx1 asking for term 7 — no such term exists; we expect
    // TX_NOT_FOUND + term_mismatch with the actual covering term (1)
    // and its start (0).
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
    assert_eq!(resp.term, 1, "covering term for tx1");
    assert_eq!(resp.term_start_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_rpc_matches_caller_term_when_correct() {
    let (_ctl, ledger, term, handler) = setup().await;

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

    assert_ne!(
        resp.status,
        proto::TransactionStatus::TxNotFound as i32,
        "correct term must not trip NOT_FOUND"
    );
    assert!(!resp.term_mismatch);
    assert_eq!(resp.term, 1);
    assert_eq!(resp.term_start_tx_id, 0);
}

// ── 3. WaitForTransaction RPC ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_rpc_returns_not_found_for_unknown_tx_immediately() {
    let (_ctl, _ledger, _term, handler) = setup().await;

    // Use a long timeout on the caller side to prove the handler did
    // NOT block — if the wait loop ran at all, the test would stall.
    let start = Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        handler.wait_for_transaction(Request::new(proto::WaitForTransactionRequest {
            transaction_id: 99_999,
            term: 0,
            wait_level: proto::WaitLevel::Committed as i32,
        })),
    )
    .await
    .expect("wait_for_transaction should return promptly")
    .unwrap()
    .into_inner();

    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(resp.outcome, proto::WaitOutcome::NotFound as i32);
    assert_eq!(resp.term, 0);
    assert_eq!(resp.term_start_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_rpc_returns_term_mismatch_with_actual_term() {
    let (_ctl, ledger, term, handler) = setup().await;

    term.new_term(0).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    // term 2 starts after tx1 so the ring clearly separates the two.
    term.new_term(tx1 + 1).unwrap();

    let resp = handler
        .wait_for_transaction(Request::new(proto::WaitForTransactionRequest {
            transaction_id: tx1,
            term: 5, // wrong
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
async fn wait_rpc_reached_carries_actual_term() {
    let (_ctl, ledger, term, handler) = setup().await;

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
            term: 0, // opt out of fencing
            wait_level: proto::WaitLevel::Committed as i32,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.outcome, proto::WaitOutcome::Reached as i32);
    assert_eq!(resp.term, 1);
    assert_eq!(resp.term_start_tx_id, 0);
}

// ── 4. Term durability — explicit storage-layer reopen ─────────────────────

#[test]
fn term_storage_recovers_current_after_reopen() {
    // Pure storage-layer test — no Ledger / handler involvement,
    // so it stays on `tempfile::TempDir` rather than going through
    // the harness.
    let td = TempDir::new().unwrap();
    let dir = td.path().to_string_lossy().into_owned();

    let term = Term::open_in_dir(&dir).unwrap();
    term.new_term(0).unwrap(); // 1
    term.new_term(10).unwrap(); // 2
    term.new_term(20).unwrap(); // 3
    assert_eq!(term.get_current_term(), 3);

    // Drop the original handle and reopen — current + ring must persist.
    drop(term);

    let reopened = Term::open_in_dir(&dir).unwrap();
    assert_eq!(reopened.get_current_term(), 3);
    assert_eq!(
        reopened.last_record(),
        Some(TermRecord {
            term: 3,
            start_tx_id: 20
        })
    );

    // Cold path (via storage) must see every persisted record.
    let mut storage = TermStorage::open(&dir).unwrap();
    let mut collected = Vec::new();
    storage.scan(|r| collected.push(r)).unwrap();
    assert_eq!(collected.len(), 3);
    assert_eq!(
        collected,
        vec![
            TermRecord {
                term: 1,
                start_tx_id: 0,
            },
            TermRecord {
                term: 2,
                start_tx_id: 10,
            },
            TermRecord {
                term: 3,
                start_tx_id: 20,
            },
        ]
    );
}
