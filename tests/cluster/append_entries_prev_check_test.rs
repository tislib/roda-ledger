//! ADR-0016 §8 — `prev_tx_id` / `prev_term` enforcement on
//! `AppendEntries`, plus the §9 divergence-watermark stash.
//!
//! Hits `NodeHandler::append_entries` directly via `tonic::Request`,
//! no networking. Each test shapes its own follower term log and
//! committed transactions, then sends a crafted `AppendEntriesRequest`
//! and asserts both the response code and the
//! `take_divergence_watermark` side-effect.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::node as proto;
use roda_ledger::cluster::proto::node::node_server::Node;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl, NodeHandler, Role, Term};
use roda_ledger::ledger::Ledger;
use roda_ledger::storage::{TermRecord, TermStorage};
use roda_ledger::transaction::Operation;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Request;

// ── harness ────────────────────────────────────────────────────────────────

/// Build a single-slot Bare-mode harness with role pinned to
/// Follower, returning `(ctl, ledger, term, handler)`. The handler
/// claims `node_id == 2` to match the historical setup (it's the
/// follower receiving from leader id 1).
async fn setup() -> (ClusterTestingControl, Arc<Ledger>, Arc<Term>, NodeHandler) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Follower))
        .await
        .expect("bare start");
    let ledger = ctl.ledger(0).expect("ledger");
    let term = ctl.term(0).expect("term");
    let handler = ctl
        .node_handler(0, /* claimed_node_id */ 2)
        .expect("node_handler");
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

/// Convenience: build an `AppendEntriesRequest` with all the bits a
/// caller might tune. `wal_bytes` always empty (heartbeat) — these
/// tests are about prev-log enforcement, not entry application.
fn make_request(
    term: u64,
    prev_tx_id: u64,
    prev_term: u64,
    leader_commit_tx_id: u64,
) -> proto::AppendEntriesRequest {
    proto::AppendEntriesRequest {
        leader_id: 1,
        term,
        prev_tx_id,
        prev_term,
        from_tx_id: prev_tx_id + 1,
        to_tx_id: prev_tx_id,
        wal_bytes: Vec::new(),
        leader_commit_tx_id,
    }
}

// ── tests ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_tx_id_zero_is_accepted_unconditionally() {
    // First-RPC sentinel: leader signals "I have no prior tx" via
    // `prev_tx_id == 0`. The follower must accept regardless of its
    // own log content.
    let (_ctl, ledger, _term, handler) = setup().await;
    // Even with some local history, prev=(0,0) bypasses the check.
    let tx = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 7,
        user_ref: 0,
    });
    wait_committed(&ledger, tx).await;

    let resp = handler
        .append_entries(Request::new(make_request(
            /* term */ 1, /* prev_tx_id */ 0, /* prev_term */ 0,
            /* leader_commit_tx_id */ 0,
        )))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success, "prev=(0,0) heartbeat must succeed");
    assert_eq!(resp.reject_reason, proto::RejectReason::RejectNone as u32);
    assert!(handler.take_divergence_watermark().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_log_match_with_correct_term_is_accepted() {
    let (_ctl, ledger, term, handler) = setup().await;

    // Open term 1 covering tx 1+, then commit tx 1 and tx 2.
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

    // Leader claims prev_tx_id=1 was written under term=1 — matches us.
    let resp = handler
        .append_entries(Request::new(make_request(
            /* term */ 1, /* prev_tx_id */ 1, /* prev_term */ 1,
            /* leader_commit_tx_id */ 2,
        )))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.success);
    assert_eq!(resp.reject_reason, proto::RejectReason::RejectNone as u32);
    assert!(handler.take_divergence_watermark().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_tx_id_beyond_our_last_commit_is_rejected_as_gap() {
    let (_ctl, ledger, term, handler) = setup().await;
    term.new_term(1).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;
    // Our last_commit_id is 1. Leader claims prev_tx_id=50 — gap.
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
    // Gaps are not divergence — we just don't have prev_tx_id yet.
    // The watermark is still set to the leader's view, but the
    // supervisor decides what to do with it; spec-wise either
    // behaviour is fine. We at least assert we replied with the
    // right reject code.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prev_term_mismatch_at_existing_tx_id_triggers_divergence() {
    // Our tx 1 was written under term 1. Leader claims it should be
    // term 2 — the classic Raft divergence case (different leader
    // wrote different content under a different term at this tx id).
    let (_ctl, ledger, term, handler) = setup().await;
    term.new_term(1).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    let leader_commit = 17u64;
    let resp = handler
        .append_entries(Request::new(make_request(
            /* term */ 2,
            /* prev_tx_id */ 1,
            /* prev_term */ 2, // wrong: ours was 1
            /* leader_commit_tx_id */ leader_commit,
        )))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectPrevMismatch as u32
    );
    // Divergence stashed for the supervisor: it equals the leader's
    // commit watermark from the rejecting request.
    assert_eq!(
        handler.take_divergence_watermark(),
        Some(leader_commit),
        "divergence at prev_tx_id must stash leader_commit_tx_id",
    );
    // Take is destructive — second call returns None.
    assert!(handler.take_divergence_watermark().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn divergence_watermark_updates_to_latest_leader_commit() {
    // Two divergence rejects in a row should leave the most recent
    // leader_commit_tx_id stashed (we don't queue them).
    let (_ctl, ledger, term, handler) = setup().await;
    term.new_term(1).unwrap();
    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    let _ = handler
        .append_entries(Request::new(make_request(2, 1, 9, 100)))
        .await
        .unwrap();
    let _ = handler
        .append_entries(Request::new(make_request(2, 1, 9, 250)))
        .await
        .unwrap();

    assert_eq!(handler.take_divergence_watermark(), Some(250));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_role_rejects_append_entries() {
    // Same setup, just with a Leader-role harness so the handler
    // returns NotFollower without touching the log.
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Leader))
        .await
        .expect("bare start");
    let handler = ctl
        .node_handler(0, /* claimed_node_id */ 1)
        .expect("handler");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_lookup_path_via_term_storage_also_detects_divergence() {
    // Pre-seed the term log directly via storage so the Term hot ring
    // sees `term=1 @ start_tx_id=0` from a fresh boot — exercises the
    // get_term_at_tx hot path.
    //
    // Use `autostart: false` so the harness allocates a data dir but
    // does NOT open `Ledger`/`Term`/`Vote` yet — we get to write a
    // `TermRecord` to disk before the in-memory hot ring is populated.
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        autostart: false,
        ..ClusterTestingConfig::bare(Role::Follower)
    })
    .await
    .expect("bare start");

    let data_dir = ctl.data_dir(0).unwrap().to_path_buf();
    std::fs::create_dir_all(&data_dir).unwrap();
    {
        let mut storage = TermStorage::open(&data_dir.to_string_lossy()).unwrap();
        storage
            .append(TermRecord {
                term: 1,
                start_tx_id: 0,
            })
            .unwrap();
        storage.sync().unwrap();
    }

    // Now bring the bare components up — Term::open_in_dir will see
    // the pre-seeded record on its first scan.
    ctl.start_node(0).await.expect("start_node");
    let ledger = ctl.ledger(0).expect("ledger");
    let handler = ctl.node_handler(0, 2).expect("handler");

    let tx1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1,
        user_ref: 0,
    });
    wait_committed(&ledger, tx1).await;

    // Leader claims tx 1 was term 7 — disagrees with our term log
    // (which has only `term=1` at start_tx=0).
    let resp = handler
        .append_entries(Request::new(make_request(
            /* term */ 7, /* prev_tx_id */ 1, /* prev_term */ 7,
            /* leader_commit_tx_id */ 42,
        )))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectPrevMismatch as u32
    );
    assert_eq!(handler.take_divergence_watermark(), Some(42));
}
