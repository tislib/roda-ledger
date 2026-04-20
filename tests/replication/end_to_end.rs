use super::common::{
    apply_ok, build_range, build_tx, config_for, make_append, start_ledger, wait_for_commit,
    TempDir, TEST_TERM,
};
use roda_ledger::ledger::Ledger;
use roda_ledger::replication::{AppendEntries, RejectReason, Replication};
use std::sync::Arc;
use std::time::Duration;

fn new_replication(ledger: &Arc<Ledger>) -> Replication {
    Replication::new(ledger.ledger_context(), 2, TEST_TERM)
}

#[test]
fn single_append_commits_on_follower() {
    let dir = TempDir::new("e2e_single");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let bytes = build_tx(1, 42, 500);
    let ok = apply_ok(&replication, make_append(1, 1, &bytes), 0);
    assert_eq!(ok.last_tx_id, 1);

    wait_for_commit(&ledger, 1, Duration::from_secs(5));
    assert_eq!(ledger.last_commit_id(), 1);
}

#[test]
fn sequential_appends_advance_watermark() {
    let dir = TempDir::new("e2e_sequential");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let mut last_local = 0u64;
    for tx_id in 1..=3 {
        let bytes = build_tx(tx_id, 1, 100);
        let ok = apply_ok(&replication, make_append(tx_id, tx_id, &bytes), last_local);
        assert_eq!(ok.last_tx_id, tx_id);
        wait_for_commit(&ledger, tx_id, Duration::from_secs(5));
        last_local = tx_id;
    }

    assert_eq!(ledger.last_commit_id(), 3);
}

#[test]
fn batched_append_commits_whole_range() {
    let dir = TempDir::new("e2e_batch");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let bytes = build_range(1, 50, 7, 10);
    let ok = apply_ok(&replication, make_append(1, 50, &bytes), 0);
    assert_eq!(ok.last_tx_id, 50);

    wait_for_commit(&ledger, 50, Duration::from_secs(5));
    assert_eq!(ledger.last_commit_id(), 50);
}

#[test]
fn corrupted_range_leaves_watermark_untouched() {
    let dir = TempDir::new("e2e_bad_crc");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let mut bytes = build_tx(1, 1, 100);
    bytes[4] ^= 0xFF;
    let err = replication
        .process(make_append(1, 1, &bytes), 0)
        .unwrap_err();
    assert_eq!(err.reason, RejectReason::CrcFailed);

    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(ledger.last_commit_id(), 0);

    let bytes = build_tx(1, 1, 100);
    apply_ok(&replication, make_append(1, 1, &bytes), 0);
    wait_for_commit(&ledger, 1, Duration::from_secs(5));
}

#[test]
fn prev_tx_id_mismatch_is_rejected() {
    let dir = TempDir::new("e2e_prev_mismatch");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let bytes = build_tx(1, 1, 100);
    apply_ok(&replication, make_append(1, 1, &bytes), 0);
    wait_for_commit(&ledger, 1, Duration::from_secs(5));

    let bytes2 = build_tx(2, 1, 100);
    let bad = AppendEntries {
        term: TEST_TERM,
        prev_tx_id: 5,
        prev_term: TEST_TERM,
        from_tx_id: 2,
        to_tx_id: 2,
        wal_bytes: &bytes2,
        leader_commit_tx_id: 2,
    };
    let err = replication.process(bad, 1).unwrap_err();
    assert_eq!(err.reason, RejectReason::PrevMismatch);

    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(ledger.last_commit_id(), 1);
}

#[test]
fn stale_term_is_rejected() {
    let dir = TempDir::new("e2e_term_stale");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    let bytes = build_tx(1, 1, 100);
    let stale = AppendEntries {
        term: 0,
        prev_tx_id: 0,
        prev_term: 0,
        from_tx_id: 1,
        to_tx_id: 1,
        wal_bytes: &bytes,
        leader_commit_tx_id: 1,
    };
    let err = replication.process(stale, 0).unwrap_err();
    assert_eq!(err.reason, RejectReason::TermStale);
    assert_eq!(err.term, TEST_TERM);
}

#[test]
fn replicated_data_survives_restart_as_leader() {
    let dir = TempDir::new("e2e_survives_restart");
    let target_account = 42u64;
    let amount_each = 7u64;
    let n = 20u64;

    {
        let follower = start_ledger(config_for(&dir.as_str(), true));
        let replication = new_replication(&follower);
        let bytes = build_range(1, n, target_account, amount_each);
        apply_ok(&replication, make_append(1, n, &bytes), 0);
        wait_for_commit(&follower, n, Duration::from_secs(5));
        drop(follower);
    }

    let leader = start_ledger(config_for(&dir.as_str(), false));
    let balance = leader.get_balance(target_account);
    assert_eq!(balance as i64, (n * amount_each) as i64);
    assert_eq!(leader.last_commit_id(), n);
}
