//! End-to-end follower tests: a real `Ledger` in `replication_mode`,
//! fed through the `Replication` stage. Verifies records land in the
//! live WAL pipeline and the commit watermark advances, with no
//! Transactor thread running.

use super::common::{
    apply_ok, build_range, build_tx, config_for, make_append, start_ledger, wait_for_commit,
    TempDir, TEST_TERM,
};
use roda_ledger::ledger::Ledger;
use roda_ledger::replication::{AppendEntries, RejectReason, Replication};
use std::sync::Arc;
use std::time::Duration;

/// Build a `Replication` stage wired to the given follower Ledger.
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

    // Three back-to-back RPCs, each carrying a single transaction.
    // Second/third must pass the prev_tx_id consistency check.
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

    // 50 credits of 10 each into account 7.
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

    // Corrupt the CRC before it ever reaches the queue. The
    // invariant under test: a rejected RPC must NOT mutate any
    // on-disk or in-memory follower state.
    let mut bytes = build_tx(1, 1, 100);
    bytes[4] ^= 0xFF; // smash metadata.crc32c
    let err = replication
        .process(make_append(1, 1, &bytes), 0)
        .unwrap_err();
    assert_eq!(err.reason, RejectReason::CrcFailed);

    // Watermark must not have moved.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(ledger.last_commit_id(), 0);

    // Following clean RPC for tx_id=1 still succeeds — the failure
    // did not poison the stream.
    let bytes = build_tx(1, 1, 100);
    apply_ok(&replication, make_append(1, 1, &bytes), 0);
    wait_for_commit(&ledger, 1, Duration::from_secs(5));
}

#[test]
fn prev_tx_id_mismatch_is_rejected() {
    let dir = TempDir::new("e2e_prev_mismatch");
    let ledger = start_ledger(config_for(&dir.as_str(), true));
    let replication = new_replication(&ledger);

    // First batch lands cleanly.
    let bytes = build_tx(1, 1, 100);
    apply_ok(&replication, make_append(1, 1, &bytes), 0);
    wait_for_commit(&ledger, 1, Duration::from_secs(5));

    // Leader now sends tx 2 but *claims* prev_tx_id=5 — the
    // follower must reject before touching the WAL queue.
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
        term: 0, // < follower.current_term (1)
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
    // Write N transactions through Replication into a follower,
    // drop the ledger, reopen the same data directory in *leader*
    // mode, and verify the account balance matches what the leader
    // would have computed.
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
        // explicit drop to force wal.stop to be written
        drop(follower);
    }

    // Reopen as a normal (non-replication) Ledger; recovery should
    // replay the WAL we just built up via Replication.
    let leader = start_ledger(config_for(&dir.as_str(), false));
    let balance = leader.get_balance(target_account);
    // The last TxEntry's computed_balance we stamped was
    // `running = n * amount_each`; the balance cache reads it back
    // verbatim.
    assert_eq!(balance as i64, (n * amount_each) as i64);
    assert_eq!(leader.last_commit_id(), n);
}
