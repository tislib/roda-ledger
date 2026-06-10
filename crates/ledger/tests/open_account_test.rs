use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::{Operation, WaitLevel};
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::FailReason;

fn started(config: LedgerConfig) -> Ledger {
    let mut l = Ledger::new(config);
    l.start().unwrap();
    l
}

/// Deposit and return the resulting fail_reason (waits until the snapshot
/// reflects it, so `get_balance` is current afterward).
fn deposit(ledger: &Ledger, account: u64, amount: u64) -> FailReason {
    ledger
        .submit_and_wait(
            Operation::Deposit {
                account,
                amount,
                user_ref: 0,
            },
            WaitLevel::OnSnapshot,
        )
        .fail_reason
}

#[test]
fn open_then_deposit_is_enforced() {
    let ledger = started(LedgerConfig::temp());
    ledger.open_accounts(10); // ids 1..=10
    assert_eq!(deposit(&ledger, 5, 100), FailReason::NONE);
    assert_eq!(ledger.get_balance(5), 100);

    // account 11 was never opened
    assert_eq!(deposit(&ledger, 11, 50), FailReason::ACCOUNT_NOT_FOUND);
    assert_eq!(ledger.get_balance(11), 0);
}

#[test]
fn deposit_into_unopened_account_is_rejected() {
    let ledger = started(LedgerConfig::temp());
    assert_eq!(deposit(&ledger, 1, 100), FailReason::ACCOUNT_NOT_FOUND);
}

#[test]
fn open_count_zero_opens_exactly_one() {
    let ledger = started(LedgerConfig::temp());
    ledger.submit_and_wait(
        Operation::OpenAccount {
            count: 0,
            user_ref: 0,
        },
        WaitLevel::Committed,
    );
    assert_eq!(deposit(&ledger, 1, 10), FailReason::NONE);
    assert_eq!(deposit(&ledger, 2, 10), FailReason::ACCOUNT_NOT_FOUND);
}

#[test]
fn sequential_opens_extend_the_range() {
    let ledger = started(LedgerConfig::temp());
    ledger.open_accounts(3); // 1..=3
    ledger.open_accounts(2); // 4..=5
    assert_eq!(deposit(&ledger, 5, 10), FailReason::NONE);
    assert_eq!(deposit(&ledger, 6, 10), FailReason::ACCOUNT_NOT_FOUND);
}

#[test]
fn opening_past_initial_size_resizes() {
    let config = LedgerConfig {
        initial_account_size: 4,
        ..LedgerConfig::temp()
    };
    let ledger = started(config);
    ledger.open_accounts(500); // forces geometric growth from 4
    assert_eq!(deposit(&ledger, 500, 100), FailReason::NONE);
    assert_eq!(ledger.get_balance(500), 100);
}

#[test]
fn user_cannot_name_the_system_account() {
    let ledger = started(LedgerConfig::temp());
    ledger.open_accounts(5);
    // account 0 is SYSTEM (internal) — a user-named Deposit is rejected.
    assert_eq!(deposit(&ledger, 0, 100), FailReason::ACCOUNT_NOT_FOUND);
}

#[test]
fn recovery_reconstructs_opened_accounts_from_wal() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = format!("temp_open_account_recovery_{}", nanos);
    let make = || LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.clone(),
            snapshot_frequency: 2,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };

    // Phase 1: open 5 accounts, fund #3, leave #5 opened-but-zero.
    {
        let ledger = started(make());
        ledger.open_accounts(5);
        assert_eq!(deposit(&ledger, 3, 100), FailReason::NONE);
    }

    // Phase 2: restart — recovery replays the WAL (no seal at this size).
    {
        let ledger = started(make());
        assert_eq!(ledger.get_balance(3), 100, "balance must survive restart");
        // #5 was opened with zero balance — existence reconstructed from the
        // replayed AccountOpened high-water, so a deposit succeeds.
        assert_eq!(deposit(&ledger, 5, 10), FailReason::NONE);
        // #6 was never opened — still absent after recovery.
        assert_eq!(deposit(&ledger, 6, 10), FailReason::ACCOUNT_NOT_FOUND);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_accounts_returns_allocated_range() {
    let ledger = started(LedgerConfig::temp());

    let r1 = ledger.open_accounts(10);
    assert_eq!(r1.fail_reason, FailReason::NONE);
    assert_eq!(r1.begin_account_id, 1);
    assert_eq!(r1.count, 10);

    // The next open starts right after the first range (sequential allocation).
    let r2 = ledger.open_accounts(5);
    assert_eq!(r2.fail_reason, FailReason::NONE);
    assert_eq!(r2.begin_account_id, 11);
    assert_eq!(r2.count, 5);
    assert_ne!(r2.tx_id, r1.tx_id);
}

#[test]
fn open_accounts_count_zero_returns_one() {
    let ledger = started(LedgerConfig::temp());
    let r = ledger.open_accounts(0);
    assert_eq!(r.fail_reason, FailReason::NONE);
    assert_eq!(r.begin_account_id, 1);
    assert_eq!(r.count, 1);
}

#[test]
fn open_accounts_tx_carries_account_opened_record() {
    use ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
    use storage::entities::WalEntry;

    let ledger = started(LedgerConfig::temp());
    let r = ledger.open_accounts(7);

    let resp = ledger.query_block(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id: r.tx_id },
        respond: Box::new(|_| {}),
    });
    match resp {
        QueryResponse::Transaction(Some(result)) => {
            assert_eq!(result.meta.tx_id, r.tx_id);
            let opened = result
                .entries
                .iter()
                .find_map(|e| match e {
                    WalEntry::AccountOpened(a) => Some(a),
                    _ => None,
                })
                .expect("AccountOpened record present in the tx");
            assert_eq!(opened.begin_account_id, 1);
            assert_eq!(opened.count, 7);
            assert_eq!(opened.user_ref, 0);
        }
        _ => panic!("expected a Transaction result"),
    }
}

#[test]
fn recovery_reconstructs_opened_accounts_across_snapshot() {
    // Regression (ADR-022): an account opened but never funded must survive a
    // SNAPSHOT-based restart. The open's AccountOpened is compacted into the
    // snapshot, so recovery must restore OPEN status from the snapshot's
    // persisted next_account_id — not merely from the funded balance set.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = format!("temp_open_account_snapshot_recovery_{}", nanos);
    let make = || LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.clone(),
            transaction_count_per_segment: 8, // rotate quickly
            snapshot_frequency: 1,            // snapshot every sealed segment
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    };

    // Phase 1: open 50 (ids 1..=50), fund #3, leave #50 opened-but-zero, then
    // push enough txs to seal + snapshot past the open, then shut down cleanly.
    {
        let ledger = started(make());
        ledger.open_accounts(50);
        assert_eq!(deposit(&ledger, 3, 100), FailReason::NONE);
        let mut last = 0;
        for _ in 0..40 {
            last = ledger.submit(Operation::Deposit {
                account: 3,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last);
        ledger.wait_for_seal();
    }

    // Phase 2: restart — recovery loads the snapshot (open is below its watermark).
    {
        let ledger = started(make());
        assert_eq!(
            ledger.get_balance(3),
            140,
            "funded balance must survive snapshot restart"
        );
        // #50 was opened but never funded — existence comes from the snapshot's
        // next_account_id, so a deposit succeeds.
        assert_eq!(deposit(&ledger, 50, 10), FailReason::NONE);
        // #51 was never opened — still absent after recovery.
        assert_eq!(deposit(&ledger, 51, 10), FailReason::ACCOUNT_NOT_FOUND);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
