use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transactor::transaction::Operation;
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::FailReason;

/// Existence enforcement near the snapshot capacity (`max_accounts`).
/// The old open-ceiling is gone: opening a large count succeeds, a deposit
/// into an opened id works, and a deposit into an unopened id that is still
/// below `max_accounts` is rejected with ACCOUNT_NOT_FOUND. (Depositing into
/// an id >= max_accounts is unsupported — the snapshot Vec is sized to it —
/// so this test stays strictly below the cap.)
#[test]
fn test_max_accounts_boundary() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    // Opening 90 accounts (ids 1..=90) succeeds — there is no open ceiling.
    let opened = ledger.open_accounts(90);
    assert_eq!(opened.fail_reason, FailReason::NONE, "open should succeed");

    // Account 90 (opened, < max_accounts) should accept a deposit.
    let ok_result = ledger.submit_and_wait_result(Operation::Deposit {
        account: 90,
        amount: 1,
        user_ref: 0,
    });
    assert!(!ok_result.is_err(), "opened account 90 should be valid");
    assert_eq!(ledger.get_balance(90), 1);

    // Account 95 was never opened (still < max_accounts) → ACCOUNT_NOT_FOUND.
    let fail_result = ledger.submit_and_wait_result(Operation::Deposit {
        account: 95,
        amount: 1,
        user_ref: 0,
    });
    assert!(
        fail_result.is_err(),
        "unopened account 95 should be rejected"
    );
    assert_eq!(fail_result.get_fail_reason(), FailReason::ACCOUNT_NOT_FOUND);
    assert_eq!(ledger.get_balance(95), 0);
}

/// Deposit of amount 0 - verify defined behavior.
#[test]
fn test_zero_amount_deposit() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    ledger.open_accounts(1); // id 1

    let id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 0,
        user_ref: 0,
    });
    ledger.wait_for_transaction(id);

    // Zero-amount deposit should not change balance
    assert_eq!(ledger.get_balance(1), 0);
}

/// Simplest case: 1 deposit, verify balance.
#[test]
fn test_single_transaction() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    ledger.open_accounts(1); // id 1

    let id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 42,
        user_ref: 0,
    });
    ledger.wait_for_transaction(id);

    assert_eq!(ledger.get_balance(1), 42);
    assert_eq!(ledger.get_balance(0), -42);
}

/// 1MB segments, submit enough to cross boundary. Verify no data loss on restart.
#[test]
fn test_segment_exact_boundary_rotation() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = format!("temp_boundary_rotation_{}", nanos);

    let total_deposits = 20_000u64; // ~20K * 80B (2 records) = ~1.6MB -> triggers rotation

    // Phase 1: submit and wait
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 1000,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();
        ledger.open_accounts(1); // id 1 — reconstructed from the WAL on restart

        let mut last_id = 0u64;
        for _ in 0..total_deposits {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Phase 2: restart and verify
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                transaction_count_per_segment: 100,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            total_deposits as i64,
            "balance mismatch after segment rotation + restart"
        );
    }

    let _ = std::fs::remove_dir_all(dir);
}

/// Transfer from account to itself should produce defined behavior.
#[test]
fn test_self_transfer() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    ledger.open_accounts(3); // ids 1..=3 (uses 3)

    // Fund account 3
    let dep_id = ledger.submit(Operation::Deposit {
        account: 3,
        amount: 100,
        user_ref: 0,
    });
    ledger.wait_for_transaction(dep_id);

    // Self-transfer
    let xfer_id = ledger.submit(Operation::Transfer {
        from: 3,
        to: 3,
        amount: 50,
        user_ref: 0,
    });
    ledger.wait_for_transaction(xfer_id);

    // Balance should remain 100 (self-transfer is a no-op)
    assert_eq!(ledger.get_balance(3), 100);
}

/// Transfer with insufficient funds is rejected, balances unchanged.
#[test]
fn test_transfer_insufficient_funds() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();
    ledger.open_accounts(2); // ids 1..=2

    let dep_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 50,
        user_ref: 0,
    });
    ledger.wait_for_transaction(dep_id);

    let xfer_result = ledger.submit_and_wait_result(Operation::Transfer {
        from: 1,
        to: 2,
        amount: 100, // more than balance
        user_ref: 0,
    });

    assert!(
        xfer_result.is_err(),
        "transfer with insufficient funds should be rejected"
    );
    assert_eq!(ledger.get_balance(1), 50, "from balance unchanged");
    assert_eq!(ledger.get_balance(2), 0, "to balance unchanged");
}
