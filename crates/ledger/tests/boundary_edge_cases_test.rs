use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::time::Duration;
use storage::StorageConfig;
use storage::entities::FailReason;

/// Account at max_accounts fails with ACCOUNT_LIMIT_EXCEEDED.
#[test]
fn test_max_accounts_boundary() {
    let config = LedgerConfig {
        max_accounts: 100,
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    // Account 99 (last valid) should succeed
    let ok_id = ledger.submit(Operation::Deposit {
        account: 99,
        amount: 1,
        user_ref: 0,
    });
    ledger.wait_for_transaction(ok_id);
    let status = ledger.get_transaction_status(ok_id);
    assert!(status.is_ok(), "account 99 should be valid");
    assert_eq!(ledger.get_balance(99), 1);

    // Account 100 (out of bounds) should fail
    let fail_id = ledger.submit(Operation::Deposit {
        account: 100,
        amount: 1,
        user_ref: 0,
    });
    ledger.wait_for_transaction(fail_id);
    let status = ledger.get_transaction_status(fail_id);
    assert!(status.is_err(), "account 100 should be rejected");
    assert_eq!(status.error_reason(), FailReason::ACCOUNT_LIMIT_EXCEEDED);
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
                transaction_count_per_segment: 100,
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

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

    let dep_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 50,
        user_ref: 0,
    });
    ledger.wait_for_transaction(dep_id);

    let xfer_id = ledger.submit(Operation::Transfer {
        from: 1,
        to: 2,
        amount: 100, // more than balance
        user_ref: 0,
    });
    ledger.wait_for_transaction(xfer_id);

    let status = ledger.get_transaction_status(xfer_id);
    assert!(
        status.is_err(),
        "transfer with insufficient funds should be rejected"
    );
    assert_eq!(ledger.get_balance(1), 50, "from balance unchanged");
    assert_eq!(ledger.get_balance(2), 0, "to balance unchanged");
}
