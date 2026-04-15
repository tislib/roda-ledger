use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::Operation;
use std::fs;
use std::time::Duration;

fn unique_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temp_{}_{}", name, nanos)
}

fn make_config(dir: &str) -> LedgerConfig {
    LedgerConfig {
        storage: StorageConfig {
            data_dir: dir.to_string(),
            transaction_count_per_segment: 100,
            snapshot_frequency: 1,
            ..Default::default()
        },
        seal_check_internal: Duration::from_millis(1),
        ..Default::default()
    }
}

/// Duplicate user_ref within the window is rejected.
/// Only the first deposit should affect the balance.
#[test]
fn test_duplicate_user_ref_rejected_with_dedup() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 42,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 42, // same user_ref → DUPLICATE
    });
    ledger.wait_for_transaction(id2);

    assert_ne!(id1, id2, "tx IDs must be different");
    assert_eq!(
        ledger.get_balance(1),
        100,
        "only first deposit should affect balance; second is DUPLICATE"
    );
}

/// With user_ref=0, duplicate submissions are both processed (dedup bypassed).
#[test]
fn test_duplicate_user_ref_both_processed_when_user_ref_zero() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    ledger.wait_for_transaction(id2);

    assert_ne!(id1, id2, "tx IDs must be different");
    assert_eq!(
        ledger.get_balance(1),
        200,
        "both deposits should process when user_ref is 0 (dedup bypassed)"
    );
}

/// user_ref=0 bypasses dedup check.
#[test]
fn test_zero_user_ref_bypasses_dedup() {
    let config = LedgerConfig {
        seal_check_internal: Duration::from_millis(10),
        ..LedgerConfig::temp()
    };
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let _id1 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    let id2 = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 100,
        user_ref: 0,
    });
    ledger.wait_for_transaction(id2);

    assert_eq!(ledger.get_balance(1), 200, "user_ref=0 should bypass dedup");
}

/// After crash+restart, committed transactions are not re-executed.
#[test]
fn test_restart_no_reprocessing() {
    let dir = unique_dir("restart_no_reprocess");
    let initial = 10_000u64;

    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..initial {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Restart and submit one more
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let new_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
        ledger.wait_for_transaction(new_id);

        // Balance should be initial + 1, not 2*initial + 1
        assert_eq!(
            ledger.get_balance(1),
            (initial + 1) as i64,
            "committed transactions must not be re-executed on restart"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// Restart 3 times without new transactions, balance must be identical each time.
#[test]
fn test_recovery_idempotent_multiple_restarts() {
    let dir = unique_dir("recovery_idempotent");

    // Initial data
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..10_000 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Restart 3 times, verify balance each time
    for restart in 0..3 {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            10_000,
            "balance changed on restart #{} without new transactions",
            restart + 1
        );
    }

    let _ = fs::remove_dir_all(dir);
}

/// tx_id sequence continues correctly across restarts.
#[test]
fn test_tx_id_continuity_across_restarts() {
    let dir = unique_dir("txid_continuity");

    let first_last_id;
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        let mut last_id = 0u64;
        for _ in 0..100 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
        first_last_id = last_id;
    }

    // Second session
    let second_first_id;
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();

        second_first_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });

        let mut last_id = second_first_id;
        for _ in 1..100 {
            last_id = ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_id);
    }

    assert!(
        second_first_id > first_last_id,
        "second session first ID {} must be > first session last ID {}",
        second_first_id,
        first_last_id
    );

    // Third session: verify accumulated balance
    {
        let mut ledger = Ledger::new(make_config(&dir));
        ledger.start().unwrap();
        assert_eq!(ledger.get_balance(1), 200);
    }

    let _ = fs::remove_dir_all(dir);
}
