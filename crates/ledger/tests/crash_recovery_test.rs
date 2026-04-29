use ledger::ledger::{Ledger, LedgerConfig};
use storage::StorageConfig;
use ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

#[test]
fn crash_recovery_test() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_crash_recovery_test_{}", nanos);
    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let account_id = 1;
    let num_transactions = 1_000_000;
    let deposit_amount = 1;

    // Phase 1: Insert transactions — WAL replay will be used on restart (no snapshot at 64MB segments)
    {
        let config = LedgerConfig {
            queue_size: 1024,
            max_accounts: 1_000_000,
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_tx_id = 0;
        for _ in 0..num_transactions {
            last_tx_id = ledger.submit(Operation::Deposit {
                account: account_id,
                amount: deposit_amount,
                user_ref: 0,
            });
        }

        // Wait until transactions reach WAL
        loop {
            let status = ledger.get_transaction_status(last_tx_id);
            if status.is_committed() {
                break;
            }
            std::thread::yield_now();
        }
    }

    // Phase 2: Start again and add more transactions
    {
        let config = LedgerConfig {
            queue_size: 1024,
            max_accounts: 1_000_000,
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap(); // triggers WAL replay

        let mut last_tx_id = 0;
        for _ in 0..num_transactions {
            last_tx_id = ledger.submit(Operation::Deposit {
                account: account_id,
                amount: deposit_amount,
                user_ref: 0,
            });
        }

        loop {
            let status = ledger.get_transaction_status(last_tx_id);
            if status.is_committed() {
                break;
            }
            std::thread::yield_now();
        }
    }

    // Phase 3: Start again and verify
    {
        let config = LedgerConfig {
            queue_size: 1024,
            max_accounts: 1_000_000,
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                snapshot_frequency: 2,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap(); // triggers WAL replay

        let balance = ledger.get_balance(account_id);

        // Verify balance: 2 phases × 1000 transactions × 1 deposit each
        assert_eq!(balance, (2 * num_transactions * deposit_amount) as i64);
    }

    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
