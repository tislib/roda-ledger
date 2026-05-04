use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;
use storage::StorageConfig;

/// Force segment rotation and snapshot:
///   100 tx/segment + snapshot_frequency=1 ensures snapshot after first seal.
const SMALL_SEGMENT_TX_COUNT: u64 = 100;
const SNAP_EVERY_SEAL: u32 = 1;

#[test]
fn test_replay_functionality() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_dir = format!("temp_replay_test_{}", nanos);
    if Path::new(&temp_dir).exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    // Phase 1: Create some state, trigger a snapshot via WAL segment seal
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                transaction_count_per_segment: SMALL_SEGMENT_TX_COUNT,
                snapshot_frequency: SNAP_EVERY_SEAL,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Submit enough transactions to trigger a WAL seal and snapshot
        for _ in 0..35_000 {
            ledger.submit(Operation::Deposit {
                account: 1,
                amount: 1,
                user_ref: 0,
            });
        }
        let last_id = ledger.submit(Operation::Deposit {
            account: 2,
            amount: 200,
            user_ref: 0,
        });
        ledger.wait_for_transaction(last_id);

        assert_eq!(ledger.get_balance(1), 35_000);
        assert_eq!(ledger.get_balance(2), 200);
    }

    // Phase 2: Restart, add more transactions (no new snapshot expected)
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                transaction_count_per_segment: SMALL_SEGMENT_TX_COUNT,
                snapshot_frequency: 100,
                ..Default::default()
            },
            // High frequency → no new snapshot during phase 2 short run
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        // Balances restored from snapshot + WAL replay
        assert_eq!(
            ledger.get_balance(1),
            35_000,
            "Account 1 should be restored"
        );
        assert_eq!(ledger.get_balance(2), 200, "Account 2 should be restored");

        ledger.submit(Operation::Deposit {
            account: 1,
            amount: 50,
            user_ref: 0,
        });
        let last_id = ledger.submit(Operation::Transfer {
            from: 2,
            to: 1,
            amount: 30,
            user_ref: 0,
        });
        ledger.wait_for_transaction(last_id);

        assert_eq!(ledger.get_balance(1), 35_080);
        assert_eq!(ledger.get_balance(2), 170);
    }

    // Phase 3: Restart again and verify everything is replayed correctly
    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: temp_dir.to_string(),
                transaction_count_per_segment: SMALL_SEGMENT_TX_COUNT,
                snapshot_frequency: 100,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        assert_eq!(
            ledger.get_balance(1),
            35_080,
            "Account 1 balance should survive restart"
        );
        assert_eq!(
            ledger.get_balance(2),
            170,
            "Account 2 balance should survive restart"
        );

        // New transactions after replay should work correctly
        let last_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 20,
            user_ref: 0,
        });
        ledger.wait_for_transaction(last_id);
        assert_eq!(ledger.get_balance(1), 35_100);
    }

    let _ = fs::remove_dir_all(temp_dir);
}
