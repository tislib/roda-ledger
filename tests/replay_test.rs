use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;
use std::fs;
use std::path::Path;
use std::time::Duration;

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

    // Phase 1: Create some state and a snapshot
    {
        let config = LedgerConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start();

        ledger.submit(Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 0,
        });
        let last_id = ledger.submit(Operation::Deposit {
            account: 2,
            amount: 200,
            user_ref: 0,
        });
        ledger.wait_for_transaction(last_id);

        // Wait for a snapshot to be created
        std::thread::sleep(Duration::from_millis(500));

        // Check balances before "crash"
        assert_eq!(ledger.get_balance(1), 100);
        assert_eq!(ledger.get_balance(2), 200);
    }

    // Give some time for the old threads to hopefully finish or at least not interfere with files
    std::thread::sleep(Duration::from_millis(200));

    {
        let config = LedgerConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_secs(10), // Long interval to avoid snapshotting phase 2
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start();

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

        assert_eq!(ledger.get_balance(1), 180);
        assert_eq!(ledger.get_balance(2), 170);

        // Wait for WAL flush but no snapshot
        std::thread::sleep(Duration::from_millis(200));
    }

    // Wait a bit to simulate a restart
    std::thread::sleep(Duration::from_millis(500));

    // Phase 3: Start a new instance and see if it replayed
    {
        let config = LedgerConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            // Disable auto snapshots for phase 3 to avoid interference
            snapshot_interval: Duration::from_secs(3600),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
        ledger.start(); // This should trigger replay

        // Check balances after replay
        // Account 1: 100 (snapshot) + 50 (WAL) + 30 (WAL) = 180
        // Account 2: 200 (snapshot) - 30 (WAL) = 170
        assert_eq!(ledger.get_balance(1), 180);
        assert_eq!(ledger.get_balance(2), 170);

        // Further transactions should work correctly
        let last_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 20,
            user_ref: 0,
        });
        ledger.wait_for_transaction(last_id);
        assert_eq!(ledger.get_balance(1), 200);
    }

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}
