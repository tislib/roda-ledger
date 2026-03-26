use roda_ledger::wallet::{Wallet, WalletConfig};
use std::fs;
use std::path::Path;
use std::time::Duration;

#[test]
fn test_replay_functionality() {
    let temp_dir = "temp_replay_test";
    if Path::new(temp_dir).exists() {
        let _ = fs::remove_dir_all(temp_dir);
    }

    // Phase 1: Create some state and a snapshot
    {
        let config = WalletConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let mut wallet = Wallet::new_with_config(config);
        wallet.start();

        wallet.deposit(1, 100);
        wallet.deposit(2, 200);
        wallet.wait_pending_operations();

        // Wait for a snapshot to be created
        std::thread::sleep(Duration::from_millis(500));

        // Check balances before "crash"
        assert_eq!(wallet.get_balance(1), 100);
        assert_eq!(wallet.get_balance(2), 200);

        // Phase 2: Create some more state that won't be in the snapshot, but will be in WAL
        // We set a long snapshot interval for the next phase, but we can't easily change it.
        // So we just perform transactions and crash quickly.
    }

    // Give some time for the old threads to hopefully finish or at least not interfere with files
    std::thread::sleep(Duration::from_millis(200));

    {
        let config = WalletConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_secs(10), // Long interval to avoid snapshotting phase 2
            ..Default::default()
        };

        let mut wallet = Wallet::new_with_config(config);
        wallet.start();

        wallet.deposit(1, 50);
        wallet.transfer(2, 1, 30);
        wallet.wait_pending_operations();

        assert_eq!(wallet.get_balance(1), 180);
        assert_eq!(wallet.get_balance(2), 170);

        // Wait for WAL flush but no snapshot
        std::thread::sleep(Duration::from_millis(200));
    }

    // Wait a bit to simulate a restart
    std::thread::sleep(Duration::from_millis(500));

    // Phase 3: Start a new instance and see if it replayed
    {
        let config = WalletConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            // Disable auto snapshots for phase 3 to avoid interference
            snapshot_interval: Duration::from_secs(3600),
            ..Default::default()
        };

        let mut wallet = Wallet::new_with_config(config);
        wallet.start(); // This should trigger replay

        // Check balances after replay
        // Account 1: 100 (snapshot) + 50 (WAL) + 30 (WAL) = 180
        // Account 2: 200 (snapshot) - 30 (WAL) = 170
        assert_eq!(wallet.get_balance(1), 180);
        assert_eq!(wallet.get_balance(2), 170);

        // Further transactions should work correctly
        wallet.deposit(1, 20);
        wallet.wait_pending_operations();
        assert_eq!(wallet.get_balance(1), 200);
    }

    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}
