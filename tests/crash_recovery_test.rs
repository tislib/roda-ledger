use roda_ledger::wallet::{Wallet, WalletConfig};
use std::fs;
use std::path::Path;
use std::time::Duration;

#[test]
fn crash_recovery_test() {
    let temp_dir = "temp_crash_recovery_test";
    if Path::new(temp_dir).exists() {
        let _ = fs::remove_dir_all(temp_dir);
    }

    let account_id = 1;
    let num_transactions = 1_000_000;
    let deposit_amount = 1;

    // Phase 1: Insert 1 million transactions
    {
        let config = WalletConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_millis(100),
            capacity: 100_000,
        };

        let mut wallet = Wallet::new_with_config(config);
        wallet.start();

        let mut last_tx_id = 0;
        for _ in 0..num_transactions {
            last_tx_id = wallet.deposit(account_id, deposit_amount);
        }

        // Wait until 1M transactions reach WAL by checking status
        loop {
            let status = wallet.get_transaction_status(last_tx_id);
            if status.is_committed() {
                break;
            }
            std::thread::yield_now();
        }

        // Ledger is dropped here when it goes out of scope
    }

    // Phase 2: Start again and verify
    {
        let config = WalletConfig {
            location: Some(temp_dir.to_string()),
            in_memory: false,
            snapshot_interval: Duration::from_millis(100),
            capacity: 100_000,
        };

        let mut wallet = Wallet::new_with_config(config);
        wallet.start(); // This should trigger replay

        let balance = wallet.get_balance(account_id);

        // Verify balance
        assert_eq!(balance.balance, num_transactions * deposit_amount);

        // Final cleanup
        wallet.destroy();
    }
}
