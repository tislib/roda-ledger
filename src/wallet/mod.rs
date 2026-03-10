pub mod balance;
pub mod transaction;

use crate::ledger::{Ledger, LedgerConfig};
use crate::transaction::{Transaction, TransactionStatus};
use crate::wallet::balance::WalletBalance;
use crate::wallet::transaction::WalletTransaction;

#[derive(Clone)]
pub struct WalletConfig {
    pub capacity: usize,
    pub location: Option<String>,
    pub in_memory: bool,
    pub snapshot_interval: std::time::Duration,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            location: None,
            in_memory: false,
            snapshot_interval: std::time::Duration::from_secs(600),
        }
    }
}

pub struct Wallet {
    ledger: Ledger<WalletTransaction, WalletBalance>,
    last_transaction_id: u64,
    location: Option<String>,
}

impl Wallet {
    pub fn new(capacity: usize) -> Self {
        Self::new_with_config(WalletConfig {
            capacity,
            ..Default::default()
        })
    }

    pub fn new_with_config(config: WalletConfig) -> Self {
        // Resolve location: if not provided and not in-memory, create a random folder in temp dir
        let resolved_location = if !config.in_memory {
            match &config.location {
                Some(loc) => Some(loc.clone()),
                None => {
                    let mut dir = std::env::temp_dir();
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos();
                    dir.push(format!("roda-ledger-{}", nanos));
                    let _ = std::fs::create_dir_all(&dir);
                    Some(dir.to_string_lossy().to_string())
                }
            }
        } else {
            None
        };

        let ledger_config = LedgerConfig {
            capacity: config.capacity,
            location: resolved_location.clone(),
            in_memory: config.in_memory,
            snapshot_interval: config.snapshot_interval,
        };
        Self {
            ledger: Ledger::new(ledger_config),
            last_transaction_id: 0,
            location: resolved_location,
        }
    }

    pub fn submit(&mut self, transaction: Transaction<WalletTransaction, WalletBalance>) -> u64 {
        let transaction_id = self.ledger.submit(transaction);
        self.last_transaction_id = transaction_id;
        transaction_id
    }

    pub fn deposit(&mut self, account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::deposit(account_id, amount);
        let tx = Transaction::new(tx_data);
        let transaction_id = self.ledger.submit(tx);
        self.last_transaction_id = transaction_id;
        transaction_id
    }

    pub fn withdraw(&mut self, account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::withdraw(account_id, amount);
        let tx = Transaction::new(tx_data);
        let transaction_id = self.ledger.submit(tx);
        self.last_transaction_id = transaction_id;
        transaction_id
    }

    pub fn transfer(&mut self, from_account_id: u64, to_account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::transfer(from_account_id, to_account_id, amount);
        let tx = Transaction::new(tx_data);
        let transaction_id = self.ledger.submit(tx);
        self.last_transaction_id = transaction_id;
        transaction_id
    }

    pub fn get_balance(&self, account_id: u64) -> WalletBalance {
        self.ledger.get_balance(account_id)
    }

    pub fn wait_pending_operations(&mut self) {
        self.ledger.wait_for_transaction(self.last_transaction_id);
    }

    pub fn get_transaction_status(&self, transaction_id: u64) -> TransactionStatus {
        self.ledger.get_transaction_status(transaction_id)
    }

    pub fn start(&mut self) {
        self.ledger.start();
    }

    /// Destroys the wallet by stopping its internal threads and removing its data directory.
    /// If the wallet was created in-memory or has no associated location, this is a no-op for filesystem.
    pub fn destroy(self) {
        let location = self.location.clone();
        // Drop ledger explicitly to ensure all background threads are stopped and files are closed
        drop(self.ledger);
        if let Some(loc) = location {
            let _ = std::fs::remove_dir_all(loc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_persistence() {
        // Create a wallet with a random temp location (default behavior when location is None)
        let mut wallet1 = Wallet::new(10);
        wallet1.start();
        wallet1.deposit(1, 150);
        wallet1.wait_pending_operations();

        // Capture the location to reuse for the next wallet
        let location = wallet1.location.clone();
        assert!(location.is_some());

        // Drop the first wallet to stop threads but keep data intact
        drop(wallet1);

        // Create a second wallet pointing to the same location to test persistence
        let mut wallet2 = Wallet::new_with_config(WalletConfig {
            capacity: 10,
            location: location.clone(),
            in_memory: false,
            ..Default::default()
        });
        wallet2.start();

        // Balance should persist
        assert_eq!(wallet2.get_balance(1).balance, 150);

        // Cleanup
        wallet2.destroy();
    }

    #[test]
    fn test_wallet_deposit() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_withdraw() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.withdraw(1, 50);
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 50);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_transfer() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.deposit(2, 50);

        wallet.transfer(1, 2, 30);
        wallet.wait_pending_operations();

        let balance1 = wallet.get_balance(1);
        let balance2 = wallet.get_balance(2);

        assert_eq!(balance1.balance, 70);
        assert_eq!(balance2.balance, 80);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_insufficient_funds_withdraw() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        let failed_transaction_id = wallet.withdraw(1, 150); // Should fail
        wallet.deposit(1, 50); // This should still work if transactor is alive
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 150); // 100 + 50

        let failed_transaction_status = wallet.get_transaction_status(failed_transaction_id);

        assert!(failed_transaction_status.is_err());

        wallet.destroy();
    }

    #[test]
    fn test_wallet_insufficient_funds_transfer() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.deposit(2, 50);

        let failed_transaction_id = wallet.transfer(1, 2, 150); // Should fail
        wallet.wait_pending_operations();

        let balance1 = wallet.get_balance(1);
        let balance2 = wallet.get_balance(2);

        assert_eq!(balance1.balance, 100);
        assert_eq!(balance2.balance, 50);

        let failed_transaction_status = wallet.get_transaction_status(failed_transaction_id);
        assert!(failed_transaction_status.is_err());

        wallet.destroy();
    }

    #[test]
    fn test_wallet_multi_account() {
        let mut wallet = Wallet::new(100);
        wallet.start();

        for i in 1..=10 {
            wallet.deposit(i as u64, 1000);
        }

        for i in 1..10 {
            wallet.transfer(i as u64, (i + 1) as u64, 100);
        }
        wallet.wait_pending_operations();

        assert_eq!(wallet.get_balance(1).balance, 900);
        for i in 2..10 {
            assert_eq!(wallet.get_balance(i).balance, 1000); // Received 100, sent 100
        }
        assert_eq!(wallet.get_balance(10).balance, 1100);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_zero_amount() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.deposit(1, 0);
        wallet.withdraw(1, 0);
        wallet.transfer(1, 2, 0);
        wallet.wait_pending_operations();

        assert_eq!(wallet.get_balance(1).balance, 100);
        assert_eq!(wallet.get_balance(2).balance, 0);

        wallet.destroy();
    }

    #[test]
    fn test_successful_transaction_status() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        let tx_id = wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let status = wallet.get_transaction_status(tx_id);
        assert!(status.is_ok());
        assert!(status.is_committed());

        wallet.destroy();
    }

    #[test]
    fn test_wallet_transfer_to_self() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.transfer(1, 1, 30); // Transfer to self
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100); // Should remain 100

        wallet.destroy();
    }

    #[test]
    fn test_wallet_non_existent_account() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.withdraw(999, 100); // Withdraw from non-existent account
        wallet.transfer(998, 1, 100); // Transfer from non-existent account
        wallet.wait_pending_operations();

        assert_eq!(wallet.get_balance(999).balance, 0);
        assert_eq!(wallet.get_balance(998).balance, 0);
        assert_eq!(wallet.get_balance(1).balance, 0);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_massive_scenario() {
        let num_accounts = 50;
        let transactions_per_account = 20;
        let mut wallet = Wallet::new(num_accounts * transactions_per_account * 2);
        wallet.start();

        // Initial deposits
        for i in 1..=num_accounts as u64 {
            wallet.deposit(i, 1000);
        }

        // Random-ish transfers
        for _ in 0..transactions_per_account {
            for i in 1..=num_accounts as u64 {
                let to = (i % num_accounts as u64) + 1;
                if i != to {
                    wallet.transfer(i, to, 10);
                }
            }
        }

        wallet.wait_pending_operations();

        // Check total balance (should be preserved)
        let mut total_balance = 0;
        for i in 1..=num_accounts as u64 {
            total_balance += wallet.get_balance(i).balance;
        }
        assert_eq!(total_balance, num_accounts as u64 * 1000);

        wallet.destroy();
    }

    #[test]
    fn test_wallet_in_memory() {
        let mut wallet = Wallet::new_with_config(WalletConfig {
            capacity: 10,
            in_memory: true,
            ..Default::default()
        });
        wallet.start();
        wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100);

        // Verify no "data" folder or "wal.bin" was created in current directory if it didn't exist
        // But since other tests might have created it, it's hard to verify here without a temp dir.

        wallet.destroy();
    }

    #[test]
    fn test_wallet_custom_location() {
        let temp_dir = "temp_ledger_test";
        let mut wallet = Wallet::new_with_config(WalletConfig {
            capacity: 10,
            location: Some(temp_dir.to_string()),
            in_memory: false,
            ..Default::default()
        });
        wallet.start();
        wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100);

        // Verify file exists
        let wal_path = std::path::Path::new(temp_dir).join("wal.bin");
        assert!(wal_path.exists());

        // Cleanup via destroy
        wallet.destroy();
    }
}
