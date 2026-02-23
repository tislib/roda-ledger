pub mod balance;
pub mod transaction;

use crate::ledger::Ledger;
use crate::transaction::Transaction;
use crate::wallet::balance::WalletBalance;
use crate::wallet::transaction::WalletTransaction;

pub struct Wallet {
    ledger: Ledger<WalletTransaction, WalletBalance>,
    op_count: u64,
}

impl Wallet {
    pub fn new(capacity: usize) -> Self {
        Self {
            ledger: Ledger::new(capacity, None),
            op_count: 0,
        }
    }

    pub fn deposit(&mut self, account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::deposit(account_id, amount);
        let tx = Transaction::new(tx_data);
        self.op_count += 1;
        self.ledger.submit(tx)
    }

    pub fn withdraw(&mut self, account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::withdraw(account_id, amount);
        let tx = Transaction::new(tx_data);
        self.op_count += 1;
        self.ledger.submit(tx)
    }

    pub fn transfer(&mut self, from_account_id: u64, to_account_id: u64, amount: u64) -> u64 {
        let tx_data = WalletTransaction::transfer(from_account_id, to_account_id, amount);
        let tx = Transaction::new(tx_data);
        self.op_count += 1;
        self.ledger.submit(tx)
    }

    pub fn get_balance(&self, account_id: u64) -> WalletBalance {
        self.ledger.get_balance(account_id)
    }

    pub fn wait_pending_operations(&mut self) {
        self.ledger.tick(self.op_count);
    }

    pub fn start(&mut self) {
        self.ledger.start();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_deposit() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100);
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
    }

    #[test]
    fn test_wallet_insufficient_funds_withdraw() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.withdraw(1, 150); // Should fail
        wallet.deposit(1, 50); // This should still work if transactor is alive
        wallet.wait_pending_operations();

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 150); // 100 + 50
    }

    #[test]
    fn test_wallet_insufficient_funds_transfer() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 100);
        wallet.deposit(2, 50);

        wallet.transfer(1, 2, 150); // Should fail
        wallet.wait_pending_operations();

        let balance1 = wallet.get_balance(1);
        let balance2 = wallet.get_balance(2);

        assert_eq!(balance1.balance, 100);
        assert_eq!(balance2.balance, 50);
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
    }
}
