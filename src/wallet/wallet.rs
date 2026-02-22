use crate::ledger::Ledger;
use crate::transaction::Transaction;
use crate::wallet::balance::WalletBalance;
use crate::wallet::transaction::WalletTransaction;

pub struct Wallet {
    pub ledger: Ledger<WalletTransaction, WalletBalance>,
}

impl Wallet {
    pub fn new(capacity: usize) -> Self {
        Self {
            ledger: Ledger::new(capacity),
        }
    }

    pub fn deposit(&self, id: u64, account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::Deposit { account_id, amount };
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn withdraw(&self, id: u64, account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::Withdraw { account_id, amount };
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn transfer(&self, id: u64, from_account_id: u64, to_account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::Transfer { from_account_id, to_account_id, amount };
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn get_balance(&self, account_id: u64) -> Result<WalletBalance, String> {
        self.ledger.get_balance(account_id)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use super::*;

    #[test]
    fn test_wallet_deposit() {
        let wallet = Wallet::new(10);
        wallet.deposit(1, 1, 100);
        
        // Wait for background processing
        thread::sleep(Duration::from_millis(10));

        let balance = wallet.get_balance(1).unwrap();
        assert_eq!(balance.balance, 100);
    }

    #[test]
    fn test_wallet_withdraw() {
        let wallet = Wallet::new(10);
        wallet.deposit(1, 1, 100);
        thread::sleep(Duration::from_millis(10));

        wallet.withdraw(2, 1, 50);
        thread::sleep(Duration::from_millis(10));

        let balance = wallet.get_balance(1).unwrap();
        assert_eq!(balance.balance, 50);
    }

    #[test]
    fn test_wallet_transfer() {
        let wallet = Wallet::new(10);
        wallet.deposit(1, 1, 100);
        wallet.deposit(2, 2, 50);
        thread::sleep(Duration::from_millis(10));

        wallet.transfer(3, 1, 2, 30);
        thread::sleep(Duration::from_millis(10));

        let balance1 = wallet.get_balance(1).unwrap();
        let balance2 = wallet.get_balance(2).unwrap();
        
        assert_eq!(balance1.balance, 70);
        assert_eq!(balance2.balance, 80);
    }
}
