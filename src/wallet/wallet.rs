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
            ledger: Ledger::new(capacity, None),
        }
    }

    pub fn deposit(&mut self, id: u64, account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::deposit(account_id, amount);
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn withdraw(&mut self, id: u64, account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::withdraw(account_id, amount);
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn transfer(&mut self, id: u64, from_account_id: u64, to_account_id: u64, amount: u64) {
        let tx_data = WalletTransaction::transfer(from_account_id, to_account_id, amount);
        let tx = Transaction::new(id, tx_data);
        self.ledger.submit(tx);
    }

    pub fn get_balance(&self, account_id: u64) -> WalletBalance {
        self.ledger.get_balance(account_id)
    }

    pub fn tick(&mut self, tick_count: i32) {
        self.ledger.tick(tick_count);
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
        wallet.deposit(1, 1, 100);
        wallet.tick(1);

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 100);
    }

    #[test]
    fn test_wallet_withdraw() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 1, 100);
        wallet.withdraw(2, 1, 50);
        wallet.tick(2);

        let balance = wallet.get_balance(1);
        assert_eq!(balance.balance, 50);
    }

    #[test]
    fn test_wallet_transfer() {
        let mut wallet = Wallet::new(10);
        wallet.start();
        wallet.deposit(1, 1, 100);
        wallet.deposit(2, 2, 50);

        wallet.transfer(3, 1, 2, 30);
        wallet.tick(3);

        let balance1 = wallet.get_balance(1);
        let balance2 = wallet.get_balance(2);
        
        assert_eq!(balance1.balance, 70);
        assert_eq!(balance2.balance, 80);
    }
}
