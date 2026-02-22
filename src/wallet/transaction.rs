use crate::transaction::{TransactionDataType, TransactionExecutionContext};
use crate::wallet::balance::WalletBalance;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, Default)]
pub struct WalletTransaction {
    pub tag: u64,
    pub account_id: u64,
    pub to_account_id: u64,
    pub amount: u64,
}

impl WalletTransaction {
    pub const DEPOSIT: u64 = 0;
    pub const WITHDRAW: u64 = 1;
    pub const TRANSFER: u64 = 2;

    pub fn deposit(account_id: u64, amount: u64) -> Self {
        Self {
            tag: Self::DEPOSIT,
            account_id,
            to_account_id: 0,
            amount,
        }
    }

    pub fn withdraw(account_id: u64, amount: u64) -> Self {
        Self {
            tag: Self::WITHDRAW,
            account_id,
            to_account_id: 0,
            amount,
        }
    }

    pub fn transfer(from_account_id: u64, to_account_id: u64, amount: u64) -> Self {
        Self {
            tag: Self::TRANSFER,
            account_id: from_account_id,
            to_account_id,
            amount,
        }
    }
}

impl TransactionDataType for WalletTransaction {
    type BalanceData = WalletBalance;

    fn process(
        &self,
        ctx: &mut impl TransactionExecutionContext<Self::BalanceData>,
    ) -> Result<(), String> {
        match self.tag {
            WalletTransaction::DEPOSIT => {
                let mut balance = ctx.get_balance(self.account_id);

                balance.balance += self.amount;
                ctx.update_balance(self.account_id, balance);

                Ok(())
            }
            WalletTransaction::WITHDRAW => {
                let mut balance = ctx.get_balance(self.account_id);

                if self.amount > balance.balance {
                    return Err("Insufficient funds for withdrawal".to_string());
                }

                balance.balance -= self.amount;
                ctx.update_balance(self.account_id, balance);

                Ok(())
            }
            WalletTransaction::TRANSFER => {
                if self.account_id == self.to_account_id {
                    return Ok(());
                }

                let mut from_balance = ctx.get_balance(self.account_id);
                let mut to_balance = ctx.get_balance(self.to_account_id);

                if self.amount > from_balance.balance {
                    return Err("Insufficient funds for transfer".to_string());
                }

                from_balance.balance -= self.amount;
                to_balance.balance += self.amount;

                ctx.update_balance(self.account_id, from_balance);
                ctx.update_balance(self.to_account_id, to_balance);

                Ok(())
            }
            _ => Err(format!("Unknown transaction tag: {}", self.tag)),
        }
    }
}
