use crate::transaction::{TransactionDataType, TransactionExecutionContext};
use crate::wallet::balance::WalletBalance;

pub enum WalletTransaction {
    Deposit {
        account_id: u64,
        amount: u64,
    },
    Withdraw {
        account_id: u64,
        amount: u64,
    },
    Transfer {
        from_account_id: u64,
        to_account_id: u64,
        amount: u64,
    },
}

impl TransactionDataType for WalletTransaction {
    type BalanceData = WalletBalance;

    fn process(
        &self,
        ctx: &impl TransactionExecutionContext<Self::BalanceData>,
    ) -> Result<(), String> {
        match self {
            WalletTransaction::Deposit { account_id, amount } => {
                let mut balance = ctx.get_balance(*account_id)?;

                balance.balance += amount;
                ctx.update_balance(*account_id, balance)?;

                Ok(())
            }
            WalletTransaction::Withdraw { account_id, amount } => {
                let mut balance = ctx.get_balance(*account_id)?;

                balance.balance -= amount;
                ctx.update_balance(*account_id, balance)?;

                Ok(())
            }
            WalletTransaction::Transfer {
                from_account_id,
                to_account_id,
                amount,
            } => {
                let mut from_balance = ctx.get_balance(*from_account_id)?;
                let mut to_balance = ctx.get_balance(*to_account_id)?;

                if *amount > from_balance.balance {
                    return Err("Insufficient funds for transfer".to_string());
                }

                from_balance.balance -= amount;
                to_balance.balance += amount;

                ctx.update_balance(*from_account_id, from_balance)?;
                ctx.update_balance(*to_account_id, to_balance)?;

                Ok(())
            }
        }
    }
}
