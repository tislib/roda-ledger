use crate::entities::FailReason;
use crate::transaction::{TransactionDataType, TransactionExecutionContext};
use bytemuck::{Pod, Zeroable};

pub const SYSTEM_ACCOUNT_ID: u64 = 0;

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
    fn process(&self, ctx: &mut TransactionExecutionContext<'_>) {
        if self.amount == 0 {
            return;
        }

        match self.tag {
            WalletTransaction::DEPOSIT => {
                ctx.debit(self.account_id, self.amount);
                ctx.credit(SYSTEM_ACCOUNT_ID, self.amount);
            }
            WalletTransaction::WITHDRAW => {
                let balance = ctx.get_balance(self.account_id);
                if balance < self.amount as i64 {
                    ctx.fail(FailReason::INSUFFICIENT_FUNDS);
                    return;
                }
                ctx.credit(self.account_id, self.amount);
                ctx.debit(SYSTEM_ACCOUNT_ID, self.amount);
            }
            WalletTransaction::TRANSFER => {
                if self.account_id == self.to_account_id {
                    return;
                }
                let balance = ctx.get_balance(self.account_id);
                if balance < self.amount as i64 {
                    ctx.fail(FailReason::INSUFFICIENT_FUNDS);
                    return;
                }
                ctx.credit(self.account_id, self.amount);
                ctx.debit(self.to_account_id, self.amount);
            }
            _ => ctx.fail(FailReason::INVALID_OPERATION),
        }
    }
}
