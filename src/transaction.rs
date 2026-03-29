use crate::balance::Balance;
use crate::entities::{EntryKind, FailReason, TxEntry};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Transfer {
        from: u64,
        to: u64,
        amount: u64,
        user_ref: u64,
    },
    Deposit {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Withdrawal {
        account: u64,
        amount: u64,
        user_ref: u64,
    },
    Composite(Box<CompositeOperation>),
    Named {
        name: String,
        params: Vec<i64>,
        user_ref: u64,
    },
}

impl Operation {
    pub fn user_ref(&self) -> u64 {
        match self {
            Operation::Transfer { user_ref, .. } => *user_ref,
            Operation::Deposit { user_ref, .. } => *user_ref,
            Operation::Withdrawal { user_ref, .. } => *user_ref,
            Operation::Composite(op) => op.user_ref,
            Operation::Named { user_ref, .. } => *user_ref,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompositeOperation {
    pub steps: SmallVec<[Step; 8]>,
    pub flags: CompositeOperationFlags,
    pub user_ref: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Step {
    Credit { account_id: u64, amount: u64 },
    Debit { account_id: u64, amount: u64 },
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub struct CompositeOperationFlags: u64 {
        const CHECK_NEGATIVE_BALANCE = 0b00000001;
    }
}

pub struct TransactionExecutionContext<'a> {
    pub(crate) balances: &'a mut [Balance],
    pub(crate) entries: Vec<TxEntry>,
    pub(crate) fail_reason: FailReason,
}

impl<'a> TransactionExecutionContext<'a> {
    pub fn new(balances: &'a mut [Balance]) -> Self {
        Self {
            balances,
            entries: Vec::new(),
            fail_reason: FailReason::NONE,
        }
    }

    #[inline]
    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.balances.get(account_id as usize).copied().unwrap_or(0)
    }

    #[inline]
    pub fn credit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }

        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_sub(amount as i64);
            self.entries.push(TxEntry {
                tx_id: 0,
                account_id,
                amount,
                kind: EntryKind::Credit,
                _pad: [0; 7],
            });
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    pub fn debit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }

        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_add(amount as i64);
            self.entries.push(TxEntry {
                tx_id: 0,
                account_id,
                amount,
                kind: EntryKind::Debit,
                _pad: [0; 7],
            });
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    pub fn fail(&mut self, reason: FailReason) {
        self.fail_reason = reason;
    }

    pub fn is_failed(&self) -> bool {
        self.fail_reason.is_failure()
    }

    pub fn entries(&self) -> &Vec<TxEntry> {
        &self.entries
    }

    pub fn fail_reason(&self) -> FailReason {
        self.fail_reason
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.fail_reason = FailReason::NONE;
    }

    pub(crate) fn verify(&mut self) -> FailReason {
        if self.fail_reason.is_failure() {
            return self.fail_reason;
        }

        let mut sum_credits: u128 = 0;
        let mut sum_debits: u128 = 0;

        for entry in &self.entries {
            match entry.kind {
                EntryKind::Credit => sum_credits += entry.amount as u128,
                EntryKind::Debit => sum_debits += entry.amount as u128,
            }
        }

        if sum_credits != sum_debits {
            self.fail_reason = FailReason::ZERO_SUM_VIOLATION;
        }

        self.fail_reason
    }

    pub(crate) fn commit(&mut self) {
        // Balances are already updated directly
    }

    pub(crate) fn rollback(&mut self) {
        // Revert entries into balances
        for entry in self.entries.iter().rev() {
            if let Some(balance) = self.balances.get_mut(entry.account_id as usize) {
                match entry.kind {
                    EntryKind::Credit => {
                        *balance = balance.saturating_add(entry.amount as i64);
                    }
                    EntryKind::Debit => {
                        *balance = balance.saturating_sub(entry.amount as i64);
                    }
                }
            }
        }
    }
}

pub struct Transaction {
    pub id: u64,
    pub operation: Operation,
}

impl Transaction {
    pub fn new(operation: Operation) -> Self {
        Self { id: 0, operation }
    }
}

#[derive(Default, Debug)]
pub enum TransactionStatus {
    #[default]
    Pending,
    Error(FailReason),
    Computed,   // By Transactor
    Committed,  // Written to WAL
    OnSnapshot, // Balances are reflected from the snapshot
}

impl TransactionStatus {
    pub fn is_committed(&self) -> bool {
        match self {
            Self::Pending => false,
            Self::Error(_) => false,
            Self::Computed => false,
            Self::Committed => true,
            Self::OnSnapshot => true,
        }
    }

    pub fn balance_ready(&self) -> bool {
        match self {
            Self::Pending => false,
            Self::Error(_) => false,
            Self::Computed => false,
            Self::Committed => false,
            Self::OnSnapshot => true,
        }
    }

    pub fn is_ok(&self) -> bool {
        !self.is_err()
    }

    pub fn is_err(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    pub fn error_reason(&self) -> FailReason {
        match self {
            Self::Error(reason) => *reason,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction::{Operation, Transaction, TransactionExecutionContext};

    #[test]
    fn it_works() {
        let tr1 = Transaction::new(Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 0,
        });

        let mut balances = vec![0; 100];
        let _ctx = TransactionExecutionContext::new(&mut balances);

        // process is now handled in Transactor
        assert_eq!(tr1.operation.user_ref(), 0);
    }
}
