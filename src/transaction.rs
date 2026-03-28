use crate::balance::Balance;
use crate::entities::{EntryKind, FailReason, TxEntry};
use bytemuck::{Pod, Zeroable};

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

pub trait TransactionDataType: Pod + Zeroable + Copy + Send + Sync {
    fn process(&self, ctx: &mut TransactionExecutionContext<'_>);
    fn user_ref(&self) -> u64 {
        0
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct Transaction<Data: TransactionDataType> {
    pub id: u64,
    pub data: Data,
}

unsafe impl<Data: TransactionDataType> Pod for Transaction<Data> {}
unsafe impl<Data: TransactionDataType> Zeroable for Transaction<Data> {}

impl<Data: TransactionDataType> Transaction<Data> {
    pub fn new(data: Data) -> Self {
        Self { id: 0, data }
    }

    pub fn process(&self, ctx: &mut TransactionExecutionContext<'_>) {
        self.data.process(ctx);
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
    use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
    use bytemuck::{Pod, Zeroable};

    #[test]
    fn it_works() {
        #[repr(transparent)]
        #[derive(Copy, Clone, Debug, Default, Pod, Zeroable, PartialEq)]
        struct SimpleTransactionDataType(u64);

        impl TransactionDataType for SimpleTransactionDataType {
            fn process(&self, ctx: &mut TransactionExecutionContext<'_>) {
                ctx.credit(0, 100);
                ctx.debit(1, 100);
            }
        }

        let tr1 = Transaction::new(SimpleTransactionDataType(0));

        let mut balances = vec![0; 100];
        let mut ctx = TransactionExecutionContext::new(&mut balances);

        tr1.process(&mut ctx);
    }
}
