use std::collections::HashMap;
use crate::balance::Balance;
use bytemuck::{Pod, Zeroable};
use crate::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry};

pub struct TransactionExecutionContext<'a> {
    pub(crate) balances: &'a HashMap<u64, Balance>,
    pub(crate) local_balances: HashMap<u64, Balance>,
    pub(crate) entries: Vec<TxEntry>,
    pub(crate) fail_reason: FailReason,
}

impl<'a> TransactionExecutionContext<'a> {
    pub fn new(balances: &'a HashMap<u64, Balance>) -> Self {
        Self {
            balances,
            local_balances: HashMap::new(),
            entries: Vec::new(),
            fail_reason: FailReason::NONE,
        }
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.local_balances
            .get(&account_id)
            .cloned()
            .or_else(|| self.balances.get(&account_id).cloned())
            .unwrap_or_default()
    }

    pub fn credit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        let balance = self.get_balance(account_id);
        let new_balance = balance.saturating_sub(amount as i64);
        self.local_balances.insert(account_id, new_balance);
        self.entries.push(TxEntry {
            tx_id: 0,
            account_id,
            amount,
            kind: EntryKind::Credit,
            _pad: [0; 7],
        });
    }

    pub fn debit(&mut self, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }
        let balance = self.get_balance(account_id);
        let new_balance = balance.saturating_add(amount as i64);
        self.local_balances.insert(account_id, new_balance);
        self.entries.push(TxEntry {
            tx_id: 0,
            account_id,
            amount,
            kind: EntryKind::Debit,
            _pad: [0; 7],
        });
    }

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
    use crate::entities::{FailReason, TxEntry};
    use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
    use bytemuck::{Pod, Zeroable};
    use std::collections::HashMap;

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

        let balances = HashMap::new();
        let mut ctx = TransactionExecutionContext::new(&balances);

        tr1.process(&mut ctx);
    }
}
