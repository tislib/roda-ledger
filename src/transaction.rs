use crate::entities::FailReason;
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

pub struct Transaction {
    pub id: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitLevel {
    Processed,
    Committed,
    Snapshotted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitResult {
    pub tx_id: u64,
    pub fail_reason: FailReason,
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
