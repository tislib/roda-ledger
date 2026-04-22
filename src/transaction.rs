use crate::entities::FailReason;
use serde::{Deserialize, Serialize};

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
    Function {
        name: String,
        /// Fixed arity: exactly 8 `i64` positional parameters. Unused
        /// slots are conventionally passed as `0`.
        params: [i64; 8],
        user_ref: u64,
    },
}

impl Operation {
    pub fn user_ref(&self) -> u64 {
        match self {
            Operation::Transfer { user_ref, .. } => *user_ref,
            Operation::Deposit { user_ref, .. } => *user_ref,
            Operation::Withdrawal { user_ref, .. } => *user_ref,
            Operation::Function { user_ref, .. } => *user_ref,
        }
    }
}

pub struct Transaction {
    pub id: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitLevel {
    Computed,
    Committed,
    OnSnapshot,
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
    NotFound, // Transaction isn't found in the pipeline
    Pending,
    Error(FailReason),
    Computed,   // By Transactor
    Committed,  // Written to WAL
    OnSnapshot, // Balances are reflected from the snapshot
}

impl TransactionStatus {
    pub fn is_committed(&self) -> bool {
        match self {
            Self::NotFound => false,
            Self::Pending => false,
            Self::Error(_) => false,
            Self::Computed => false,
            Self::Committed => true,
            Self::OnSnapshot => true,
        }
    }

    pub fn balance_ready(&self) -> bool {
        match self {
            Self::NotFound => false,
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

pub struct TransactionBatch {
    pub start_tx_id: u64,
    pub operations: Vec<Operation>, // each operation will become a separate transaction
}

pub enum TransactionInput {
    Single(Transaction),
    Batch(TransactionBatch),
}

impl TransactionInput {
    pub fn single(self) -> Transaction {
        match self {
            TransactionInput::Single(tx) => tx,
            TransactionInput::Batch(_) => {
                unreachable!("Batch transactions cannot be converted to single")
            }
        }
    }

    pub fn batch(self) -> TransactionBatch {
        match self {
            TransactionInput::Batch(batch) => batch,
            TransactionInput::Single(_) => {
                unreachable!("Single transactions cannot be converted to batch")
            }
        }
    }
}
