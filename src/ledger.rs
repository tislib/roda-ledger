use std::marker::PhantomData;
use crate::balance::BalanceDataType;
use crate::sequencer::Sequencer;
use crate::transaction::TransactionDataType;
use crate::transactor::Transactor;

pub struct Ledger<B, T>
where
    B: BalanceDataType,
    T: TransactionDataType
{
    // PhantomData needs the concrete generic parameter, not the Trait name
    _balance: PhantomData<B>,
    _transaction: PhantomData<T>,

    // Stages:
    sequencer: Sequencer,
    transactor: Transactor,
}

impl<B, T> Ledger<B, T>
where
    B: BalanceDataType,
    T: TransactionDataType
{
    pub fn new() -> Self {
        Self {
            _balance: PhantomData,
            _transaction: PhantomData,
            sequencer: Sequencer {},
            transactor: Transactor {},
        }
    }
}