use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub struct Transactor<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
}

impl<Data, BalanceData> Transactor<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
        outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    ) -> Self {
        Self { inbound, outbound }
    }

    pub fn run(&self) {
        let execution_context = LedgerTransactionExecutionContext {
            _phantom: Default::default(),
        };
        loop {
            if let Some(transaction) = self.inbound.pop() {
                let result = transaction.process(&execution_context);

                if result.is_ok() {
                    let mut transaction = transaction;
                    loop {
                        let is_pushed = self.outbound.push(transaction);
                        if is_pushed.is_ok() {
                            break;
                        } else {
                            transaction = is_pushed.unwrap_err();
                        }
                    }
                }
            }
        }
    }
}

pub struct LedgerTransactionExecutionContext<BalanceData: BalanceDataType> {
    _phantom: std::marker::PhantomData<BalanceData>,
}

impl<BalanceData: BalanceDataType> TransactionExecutionContext<BalanceData>
    for LedgerTransactionExecutionContext<BalanceData>
{
    fn get_balance(&self, _account_id: u64) -> Result<BalanceData, String> {
        // Basic minimalist implementation
        Err("Not implemented".to_string())
    }

    fn update_balance(&self, _account_id: u64, _balance: BalanceData) -> Result<(), String> {
        // Basic minimalist implementation
        Ok(())
    }
}
