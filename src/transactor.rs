use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::thread::yield_now;

pub struct Transactor<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    step: Arc<AtomicU64>,
}

pub struct TransactorRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    execution_context: LedgerTransactionExecutionContext<BalanceData>,
    pub step: Arc<AtomicU64>,
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
        Self {
            inbound,
            outbound,
            step: Arc::new(Default::default()),
        }
    }

    // wait for at least one step to complete before returning
    pub fn tick(&mut self) {
        let current_step = self.step.load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..1_000 {
            let next_step = self.step.load(std::sync::atomic::Ordering::Relaxed);
            if next_step > current_step {
                return;
            }
            yield_now();
        }
    }

    pub fn start(&self) {
        let mut runner = TransactorRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            execution_context: LedgerTransactionExecutionContext {
                balances: Default::default(),
            },
            step: self.step.clone(),
        };
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || runner.run())
            .unwrap();
    }
}

impl<Data, BalanceData> TransactorRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn run(&mut self) {
        loop {
            if let Some(transaction) = self.inbound.pop() {
                let ctx = &mut self.execution_context;
                let result = transaction.process(ctx);

                if result.is_ok() {
                    let mut transaction = transaction;
                    while let Err(returned_tx) = self.outbound.push(transaction) {
                        transaction = returned_tx;
                    }
                }
                self.step.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

pub struct LedgerTransactionExecutionContext<BalanceData: BalanceDataType> {
    balances: HashMap<u64, BalanceData>,
}

impl<BalanceData: BalanceDataType> TransactionExecutionContext<BalanceData>
    for LedgerTransactionExecutionContext<BalanceData>
{
    fn get_balance(&self, account_id: u64) -> BalanceData {
        self.balances.get(&account_id).cloned().unwrap_or_default()
    }

    fn update_balance(&mut self, account_id: u64, balance: BalanceData) {
        self.balances.insert(account_id, balance);
    }
}
