use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Transactor<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, String>>,
    balances: HashMap<u64, BalanceData>,
    running: Arc<AtomicBool>,
}

pub struct TransactorRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    execution_context: LedgerTransactionExecutionContext<BalanceData>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, String>>,
    running: Arc<AtomicBool>,
}

impl<Data, BalanceData> Transactor<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
        outbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inbound,
            outbound,
            last_processed_transaction_id: Arc::new(Default::default()),
            rejected_transactions: Arc::new(Default::default()),
            balances: HashMap::new(),
            running,
        }
    }

    pub fn load_balances(&mut self, balances: Vec<(u64, BalanceData)>) {
        for (account_id, balance) in balances {
            self.balances.insert(account_id, balance);
        }
    }

    pub fn reprocess_transaction(&mut self, transaction: Transaction<Data, BalanceData>) {
        let mut ctx = LedgerTransactionExecutionContext {
            balances: std::mem::take(&mut self.balances),
        };
        let result = transaction.process(&mut ctx);
        if let Err(reason) = result {
            self.rejected_transactions
                .insert(transaction.id, reason.to_string());
        }
        self.last_processed_transaction_id
            .store(transaction.id, std::sync::atomic::Ordering::Relaxed);
        self.balances = ctx.balances;
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn rejected_count(&self) -> usize {
        self.rejected_transactions.len()
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<String> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| entry.value().clone())
    }

    pub fn start(&mut self) -> JoinHandle<()> {
        let mut runner = TransactorRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            execution_context: LedgerTransactionExecutionContext {
                balances: std::mem::take(&mut self.balances),
            },
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            rejected_transactions: self.rejected_transactions.clone(),
            running: self.running.clone(),
        };
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || runner.run())
            .unwrap()
    }
}

impl<Data, BalanceData> TransactorRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn run(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            if let Some(transaction) = self.inbound.pop() {
                let ctx = &mut self.execution_context;
                let result = transaction.process(ctx);

                if let Some(err) = result.err() {
                    self.rejected_transactions.insert(transaction.id, err);
                } else {
                    let mut transaction = transaction;
                    while let Err(returned_tx) = self.outbound.push(transaction) {
                        transaction = returned_tx;
                        if !self.running.load(Ordering::Relaxed) {
                            return;
                        }
                    }
                }
                self.last_processed_transaction_id
                    .store(transaction.id, Ordering::Relaxed);
            } else {
                std::thread::yield_now();
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
