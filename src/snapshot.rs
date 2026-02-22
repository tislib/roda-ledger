use crate::balance::BalanceDataType;
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

pub struct Snapshot<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    // Store as (account_id, transaction_id) -> BalanceData
    balances: Arc<SkipMap<(u64, u64), BalanceData>>,
    step: Arc<AtomicU64>,
}

pub struct SnapshotRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>,
    balances: Arc<SkipMap<(u64, u64), BalanceData>>,
    pub step: Arc<AtomicU64>,
}

impl<Data, BalanceData> Snapshot<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn new(inbound: Arc<ArrayQueue<Transaction<Data, BalanceData>>>) -> Self {
        Self {
            inbound,
            balances: Arc::new(SkipMap::new()),
            step: Arc::new(Default::default()),
        }
    }

    pub fn tick(&self) {
        let current_step = self.step.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            spin_loop();
            let next_step = self.step.load(std::sync::atomic::Ordering::Relaxed);
            if next_step > current_step {
                break;
            }
        }
    }

    pub fn start(&self) {
        let runner = SnapshotRunner {
            inbound: self.inbound.clone(),
            balances: self.balances.clone(),
            step: self.step.clone(),
        };
        std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut runner = runner;
                runner.run()
            })
            .unwrap();
    }

    pub fn get_balance(&self, account_id: u64) -> BalanceData {
        // With flipped tid, the first entry for account_id is the latest one
        if let Some(entry) = self.balances.range((account_id, 0)..).next() {
            let (acc_id, _tid) = *entry.key();
            if acc_id == account_id {
                return entry.value().clone();
            }
        }
        BalanceData::default()
    }

    pub fn get_balance_at(&self, account_id: u64, transaction_id: u64) -> Result<BalanceData, String> {
        // actual_tid <= transaction_id  =>  !actual_tid >= !transaction_id
        if let Some(entry) = self.balances.range((account_id, !transaction_id)..).next() {
            let (acc_id, _tid) = *entry.key();
            if acc_id == account_id {
                return Ok(entry.value().clone());
            }
        }
        Err("No history for this account before this transaction ID".to_string())
    }
}

impl<Data, BalanceData> SnapshotRunner<Data, BalanceData>
where
    BalanceData: BalanceDataType,
    Data: TransactionDataType<BalanceData = BalanceData>,
{
    pub fn run(&mut self) {
        loop {
            self.process_single();
        }
    }

    fn process_single(&mut self) {
        if let Some(transaction) = self.inbound.pop() {
            let mut ctx = SnapshotExecutionContext {
                transaction_id: transaction.id,
                balances: self.balances.clone(),
            };
            let result = transaction.process(&mut ctx);
            if let Err(e) = result {
                panic!("Transaction process failed in snapshot: {}", e);
            }
        }
        self.step.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

struct SnapshotExecutionContext<BalanceData: BalanceDataType> {
    transaction_id: u64,
    balances: Arc<SkipMap<(u64, u64), BalanceData>>,
}

impl<BalanceData: BalanceDataType> TransactionExecutionContext<BalanceData>
    for SnapshotExecutionContext<BalanceData>
{
    fn get_balance(&self, account_id: u64) -> BalanceData {
        if let Some(entry) = self.balances.range((account_id, 0)..).next() {
            let (acc_id, _tid) = *entry.key();
            if acc_id == account_id {
                return entry.value().clone();
            }
        }
        BalanceData::default()
    }

    fn update_balance(&mut self, account_id: u64, balance: BalanceData) {
        self.balances.insert((account_id, !self.transaction_id), balance);
    }
}
