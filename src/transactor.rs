use crate::balance::Balance;
use crate::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry};
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Transactor<Data: TransactionDataType> {
    inbound: Arc<ArrayQueue<Transaction<Data>>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: HashMap<u64, Balance>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
}

pub struct TransactorRunner<Data: TransactionDataType> {
    inbound: Arc<ArrayQueue<Transaction<Data>>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    balances: HashMap<u64, Balance>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    running: Arc<AtomicBool>,
}

impl<Data: TransactionDataType> Transactor<Data> {
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction<Data>>>,
        outbound: Arc<ArrayQueue<WalEntry>>,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inbound,
            outbound,
            last_processed_transaction_id: Arc::new(Default::default()),
            rejected_transactions: Arc::new(Default::default()),
            balances: HashMap::new(),
            pending_entries: 0,
            running,
        }
    }

    pub fn load_balances(&mut self, balances: Vec<(u64, Balance)>) {
        for (account_id, balance) in balances {
            self.balances.insert(account_id, balance);
        }
    }

    pub fn apply_wal_entry(&mut self, wal_entry: WalEntry) {
        let tx_id = wal_entry.tx_id();
        match wal_entry {
            WalEntry::Metadata(m) => {
                self.pending_entries = m.entry_count;
            }
            WalEntry::Entry(e) => {
                let mut balance = self.balances.get(&e.account_id).cloned().unwrap_or_default();
                match e.kind {
                    EntryKind::Credit => {
                        balance = balance.saturating_sub(e.amount as i64);
                    }
                    EntryKind::Debit => {
                        balance = balance.saturating_add(e.amount as i64);
                    }
                }
                self.balances.insert(e.account_id, balance);
                if self.pending_entries > 0 {
                    self.pending_entries -= 1;
                }
            }
        }
        if self.pending_entries == 0 {
            self.last_processed_transaction_id.store(tx_id, Ordering::Relaxed);
        }
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<FailReason> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| *entry.value())
    }

    pub fn start(&mut self) -> JoinHandle<()> {
        let mut runner = TransactorRunner {
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            balances: std::mem::take(&mut self.balances),
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

impl<Data: TransactionDataType> TransactorRunner<Data> {
    pub fn run(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            if let Some(transaction) = self.inbound.pop() {
                let mut ctx = TransactionExecutionContext::new(&self.balances);
                transaction.process(&mut ctx);

                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;

                let mut fail_reason = ctx.fail_reason();
                let mut entries = ctx.entries;

                if fail_reason.is_success() {
                    // Check zero-sum invariant
                    let mut sum_credits: u128 = 0;
                    let mut sum_debits: u128 = 0;

                    for entry in &mut entries {
                        entry.tx_id = transaction.id;
                        match entry.kind {
                            EntryKind::Credit => sum_credits += entry.amount as u128,
                            EntryKind::Debit => sum_debits += entry.amount as u128,
                        }
                    }

                    if sum_credits != sum_debits {
                        fail_reason = FailReason::ZERO_SUM_VIOLATION;
                    }
                }

                if fail_reason.is_failure() {
                    self.rejected_transactions.insert(transaction.id, fail_reason);

                    // Send only metadata for failed transactions
                    let metadata = TxMetadata {
                        tx_id: transaction.id,
                        timestamp,
                        user_ref: transaction.data.user_ref(),
                        entry_count: 0,
                        fail_reason,
                        _pad: [0; 6],
                    };

                    while let Err(_) = self.outbound.push(WalEntry::Metadata(metadata)) {
                        if !self.running.load(Ordering::Relaxed) {
                            return;
                        }
                    }
                } else {
                    // Transaction is successful
                    // Apply local balance changes to main balances
                    for (account_id, balance) in ctx.local_balances {
                        self.balances.insert(account_id, balance);
                    }

                    // Push metadata FIRST so consumers know the entry_count
                    let metadata = TxMetadata {
                        tx_id: transaction.id,
                        timestamp,
                        user_ref: transaction.data.user_ref(),
                        entry_count: entries.len() as u8,
                        fail_reason: FailReason::NONE,
                        _pad: [0; 6],
                    };

                    while let Err(_) = self.outbound.push(WalEntry::Metadata(metadata)) {
                        if !self.running.load(Ordering::Relaxed) {
                            return;
                        }
                    }

                    // Then push entries
                    for entry in entries.iter() {
                        while let Err(_) = self.outbound.push(WalEntry::Entry(*entry)) {
                            if !self.running.load(Ordering::Relaxed) {
                                return;
                            }
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

