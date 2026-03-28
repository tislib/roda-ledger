use crate::balance::Balance;
use crate::entities::{EntryKind, FailReason, TxMetadata, WalEntry};
use crate::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Transactor<Data: TransactionDataType> {
    inbound: Arc<ArrayQueue<Transaction<Data>>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: Vec<Balance>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
}

pub struct TransactorRunner<Data: TransactionDataType> {
    inbound: Arc<ArrayQueue<Transaction<Data>>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    balances: Vec<Balance>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    running: Arc<AtomicBool>,
}

impl<Data: TransactionDataType> Transactor<Data> {
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction<Data>>>,
        outbound: Arc<ArrayQueue<WalEntry>>,
        running: Arc<AtomicBool>,
        max_accounts: usize,
    ) -> Self {
        let mut accounts = Vec::with_capacity(max_accounts);
        accounts.resize(max_accounts, Balance::default());
        Self {
            inbound,
            outbound,
            last_processed_transaction_id: Arc::new(Default::default()),
            rejected_transactions: Arc::new(Default::default()),
            balances: accounts,
            pending_entries: 0,
            running,
        }
    }

    pub fn load_balances(&mut self, balances: Vec<(u64, Balance)>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(account_id as usize) {
                *slot = balance;
            }
        }
    }

    pub fn apply_wal_entry(&mut self, wal_entry: WalEntry) {
        let tx_id = wal_entry.tx_id();
        match wal_entry {
            WalEntry::Metadata(m) => {
                self.pending_entries = m.entry_count;
            }
            WalEntry::Entry(e) => {
                if let Some(balance) = self.balances.get_mut(e.account_id as usize) {
                    match e.kind {
                        EntryKind::Credit => {
                            *balance = balance.saturating_sub(e.amount as i64);
                        }
                        EntryKind::Debit => {
                            *balance = balance.saturating_add(e.amount as i64);
                        }
                    }
                }
                if self.pending_entries > 0 {
                    self.pending_entries -= 1;
                }
            }
        }
        if self.pending_entries == 0 {
            self.last_processed_transaction_id
                .store(tx_id, Ordering::Relaxed);
        }
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id.load(Ordering::Relaxed)
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<FailReason> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| *entry.value())
    }

    pub fn get_rejected_count(&self) -> u64 {
        self.rejected_transactions.len() as u64
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
        let mut entries = Vec::with_capacity(16);

        while self.running.load(Ordering::Relaxed) {
            if let Some(transaction) = self.inbound.pop() {
                let fail_reason;
                let entries_tmp;

                {
                    let mut ctx = TransactionExecutionContext::new(&mut self.balances);
                    ctx.entries = entries;
                    ctx.fail_reason = FailReason::NONE;

                    transaction.process(&mut ctx);

                    fail_reason = ctx.verify();

                    if fail_reason.is_success() {
                        ctx.commit();
                        // Update entries with tx_id
                        for entry in &mut ctx.entries {
                            entry.tx_id = transaction.id;
                        }
                    } else {
                        ctx.rollback();
                    }

                    entries_tmp = ctx.entries;
                }

                if fail_reason.is_failure() {
                    self.rejected_transactions
                        .insert(transaction.id, fail_reason);

                    // Send only metadata for failed transactions
                    let metadata = TxMetadata {
                        tx_id: transaction.id,
                        timestamp: 0, // will be written by the WAL
                        user_ref: transaction.data.user_ref(),
                        entry_count: 0,
                        fail_reason,
                        _pad: [0; 6],
                    };

                    while self.outbound.push(WalEntry::Metadata(metadata)).is_err() {
                        if !self.running.load(Ordering::Relaxed) {
                            return;
                        }
                    }
                } else {
                    // Push metadata FIRST so consumers know the entry_count
                    let metadata = TxMetadata {
                        tx_id: transaction.id,
                        timestamp: 0, // will be written by the WAL
                        user_ref: transaction.data.user_ref(),
                        entry_count: entries_tmp.len() as u8,
                        fail_reason: FailReason::NONE,
                        _pad: [0; 6],
                    };

                    while self.outbound.push(WalEntry::Metadata(metadata)).is_err() {
                        if !self.running.load(Ordering::Relaxed) {
                            return;
                        }
                    }

                    // Then push entries
                    for entry in entries_tmp.iter() {
                        while self.outbound.push(WalEntry::Entry(*entry)).is_err() {
                            if !self.running.load(Ordering::Relaxed) {
                                return;
                            }
                        }
                    }
                }
                self.last_processed_transaction_id
                    .store(transaction.id, Ordering::Relaxed);

                // Recover buffers for next transaction
                entries = entries_tmp;
                entries.clear();
            } else {
                std::thread::yield_now();
            }
        }
    }
}
