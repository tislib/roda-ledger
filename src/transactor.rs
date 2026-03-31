use crate::balance::Balance;
use crate::entities::{EntryKind, FailReason, SYSTEM_ACCOUNT_ID, TxMetadata, WalEntry};
use crate::pipeline_mode::PipelineMode;
use crate::transaction::{
    CompositeOperationFlags, Operation, Step, Transaction, TransactionExecutionContext,
};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

pub struct Transactor {
    inbound: Arc<ArrayQueue<Transaction>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: Vec<Balance>,
    pending_entries: u8,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

pub struct TransactorRunner {
    inbound: Arc<ArrayQueue<Transaction>>,
    outbound: Arc<ArrayQueue<WalEntry>>,
    balances: Vec<Balance>,
    last_processed_transaction_id: Arc<AtomicU64>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
}

impl Transactor {
    pub fn new(
        inbound: Arc<ArrayQueue<Transaction>>,
        outbound: Arc<ArrayQueue<WalEntry>>,
        running: Arc<AtomicBool>,
        max_accounts: usize,
        pipeline_mode: PipelineMode,
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
            pipeline_mode,
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
            pipeline_mode: self.pipeline_mode,
        };
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || runner.run())
            .unwrap()
    }
}

impl TransactorRunner {
    pub fn run(&mut self) {
        let mut entries = Vec::with_capacity(16);
        let mut retry_count = 0;

        while self.running.load(Ordering::Relaxed) {
            if let Some(transaction) = self.inbound.pop() {
                retry_count = 0;
                let fail_reason;
                let entries_tmp;

                {
                    let mut ctx = TransactionExecutionContext::new(&mut self.balances);
                    ctx.entries = entries;
                    ctx.fail_reason = FailReason::NONE;

                    match &transaction.operation {
                        Operation::Deposit {
                            account, amount, ..
                        } => {
                            ctx.debit(*account, *amount);
                            ctx.credit(SYSTEM_ACCOUNT_ID, *amount);
                        }
                        Operation::Withdrawal {
                            account, amount, ..
                        } => {
                            if ctx.get_balance(*account) < *amount as i64 {
                                ctx.fail(FailReason::INSUFFICIENT_FUNDS);
                            } else {
                                ctx.credit(*account, *amount);
                                ctx.debit(SYSTEM_ACCOUNT_ID, *amount);
                            }
                        }
                        Operation::Transfer {
                            from, to, amount, ..
                        } => {
                            if from == to {
                                // no-op
                            } else if ctx.get_balance(*from) < *amount as i64 {
                                ctx.fail(FailReason::INSUFFICIENT_FUNDS);
                            } else {
                                ctx.credit(*from, *amount);
                                ctx.debit(*to, *amount);
                            }
                        }
                        Operation::Composite(op) => {
                            for step in &op.steps {
                                match step {
                                    Step::Credit { account_id, amount } => {
                                        ctx.credit(*account_id, *amount);
                                    }
                                    Step::Debit { account_id, amount } => {
                                        ctx.debit(*account_id, *amount);
                                    }
                                }
                            }

                            if op
                                .flags
                                .contains(CompositeOperationFlags::CHECK_NEGATIVE_BALANCE)
                            {
                                for entry in &ctx.entries {
                                    if ctx.get_balance(entry.account_id) < 0 {
                                        ctx.fail(FailReason::INSUFFICIENT_FUNDS);
                                        break;
                                    }
                                }
                            }
                        }
                        Operation::Named { .. } => {
                            panic!("Named operations not implemented yet");
                        }
                    }

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
                        user_ref: transaction.operation.user_ref(),
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
                        user_ref: transaction.operation.user_ref(),
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
                self.pipeline_mode.wait_strategy(retry_count);
                retry_count += 1;
            }
        }
    }
}
