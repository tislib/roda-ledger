use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::dedup::{DedupCache, DedupResult};
use crate::entities::{
    EntryKind, FailReason, SYSTEM_ACCOUNT_ID, TxEntry, TxLink, TxLinkKind, TxMetadata, WalEntry,
    WalEntryKind,
};
use crate::pipeline::TransactorContext;
use crate::transaction::{CompositeOperationFlags, Operation, Step, Transaction, TransactionInput};
use crossbeam_skiplist::SkipMap;
use std::collections::{BTreeMap, HashMap};
use std::hint::spin_loop;
use std::sync::Arc;
use std::thread::JoinHandle;

pub struct Transactor {
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: Vec<Balance>,
    dedup: DedupCache,
}

pub struct TransactorRunner {
    balances: Vec<Balance>,
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    transaction_buffer: Vec<Transaction>,
    expected_next_id: u64,
    pending: BTreeMap<u64, Transaction>,
    entries: Vec<WalEntry>,
    input_retry_count: u64,
    fail_reason: FailReason,
    position: usize,
    dedup: DedupCache,
}

impl Transactor {
    pub fn new(config: &LedgerConfig) -> Self {
        let mut accounts = Vec::with_capacity(config.max_accounts);
        accounts.resize(config.max_accounts, Balance::default());
        Self {
            rejected_transactions: Arc::new(Default::default()),
            balances: accounts,
            dedup: DedupCache::new(config.storage.transaction_count_per_segment),
        }
    }

    // --- Replay accessors (called from recover) ---
    pub(crate) fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                *slot = *balance;
            }
        }
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<FailReason> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| *entry.value())
    }

    pub fn get_rejected_count(&self) -> u64 {
        self.rejected_transactions.len() as u64
    }

    pub(crate) fn dedup_cache_mut(&mut self) -> &mut DedupCache {
        &mut self.dedup
    }

    pub fn start(&mut self, ctx: TransactorContext) -> std::io::Result<JoinHandle<()>> {
        let cap = ctx.input_capacity();
        let mut runner = TransactorRunner {
            balances: std::mem::take(&mut self.balances),
            rejected_transactions: self.rejected_transactions.clone(),
            transaction_buffer: Vec::with_capacity(cap),
            entries: Vec::with_capacity(cap),
            expected_next_id: ctx.get_processed_index() + 1,
            pending: BTreeMap::new(),
            input_retry_count: 0,
            fail_reason: FailReason::NONE,
            position: 0,
            dedup: std::mem::replace(&mut self.dedup, DedupCache::new(0)),
        };
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || runner.run(ctx))
    }
}

impl TransactorRunner {
    /// Standalone constructor used by benches (no pipeline / no queues).
    pub fn new(max_accounts: usize) -> Self {
        let mut balances = Vec::with_capacity(max_accounts);
        balances.resize(max_accounts, Balance::default());
        Self {
            balances,
            expected_next_id: 1,
            pending: BTreeMap::new(),
            rejected_transactions: Arc::new(Default::default()),
            transaction_buffer: Vec::with_capacity(1),
            entries: Vec::with_capacity(16),
            input_retry_count: 0,
            fail_reason: FailReason::NONE,
            position: 0,
            dedup: DedupCache::new(0),
        }
    }

    /// Process a batch of transactions directly, bypassing inbound/outbound queues.
    pub fn process_direct_batch(&mut self, txs: impl IntoIterator<Item = Transaction>) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        for tx in txs {
            self.transaction_buffer.push(tx);
        }
        self.process(timestamp);
        self.reset();
    }

    /// Process a single transaction directly, bypassing inbound/outbound queues.
    pub fn process_direct(&mut self, tx: Transaction) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.transaction_buffer.push(tx);
        self.process(timestamp);
        self.reset();
    }

    pub fn run(&mut self, ctx: TransactorContext) {
        while ctx.is_running() {
            self.run_step(&ctx);
        }
    }

    fn run_step(&mut self, ctx: &TransactorContext) {
        let inbound = ctx.input();
        // collect transactions to process
        while let Some(txi) = inbound.pop() {
            match txi {
                TransactionInput::Single(tx) => {
                    if tx.id == self.expected_next_id {
                        self.transaction_buffer.push(tx);
                        self.expected_next_id += 1;
                        // drain any buffered transactions that are now in order
                        while let Some(buffered) = self.pending.remove(&self.expected_next_id) {
                            self.transaction_buffer.push(buffered);
                            self.expected_next_id += 1;
                        }
                    } else if tx.id > self.expected_next_id {
                        self.pending.insert(tx.id, tx);
                    }

                    // Limit batch size to avoid overly long processing steps
                    if self.transaction_buffer.len() >= self.transaction_buffer.capacity() {
                        break;
                    }
                }
                TransactionInput::Batch(tbx) => {
                    if tbx.start_tx_id == self.expected_next_id {
                        for (i, op) in tbx.operations.into_iter().enumerate() {
                            let mut tx = Transaction::new(op);
                            tx.id = tbx.start_tx_id + i as u64;
                            self.transaction_buffer.push(tx);
                            self.expected_next_id += 1;
                            // drain any buffered transactions that are now in order
                            while let Some(buffered) = self.pending.remove(&self.expected_next_id) {
                                self.transaction_buffer.push(buffered);
                                self.expected_next_id += 1;
                            }
                        }
                    } else if tbx.start_tx_id > self.expected_next_id {
                        for (i, op) in tbx.operations.into_iter().enumerate() {
                            let mut tx = Transaction::new(op);
                            tx.id = tbx.start_tx_id + i as u64;
                            self.pending.insert(tx.id, tx);
                        }
                    }

                    // Limit batch size to avoid overly long processing steps
                    if self.transaction_buffer.len() >= self.transaction_buffer.capacity() {
                        break;
                    }
                }
            }
        }

        if self.transaction_buffer.is_empty() {
            ctx.wait_strategy().retry(self.input_retry_count);
            self.input_retry_count += 1;
            return;
        }
        self.input_retry_count = 0;

        // single syscall for timestamp for this entire step
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let max_tx_id = self.process(timestamp);

        // push all accumulated entries to outbound at the end of the step
        let output = ctx.output();
        let mut i = 0;
        while i < self.entries.len() {
            let entry = self.entries[i];
            let mut retry_count = 0;
            loop {
                retry_count += 1;
                if output.push(entry).is_ok() {
                    break;
                }
                if retry_count % 10000 == 0 && !ctx.is_running() {
                    return;
                }
                spin_loop();
            }
            i += 1;
        }

        if max_tx_id > 0 {
            ctx.set_processed_index(max_tx_id);
        }

        self.reset();
    }

    /// Process the buffered transactions, producing wal entries in `self.entries`.
    /// Returns the maximum transaction id observed in this batch (0 if none).
    fn process(&mut self, timestamp: u64) -> u64 {
        let mut max_tx_id = 0;
        for idx in 0..self.transaction_buffer.len() {
            self.fail_reason = FailReason::NONE;
            let operation = &self.transaction_buffer[idx].operation;

            let tx_id = self.transaction_buffer[idx].id;
            max_tx_id = max_tx_id.max(tx_id);
            let user_ref = operation.user_ref();

            // --- Deduplication check ---
            match self.dedup.check(user_ref, tx_id) {
                DedupResult::Duplicate(original_tx_id) => {
                    self.emit_duplicate(tx_id, user_ref, timestamp, original_tx_id);
                    self.rejected_transactions
                        .insert(tx_id, FailReason::DUPLICATE);
                    // position already advanced by emit_duplicate
                    continue;
                }
                DedupResult::Proceed => {}
            }

            // --- Normal operation processing ---
            match operation.clone() {
                Operation::Deposit {
                    user_ref,
                    account,
                    amount,
                    ..
                } => {
                    self.meta(tx_id, *b"DEPOSIT\0", user_ref, timestamp);
                    self.debit(tx_id, account, amount);
                    self.credit(tx_id, SYSTEM_ACCOUNT_ID, amount);
                }
                Operation::Withdrawal {
                    user_ref,
                    account,
                    amount,
                    ..
                } => {
                    self.meta(tx_id, *b"WITHDRAW", user_ref, timestamp);
                    if self.get_balance(account) < amount as i64 {
                        self.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        self.credit(tx_id, account, amount);
                        self.debit(tx_id, SYSTEM_ACCOUNT_ID, amount);
                    }
                }
                Operation::Transfer {
                    user_ref,
                    from,
                    to,
                    amount,
                    ..
                } => {
                    self.meta(tx_id, *b"TRANSFER", user_ref, timestamp);
                    if from == to {
                        // no-op
                    } else if self.get_balance(from) < amount as i64 {
                        self.fail(FailReason::INSUFFICIENT_FUNDS);
                    } else {
                        self.credit(tx_id, from, amount);
                        self.debit(tx_id, to, amount);
                    }
                }
                Operation::Composite(op) => {
                    self.meta(tx_id, *b"COMPOSIT", op.user_ref, timestamp);
                    for step in &op.steps {
                        match step {
                            Step::Credit { account_id, amount } => {
                                self.credit(tx_id, *account_id, *amount);
                            }
                            Step::Debit { account_id, amount } => {
                                self.debit(tx_id, *account_id, *amount);
                            }
                        }
                    }

                    if op
                        .flags
                        .contains(CompositeOperationFlags::CHECK_NEGATIVE_BALANCE)
                    {
                        for i in (self.position + 1)..self.entries.len() {
                            if let WalEntry::Entry(e) = self.entries[i]
                                && self.get_balance(e.account_id) < 0
                            {
                                self.fail(FailReason::INSUFFICIENT_FUNDS);
                                break;
                            }
                        }
                    }
                }
                Operation::Named { .. } => {
                    panic!("Named operations not implemented yet");
                }
            }

            let fail_reason = self.verify();
            let meta_idx = self.position;

            if fail_reason.is_failure() {
                // rollback balance changes for entries after meta, truncate to meta+1
                self.rollback();

                // go back and update meta at position to mark as failed
                if let Some(WalEntry::Metadata(m)) = self.entries.get_mut(meta_idx) {
                    m.fail_reason = fail_reason;
                    m.entry_count = 0;
                    m.link_count = 0;
                    m.crc32c = 0;
                    let digest = crc32c::crc32c(bytemuck::bytes_of(m));
                    m.crc32c = digest;
                }

                self.rejected_transactions.insert(tx_id, fail_reason);

                // Record in dedup (even failed txs get deduped)
                self.dedup.insert(user_ref, tx_id);

                // advance: meta only
                self.position += 1;
            } else {
                let entry_count = self.entries.len() - meta_idx - 1;

                // update meta with entry_count, then calculate crc over meta + entries
                if let Some(WalEntry::Metadata(m)) = self.entries.get_mut(meta_idx) {
                    m.entry_count = entry_count as u8;
                    m.crc32c = 0;
                }
                let mut digest = if let Some(WalEntry::Metadata(m)) = self.entries.get(meta_idx) {
                    crc32c::crc32c(bytemuck::bytes_of(m))
                } else {
                    0
                };
                for i in (meta_idx + 1)..self.entries.len() {
                    if let WalEntry::Entry(e) = &self.entries[i] {
                        digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(e));
                    }
                }
                if let Some(WalEntry::Metadata(m)) = self.entries.get_mut(meta_idx) {
                    m.crc32c = digest;
                }

                // Record in dedup
                self.dedup.insert(user_ref, tx_id);

                // advance: meta + entries
                self.position += 1 + entry_count;
            }
        }

        if max_tx_id > 0 {
            self.expected_next_id = self.expected_next_id.max(max_tx_id + 1);
        }

        max_tx_id
    }

    /// Emit a duplicate transaction: TxMetadata (entry_count=0, link_count=1, fail_reason=DUPLICATE)
    /// followed by a TxLink { kind: Duplicate, to_tx_id: original }.
    fn emit_duplicate(&mut self, tx_id: u64, user_ref: u64, timestamp: u64, original_tx_id: u64) {
        let link = TxLink {
            entry_type: WalEntryKind::Link as u8,
            link_kind: TxLinkKind::Duplicate as u8,
            _pad: [0; 6],
            tx_id,
            to_tx_id: original_tx_id,
            _pad2: [0; 16],
        };

        let mut meta = TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 0,
            link_count: 1,
            fail_reason: FailReason::DUPLICATE,
            crc32c: 0,
            tx_id,
            timestamp,
            user_ref,
            tag: *b"DUPLICAT",
        };

        // CRC covers meta + link
        meta.crc32c = 0;
        let digest = crc32c::crc32c(bytemuck::bytes_of(&meta));
        let digest = crc32c::crc32c_append(digest, bytemuck::bytes_of(&link));
        meta.crc32c = digest;

        self.entries.push(WalEntry::Metadata(meta));
        self.entries.push(WalEntry::Link(link));
        self.position += 2; // meta + link
    }

    #[inline]
    fn meta(&mut self, tx_id: u64, tag: [u8; 8], user_ref: u64, timestamp: u64) {
        self.entries.push(WalEntry::Metadata(TxMetadata {
            entry_type: WalEntryKind::TxMetadata as u8,
            entry_count: 0,
            link_count: 0,
            fail_reason: FailReason::NONE,
            crc32c: 0,
            tx_id,
            timestamp,
            user_ref,
            tag,
        }));
    }

    #[inline]
    fn credit(&mut self, tx_id: u64, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }

        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_sub(amount as i64);
            self.entries.push(WalEntry::Entry(TxEntry {
                entry_type: WalEntryKind::TxEntry as u8,
                tx_id,
                account_id,
                amount,
                kind: EntryKind::Credit,
                _pad0: [0; 6],
                computed_balance: *balance,
            }));
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    fn debit(&mut self, tx_id: u64, account_id: u64, amount: u64) {
        if self.fail_reason.is_failure() {
            return;
        }

        if let Some(balance) = self.balances.get_mut(account_id as usize) {
            *balance = balance.saturating_add(amount as i64);
            self.entries.push(WalEntry::Entry(TxEntry {
                entry_type: WalEntryKind::TxEntry as u8,
                tx_id,
                account_id,
                amount,
                kind: EntryKind::Debit,
                _pad0: [0; 6],
                computed_balance: *balance,
            }));
        } else {
            self.fail_reason = FailReason::ACCOUNT_LIMIT_EXCEEDED;
        }
    }

    #[inline]
    fn get_balance(&self, account_id: u64) -> Balance {
        self.balances.get(account_id as usize).copied().unwrap_or(0)
    }

    #[inline]
    fn fail(&mut self, reason: FailReason) {
        self.fail_reason = reason;
    }

    fn verify(&mut self) -> FailReason {
        if self.fail_reason.is_failure() {
            return self.fail_reason;
        }

        let mut sum_credits: u128 = 0;
        let mut sum_debits: u128 = 0;

        for entry in self.entries.iter().skip(self.position + 1) {
            if let WalEntry::Entry(e) = entry {
                match e.kind {
                    EntryKind::Credit => sum_credits += e.amount as u128,
                    EntryKind::Debit => sum_debits += e.amount as u128,
                }
            }
        }

        if sum_credits != sum_debits {
            self.fail_reason = FailReason::ZERO_SUM_VIOLATION;
        }

        self.fail_reason
    }

    fn rollback(&mut self) {
        // revert balance changes for entries after the meta at self.position
        for entry in self.entries.iter().skip(self.position + 1) {
            if let WalEntry::Entry(e) = entry
                && let Some(balance) = self.balances.get_mut(e.account_id as usize)
            {
                match e.kind {
                    EntryKind::Credit => {
                        *balance = balance.saturating_add(e.amount as i64);
                    }
                    EntryKind::Debit => {
                        *balance = balance.saturating_sub(e.amount as i64);
                    }
                }
            }
        }

        // keep meta at self.position, remove entries after it
        self.entries.truncate(self.position + 1);
    }

    fn reset(&mut self) {
        self.transaction_buffer.clear();
        self.entries.clear();
        self.input_retry_count = 0;
        self.fail_reason = FailReason::NONE;
        self.position = 0;
    }
}
