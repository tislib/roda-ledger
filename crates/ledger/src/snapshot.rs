use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::index::{IndexedTxEntry, TransactionIndexer};
use crate::pipeline::SnapshotContext;
use crate::recover::ActiveSnapshot;
use crate::transactor::transaction::CommittedTransaction;
use crate::transactor::{STATUS_OPEN, STATUS_SYSTEM, grow_capacity, set_flag};
use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::thread::JoinHandle;
use storage::Storage;
use storage::entities::{SYSTEM_ACCOUNT_ID, TxMetadata, WalEntry};

/// A query to be executed by the Snapshot stage against the `TransactionIndexer`.
///
/// The caller creates a per-request `mpsc::sync_channel(1)`, wraps the sender
/// in `respond`, and blocks on `rx.recv()` for the result.
pub struct QueryRequest {
    pub kind: QueryKind,
    pub respond: Box<dyn FnOnce(QueryResponse) + Send>,
}

/// Discriminant for the query type.
pub enum QueryKind {
    GetTransaction { tx_id: u64 },
    GetTransactionBatch { tx_ids: Vec<u64> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResponse {
    Transaction(Option<CommittedTransaction>),
    TransactionBatch(Vec<CommittedTransaction>),
    AccountHistory(Vec<IndexedTxEntry>),
}

/// Snapshot read-side account cell (ADR-022), mirroring `TransactorAccount`:
/// a signed balance plus an 8-lane `flags` word (lane 0 = status). Both are
/// atomic so the single-writer Snapshot stage publishes lock-free to the
/// many concurrent query readers.
pub struct SnapshotAccount {
    pub balance: AtomicI64,
    pub flags: AtomicU64,
}

impl SnapshotAccount {
    fn new() -> Self {
        Self {
            balance: AtomicI64::new(0),
            flags: AtomicU64::new(0),
        }
    }
}

/// Lock-free, growable account vector for the read side (ADR-022). Reads
/// `load()` the current generation; growth (rare, driven by `OpenAccount`)
/// builds a larger copy and `store()`s it. The Snapshot stage (and recovery,
/// pre-start) is the sole writer, so copy-then-swap loses no updates; readers
/// are eventually consistent (an in-flight reader may briefly hold the older,
/// shorter generation).
type Accounts = Arc<ArcSwap<Vec<SnapshotAccount>>>;

/// Parent→bucket links (ADR-022 §3): `(parent_id, type_id) → child_id`.
/// Ordered, so a parent's buckets form a contiguous range scan. Shared
/// lock-free — the snapshot writer inserts; query readers range/get.
type Links = Arc<SkipMap<(u64, u16), u64>>;

pub struct Snapshot {
    accounts: Accounts,
    links: Links,
    indexer: Option<TransactionIndexer>,
    storage: Arc<Storage>,
    resize_factor: f64,
}

struct SnapshotRunner {
    accounts: Accounts,
    links: Links,
    indexer: TransactionIndexer,
    storage: Arc<Storage>,
    /// First tx id to tail (recovered `snapshot_index` + 1).
    from_tx_id: u64,
    resize_factor: f64,
}

impl Snapshot {
    pub fn new(config: &LedgerConfig, storage: Arc<Storage>) -> Self {
        // Start small and grow on OpenAccount; recovery resizes to the
        // persisted allocator high-water (ADR-022).
        let initial = config.initial_account_size.max(1);
        let accounts: Accounts = Arc::new(ArcSwap::from_pointee(
            (0..initial)
                .map(|_| SnapshotAccount::new())
                .collect::<Vec<_>>(),
        ));
        // Only account 0 (SYSTEM) is existent at genesis; ids 1.. stay absent.
        Self::mark_system_account(&accounts.load());

        Self {
            accounts,
            links: Arc::new(SkipMap::new()),
            indexer: Some(TransactionIndexer::new(
                config.index_circle1_size(),
                config.index_circle2_size(),
            )),
            storage,
            resize_factor: config.resize_factor,
        }
    }

    /// Bootstrap account 0 (SYSTEM) as existent — ONLY id 0. Every other cell
    /// stays `flags = 0` (NOT_EXISTENT) until opened. Matches the transactor.
    fn mark_system_account(accounts: &[SnapshotAccount]) {
        if let Some(sys) = accounts.first() {
            let mut f = 0u64;
            set_flag(&mut f, 0, STATUS_SYSTEM);
            sys.flags.store(f, Ordering::Release);
        }
    }

    /// Grow the account vector to cover `needed` ids (geometric, ADR-022),
    /// preserving existing balances/flags. No-op when already large enough.
    /// Safe because the Snapshot stage (and recovery, pre-start) is the only
    /// writer: it copies the current cells and swaps in the larger generation.
    fn ensure_capacity(accounts: &ArcSwap<Vec<SnapshotAccount>>, needed: usize, factor: f64) {
        let cur = accounts.load();
        if needed <= cur.len() {
            return;
        }
        let new_cap = grow_capacity(cur.len(), factor, needed);
        let mut next: Vec<SnapshotAccount> = Vec::with_capacity(new_cap);
        for a in cur.iter() {
            next.push(SnapshotAccount {
                balance: AtomicI64::new(a.balance.load(Ordering::Acquire)),
                flags: AtomicU64::new(a.flags.load(Ordering::Acquire)),
            });
        }
        next.resize_with(new_cap, SnapshotAccount::new);
        accounts.store(Arc::new(next));
    }

    pub fn start(
        &mut self,
        ctx: SnapshotContext,
        active_snapshot: &ActiveSnapshot,
    ) -> std::io::Result<JoinHandle<()>> {
        // Restore read-side state synchronously so reads are correct the moment
        // `start` returns, then resume tailing the WAL after the recovered tail.
        self.recover_from(active_snapshot);
        ctx.set_processed_index(active_snapshot.last_tx_id);
        let from_tx_id = active_snapshot.last_tx_id + 1;
        let runner = SnapshotRunner {
            accounts: self.accounts.clone(),
            links: self.links.clone(),
            indexer: self.indexer.take().unwrap(),
            storage: self.storage.clone(),
            from_tx_id,
            resize_factor: self.resize_factor,
        };
        std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut r = runner;
                r.run(ctx);
            })
    }

    /// Restore accounts, links, and the transaction index from `active_snapshot`
    /// (read-side self-recovery), written into the shared cells before the runner
    /// starts. The index is warmed from the active segment's committed txs.
    fn recover_from(&mut self, snap: &ActiveSnapshot) {
        self.recover_account_layout(snap.next_account_id);
        let acc = self.accounts.load();
        for (&id, account) in &snap.accounts {
            if let Some(a) = acc.get(id as usize) {
                a.balance.store(account.balance, Ordering::Release);
                if id != SYSTEM_ACCOUNT_ID {
                    a.flags.store(account.flags, Ordering::Release);
                }
            }
        }
        for &(parent_id, type_id, child_id) in &snap.links {
            self.links.insert((parent_id, type_id), child_id);
        }
        for tx in &snap.active_segment_transactions {
            self.recover_index_transaction(&tx.meta, &tx.entries);
        }
    }

    /// Reconstruct account status from the recovered allocator high-water
    /// (ADR-022): grow to cover it, then mark SYSTEM(0) plus `[1, next)` OPEN.
    /// Mirrors the transactor so the read-side reflects existence for
    /// opened-but-unfunded accounts.
    fn recover_account_layout(&mut self, next_account_id: u64) {
        Self::ensure_capacity(&self.accounts, next_account_id as usize, self.resize_factor);
        let acc = self.accounts.load();
        Self::mark_system_account(&acc);
        let mut open = 0u64;
        set_flag(&mut open, 0, STATUS_OPEN);
        let upper = (next_account_id as usize).min(acc.len());
        for id in 1..upper {
            acc[id].flags.store(open, Ordering::Release);
        }
    }

    /// Insert one recovered transaction (meta + followers) into the index so
    /// recent transactions are queryable immediately after recovery.
    fn recover_index_transaction(&mut self, meta: &TxMetadata, followers: &[WalEntry]) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_transaction(meta, followers);
        }
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.accounts
            .load()
            .get(account_id as usize)
            .map(|a| a.balance.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Raw 8-lane flags word for `account_id` (lane 0 = status). Lets query
    /// handlers distinguish OPEN / PROGRAMMED / SYSTEM / absent accounts.
    /// Returns 0 (NOT_EXISTENT) for ids beyond the current vector.
    pub fn get_flags(&self, account_id: u64) -> u64 {
        self.accounts
            .load()
            .get(account_id as usize)
            .map(|a| a.flags.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Per-type linked-bucket balances for `parent_id` (ADR-022 §8): range-scan
    /// the link table for `(parent_id, *)` and read each child's balance.
    pub fn linked_balances(&self, parent_id: u64) -> Vec<(u16, Balance)> {
        let accounts = self.accounts.load();
        self.links
            .range((parent_id, 0)..=(parent_id, u16::MAX))
            .map(|e| {
                let type_id = e.key().1;
                let child = *e.value();
                let bal = accounts
                    .get(child as usize)
                    .map(|a| a.balance.load(Ordering::Acquire))
                    .unwrap_or(0);
                (type_id, bal)
            })
            .collect()
    }
}

impl SnapshotRunner {
    pub fn run(&mut self, ctx: SnapshotContext) {
        let mut retry_count = 0;
        let inbound = ctx.query();
        // Tail the durable WAL instead of the ring; the WAL owns ring release now.
        let mut tailer = self.storage.wal_tailer(self.from_tx_id);
        // Trailer layout: buffer a transaction's followers until its TxMetadata clears
        // the commit gate, then apply index + balances together (invisible until durable).
        let mut group: Vec<WalEntry> = Vec::new();
        while ctx.is_running() {
            group.clear();
            let progressed = tailer.tail_entries(|wal_entry| {
                match wal_entry {
                    // Not durable yet: tail_entries retains this group; re-read once committed.
                    WalEntry::Metadata(m) if m.tx_id > ctx.commit_index() => return false,
                    WalEntry::Metadata(m) => {
                        // Index the whole transaction: meta + all followers, stored as-is.
                        self.indexer.insert_transaction(&m, &group);
                        // Apply balances and status flags from the followers.
                        for follower in group.drain(..) {
                            match follower {
                                WalEntry::Entry(e) => {
                                    let acc = self.accounts.load();
                                    if let Some(a) = acc.get(e.account_id as usize) {
                                        a.balance.store(e.computed_balance, Ordering::Release);
                                    }
                                }
                                WalEntry::AccountOpened(a) => {
                                    // Grow to cover the opened range, then publish its flags.
                                    let end = (a.begin_account_id + a.count as u64) as usize;
                                    Snapshot::ensure_capacity(
                                        &self.accounts,
                                        end,
                                        self.resize_factor,
                                    );
                                    let acc = self.accounts.load();
                                    let begin = a.begin_account_id as usize;
                                    for id in begin..end.min(acc.len()) {
                                        acc[id].flags.store(a.flags, Ordering::Release);
                                    }
                                }
                                WalEntry::AccountLinked(a) => {
                                    self.links.insert((a.parent_id, a.type_id), a.child_id);
                                }
                                WalEntry::AccountFlagsUpdated(a) => {
                                    let acc = self.accounts.load();
                                    if let Some(slot) = acc.get(a.account_id as usize) {
                                        slot.flags.store(a.new_flags, Ordering::Release);
                                    }
                                }
                                _ => {}
                            }
                        }
                        ctx.set_processed_index(m.tx_id);
                    }
                    // Buffer every follower as-is until its metadata clears the gate.
                    other => group.push(other),
                }
                true
            });

            if let Some(q) = inbound.pop() {
                retry_count = 0;

                let response = match q.kind {
                    QueryKind::GetTransaction { tx_id } => {
                        let result = self
                            .indexer
                            .get_transaction(tx_id)
                            .map(|(meta, entries)| CommittedTransaction { meta, entries });
                        QueryResponse::Transaction(result)
                    }
                    QueryKind::GetTransactionBatch { tx_ids } => {
                        let txs = tx_ids
                            .iter()
                            .filter_map(|&id| self.indexer.get_transaction(id))
                            .map(|(meta, entries)| CommittedTransaction { meta, entries })
                            .collect();
                        QueryResponse::TransactionBatch(txs)
                    }
                };
                (q.respond)(response);
            } else if !progressed {
                ctx.wait_strategy().retry(retry_count);
                retry_count += 1;
            } else {
                retry_count = 0;
            }
        }
    }
}
