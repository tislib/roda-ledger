use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::index::{IndexedTxEntry, IndexedTxLink, TransactionIndexer};
use crate::pipeline::SnapshotContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::JoinHandle;
use storage::Storage;
use storage::entities::{TxEntry, TxLink, TxMetadata, WalEntry};

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
    GetTransaction {
        tx_id: u64,
    },
    GetAccountHistory {
        account_id: u64,
        from_tx_id: u64,
        limit: usize,
    },
}

/// Response payload returned via the `QueryRequest.respond` callback.
/// Transaction query result containing entries and links.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionResult {
    pub entries: Vec<IndexedTxEntry>,
    pub links: Vec<IndexedTxLink>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResponse {
    Transaction(Option<TransactionResult>),
    AccountHistory(Vec<IndexedTxEntry>),
}

pub struct Snapshot {
    balances: Arc<Vec<AtomicI64>>,
    indexer: Option<TransactionIndexer>,
    storage: Arc<Storage>,
}

struct SnapshotRunner {
    balances: Arc<Vec<AtomicI64>>,
    indexer: TransactionIndexer,
    storage: Arc<Storage>,
    /// First tx id to tail (recovered `snapshot_index` + 1).
    from_tx_id: u64,
}

impl Snapshot {
    pub fn new(config: &LedgerConfig, storage: Arc<Storage>) -> Self {
        let account_count = config.max_accounts;
        let balances: Arc<Vec<AtomicI64>> =
            Arc::new((0..account_count).map(|_| AtomicI64::new(0)).collect());

        let account_heads_size = Self::next_power_of_two(account_count);

        Self {
            balances,
            indexer: Some(TransactionIndexer::new(
                config.index_circle1_size(),
                config.index_circle2_size(),
                account_heads_size,
            )),
            storage,
        }
    }

    fn next_power_of_two(n: usize) -> usize {
        if n == 0 {
            return 1;
        }
        let mut power = 1;
        while power < n {
            power *= 2;
        }
        power
    }

    pub fn start(&mut self, ctx: SnapshotContext) -> std::io::Result<JoinHandle<()>> {
        // Resume tailing the durable WAL right after the last tx recovery applied.
        let from_tx_id = ctx.get_processed_index() + 1;
        let runner = SnapshotRunner {
            balances: self.balances.clone(),
            indexer: self.indexer.take().unwrap(),
            storage: self.storage.clone(),
            from_tx_id,
        };
        std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut r = runner;
                r.run(ctx);
            })
    }

    pub(crate) fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            self.balances[*account_id as usize].store(*balance, Ordering::Release);
        }
    }

    pub(crate) fn recover_index_tx_metadata(&mut self, metadata: &TxMetadata) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_tx(metadata.tx_id);
        }
    }

    pub(crate) fn recover_index_tx_link(&mut self, tx_id: u64, link: &TxLink) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_link(tx_id, link.kind(), link.to_tx_id);
        }
    }

    pub(crate) fn recover_index_tx_entry(&mut self, tx_id: u64, entry: &TxEntry) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_entry(
                tx_id,
                entry.account_id,
                entry.amount,
                entry.kind,
                entry.computed_balance,
            )
        }
    }

    pub fn get_balance(&self, account_id: u64) -> Balance {
        self.balances[account_id as usize].load(Ordering::Acquire)
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
        let mut entries: Vec<TxEntry> = Vec::new();
        let mut links: Vec<TxLink> = Vec::new();
        while ctx.is_running() {
            entries.clear();
            links.clear();
            let progressed = tailer.tail_entries(|wal_entry| {
                match wal_entry {
                    // Not durable yet: tail_entries retains this group; re-read once committed.
                    WalEntry::Metadata(m) if m.tx_id > ctx.commit_index() => return false,
                    WalEntry::Metadata(m) => {
                        self.indexer.insert_tx(m.tx_id);
                        for e in entries.drain(..) {
                            self.indexer.insert_entry(
                                m.tx_id,
                                e.account_id,
                                e.amount,
                                e.kind,
                                e.computed_balance,
                            );
                            self.balances[e.account_id as usize]
                                .store(e.computed_balance, Ordering::Release);
                        }
                        for l in links.drain(..) {
                            self.indexer.insert_link(m.tx_id, l.kind(), l.to_tx_id);
                        }
                        ctx.set_processed_index(m.tx_id);
                    }
                    WalEntry::Entry(e) => {
                        entries.push(e);
                    }
                    WalEntry::Link(l) => links.push(l),
                    // TxTerm / FunctionRegistered carry no query index.
                    _ => {}
                }
                true
            });

            if let Some(q) = inbound.pop() {
                retry_count = 0;

                let response = match q.kind {
                    QueryKind::GetTransaction { tx_id } => {
                        let result = self.indexer.get_transaction(tx_id).map(|entries| {
                            let links = self.indexer.get_links(tx_id).cloned().unwrap_or_default();
                            TransactionResult { entries, links }
                        });
                        QueryResponse::Transaction(result)
                    }
                    QueryKind::GetAccountHistory {
                        account_id,
                        from_tx_id,
                        limit,
                    } => QueryResponse::AccountHistory(
                        self.indexer
                            .get_account_history(account_id, from_tx_id, limit),
                    ),
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
