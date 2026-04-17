use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::entities::{TxEntry, TxLink, TxMetadata, WalEntry};
use crate::index::{IndexedTxEntry, IndexedTxLink, TransactionIndexer};
use crate::pipeline::SnapshotContext;
use crate::storage::Storage;
use crate::wasm_runtime::WasmRuntime;
use spdlog::error;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::JoinHandle;
// ── Message types for the Snapshot stage queue (ADR-008) ─────────────────────

/// Single message type for the WAL→Snapshot queue.
///
/// Both `WalEntry` records and query requests flow through a single FIFO queue,
/// which gives **read-your-own-writes consistency** for free: a query submitted
/// after a transaction is enqueued after its entries and always sees the committed
/// state.
pub enum SnapshotMessage {
    Entry(WalEntry),
    Query(QueryRequest),
}

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
    /// Shared WASM registry — bumped on every `FunctionRegistered` commit
    /// so each `WasmRuntimeEngine` passively refreshes its handler cache.
    wasm_runtime: Arc<WasmRuntime>,
    /// Storage handle for resolving `{data_dir}/functions/{name}_v{N}.wasm`.
    storage: Arc<Storage>,
}

struct SnapshotRunner {
    balances: Arc<Vec<AtomicI64>>,
    /// Total remaining records (entries + links) for the current transaction.
    pending_records: u8,
    indexer: TransactionIndexer,
    wasm_runtime: Arc<WasmRuntime>,
    storage: Arc<Storage>,
}

impl Snapshot {
    pub fn new(
        config: &LedgerConfig,
        wasm_runtime: Arc<WasmRuntime>,
        storage: Arc<Storage>,
    ) -> Self {
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
            wasm_runtime,
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
        let runner = SnapshotRunner {
            balances: self.balances.clone(),
            pending_records: 0,
            indexer: self.indexer.take().unwrap(),
            wasm_runtime: self.wasm_runtime.clone(),
            storage: self.storage.clone(),
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
            indexer.insert_tx(metadata.tx_id, metadata.entry_count);
        }
    }

    pub(crate) fn recover_index_tx_link(&mut self, tx_id: u64, link: &TxLink) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_link(tx_id, link.kind(), link.to_tx_id);
        }
    }

    pub(crate) fn recover_index_tx_entry(&mut self, entry: &TxEntry) {
        if let Some(indexer) = &mut self.indexer {
            indexer.insert_entry(
                entry.tx_id,
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
        let mut last_processed_tx_id = 0;
        let inbound = ctx.input();
        while ctx.is_running() {
            if let Some(message) = inbound.pop() {
                retry_count = 0;

                match message {
                    SnapshotMessage::Entry(wal_entry) => {
                        match wal_entry {
                            WalEntry::Metadata(m) => {
                                self.indexer.insert_tx(m.tx_id, m.entry_count);
                                self.pending_records = m.entry_count.saturating_add(m.link_count);
                                last_processed_tx_id = m.tx_id;
                            }
                            WalEntry::Entry(e) => {
                                self.indexer.insert_entry(
                                    e.tx_id,
                                    e.account_id,
                                    e.amount,
                                    e.kind,
                                    e.computed_balance,
                                );
                                self.balances[e.account_id as usize]
                                    .store(e.computed_balance, Ordering::Release);
                                self.pending_records = self.pending_records.saturating_sub(1);
                            }
                            WalEntry::Link(l) => {
                                self.indexer.insert_link(
                                    last_processed_tx_id,
                                    l.kind(),
                                    l.to_tx_id,
                                );
                                self.pending_records = self.pending_records.saturating_sub(1);
                            }
                            WalEntry::SegmentSealed(_) => {}
                            WalEntry::SegmentHeader(_) => {}
                            WalEntry::FunctionRegistered(f) => {
                                // Register / unregister handler. We never
                                // touch pending_records or the tx_id cursor
                                // — this record is not a financial
                                // transaction.
                                let name = f.name_str().to_string();
                                if f.is_unregister() {
                                    if let Err(e) =
                                        self.wasm_runtime.unload_function(&name)
                                    {
                                        error!(
                                            "Snapshot: unload_function({}) failed: {}",
                                            name, e
                                        );
                                    }
                                } else {
                                    match self.storage.read_function(&name, f.version) {
                                        Ok(binary) => {
                                            if let Err(e) = self.wasm_runtime.load_function(
                                                &name,
                                                &binary,
                                                f.version,
                                                f.crc32c,
                                            ) {
                                                error!(
                                                    "Snapshot: load_function({} v{}) failed: {}",
                                                    name, f.version, e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Snapshot: read_function({} v{}) failed: {}",
                                                name, f.version, e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        if last_processed_tx_id > 0 && self.pending_records == 0 {
                            ctx.set_processed_index(last_processed_tx_id);
                        }
                    }
                    SnapshotMessage::Query(q) => {
                        let response = match q.kind {
                            QueryKind::GetTransaction { tx_id } => {
                                let result = self.indexer.get_transaction(tx_id).map(|entries| {
                                    let links =
                                        self.indexer.get_links(tx_id).cloned().unwrap_or_default();
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
                    }
                }
            } else {
                ctx.wait_strategy().retry(retry_count);
                retry_count += 1;
            }
        }
    }
}
