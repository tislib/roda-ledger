use crate::balance::Balance;
use crate::entities::{TxEntry, TxLink, TxMetadata, WalEntry};
use crate::index::{IndexedTxEntry, IndexedTxLink, TransactionIndexer};
use crate::pipeline_mode::PipelineMode;
use crossbeam_queue::ArrayQueue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
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
    inbound: Arc<ArrayQueue<SnapshotMessage>>,
    balances: Arc<Vec<AtomicI64>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
    indexer: Option<TransactionIndexer>,
}

struct SnapshotRunner {
    inbound: Arc<ArrayQueue<SnapshotMessage>>,
    balances: Arc<Vec<AtomicI64>>,
    last_processed_transaction_id: Arc<AtomicU64>,
    /// Total remaining records (entries + links) for the current transaction.
    pending_records: u8,
    running: Arc<AtomicBool>,
    pipeline_mode: PipelineMode,
    indexer: TransactionIndexer,
}

impl Snapshot {
    pub fn new(
        inbound: Arc<ArrayQueue<SnapshotMessage>>,
        account_count: usize,
        running: Arc<AtomicBool>,
        pipeline_mode: PipelineMode,
        circle1_size: usize,
        circle2_size: usize,
    ) -> Self {
        let balances: Arc<Vec<AtomicI64>> =
            Arc::new((0..account_count).map(|_| AtomicI64::new(0)).collect());

        let account_heads_size = Self::next_power_of_two(account_count);

        Self {
            inbound,
            balances,
            last_processed_transaction_id: Arc::new(Default::default()),
            running,
            pipeline_mode,
            indexer: Some(TransactionIndexer::new(
                circle1_size,
                circle2_size,
                account_heads_size,
            )),
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

    pub fn start(&mut self) -> std::io::Result<JoinHandle<()>> {
        let runner = SnapshotRunner {
            inbound: self.inbound.clone(),
            balances: self.balances.clone(),
            last_processed_transaction_id: self.last_processed_transaction_id.clone(),
            pending_records: 0,
            running: self.running.clone(),
            pipeline_mode: self.pipeline_mode,
            indexer: self.indexer.take().unwrap(),
        };
        std::thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || {
                let mut r = runner;
                r.run();
            })
    }

    pub fn last_processed_transaction_id(&self) -> u64 {
        self.last_processed_transaction_id.load(Ordering::Acquire)
    }

    pub(crate) fn store_last_processed_id(&self, id: u64) {
        self.last_processed_transaction_id
            .store(id, Ordering::Release);
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
        self.store_last_processed_id(metadata.tx_id);
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
    pub fn run(&mut self) {
        let mut retry_count = 0;
        let mut last_processed_tx_id = 0;
        while self.running.load(Ordering::Relaxed) {
            if let Some(message) = self.inbound.pop() {
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
                        }
                        if last_processed_tx_id > 0 && self.pending_records == 0 {
                            self.last_processed_transaction_id
                                .store(last_processed_tx_id, Ordering::Release);
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
                self.pipeline_mode.wait_strategy(retry_count);
                retry_count += 1;
            }
        }
    }
}
