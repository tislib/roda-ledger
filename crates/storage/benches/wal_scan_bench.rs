//! `WalScanner` backward-scan throughput (ADR-022) — the read path behind
//! `GetAccountHistory`. `Throughput::Elements(TOTAL_TX)` reports the headline
//! per-element rate (transactions/sec); the per-scan time is criterion's `time`.
//! `match_all` matches every tx (decode + yield); `match_none` matches none
//! (pure backward traversal). Dataset: 1M txs in one segment, built once.
//!
//! Run with: cargo bench -p storage --bench wal_scan_bench

use std::sync::{Arc, OnceLock};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use storage::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};
use storage::{Storage, StorageConfig, WalEntryRef};

const TOTAL_TX: u64 = 1_000_000;
/// Records per logical transaction: one TxEntry follower + its TxMetadata.
const RECORDS_PER_TX: u64 = 2;
/// Flush the write buffer every N records during the bulk fill (~32 MB).
const FLUSH_EVERY_RECORDS: usize = 800_000;
/// The account every transaction touches (so `match_all` hits every tx).
const TARGET_ACCOUNT: u64 = 1;
/// Transactions per bounded scan — a realistic `GetAccountHistory` page.
const SCAN_TX: u64 = 10_000;

struct Dataset {
    storage: Arc<Storage>,
    // Kept so the temp dir survives `dataset()` returning.
    _data_dir_guard: tempfile::TempDir,
}

fn dataset() -> &'static Dataset {
    static CELL: OnceLock<Dataset> = OnceLock::new();
    CELL.get_or_init(build)
}

fn build() -> Dataset {
    let temp = tempfile::tempdir().expect("temp dir");
    eprintln!(
        "wal_scan_bench: building {} txs ({} records) at {:?}",
        TOTAL_TX,
        TOTAL_TX * RECORDS_PER_TX,
        temp.path()
    );
    let cfg = StorageConfig {
        data_dir: temp.path().to_string_lossy().into_owned(),
        // Single active segment — no rotation; the scanner reads it backward.
        transaction_count_per_segment: TOTAL_TX + 1,
        ..StorageConfig::default()
    };
    let storage = Arc::new(Storage::new(cfg).expect("storage open"));

    let started = std::time::Instant::now();
    let mut segment = storage.active_segment().expect("open active");
    let mut buf: Vec<WalEntry> = Vec::with_capacity(FLUSH_EVERY_RECORDS);
    for tx_id in 1..=TOTAL_TX {
        push_tx(&mut buf, tx_id);
        if buf.len() >= FLUSH_EVERY_RECORDS {
            segment.write_entries(&buf);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        segment.write_entries(&buf);
    }
    drop(segment);
    eprintln!(
        "wal_scan_bench: build complete in {:.1}s",
        started.elapsed().as_secs_f64()
    );

    Dataset {
        storage,
        _data_dir_guard: temp,
    }
}

/// Push one transaction in trailer order: the follower first, then its closing
/// `TxMetadata` (the scanner reads the metadata and jumps back over followers).
fn push_tx(buf: &mut Vec<WalEntry>, tx_id: u64) {
    buf.push(WalEntry::Entry(TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::CREDIT,
        _pad0: [0; 6],
        _pad1: [0; 8],
        account_id: TARGET_ACCOUNT,
        amount: 10,
        computed_balance: 10,
    }));
    buf.push(WalEntry::Metadata(TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        fail_reason: FailReason::NONE,
        sub_item_count: 1,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    }));
}

/// Every transaction matches → the handler fires for each. Measures scan +
/// decode + yield throughput (transactions/sec).
fn scan_match_all(c: &mut Criterion) {
    let storage = dataset().storage.clone();
    let mut group = c.benchmark_group("wal_scan/match_all");
    group.throughput(Throughput::Elements(TOTAL_TX));
    group.sample_size(10);
    group.bench_function("scan_yield_every_tx", |b| {
        b.iter(|| {
            let mut count = 0u64;
            storage.wal_scanner().scan(
                0,
                0,
                |e| matches!(e, WalEntryRef::Entry(te) if te.account_id == TARGET_ACCOUNT),
                |_tx| {
                    count += 1;
                    true
                },
            );
            assert_eq!(count, TOTAL_TX, "match_all must yield every transaction");
            criterion::black_box(count)
        });
    });
    group.finish();
}

/// No transaction matches → the handler never fires. Measures the pure
/// backward traversal + decode cost (the floor for any scan).
fn scan_match_none(c: &mut Criterion) {
    let storage = dataset().storage.clone();
    let mut group = c.benchmark_group("wal_scan/match_none");
    group.throughput(Throughput::Elements(TOTAL_TX));
    group.sample_size(10);
    group.bench_function("scan_no_match", |b| {
        b.iter(|| {
            let scan_last = storage.wal_scanner().scan(
                0,
                0,
                |e| matches!(e, WalEntryRef::Entry(te) if te.account_id == u64::MAX),
                |_tx| true,
            );
            assert_eq!(scan_last, 1, "must scan back to the oldest transaction");
            criterion::black_box(scan_last)
        });
    });
    group.finish();
}

/// Per-scan latency of a bounded 10k-transaction page from the newest, capped
/// via `max_scan` — the cost a real `GetAccountHistory` window pays.
fn scan_window_10k(c: &mut Criterion) {
    let storage = dataset().storage.clone();
    let mut group = c.benchmark_group("wal_scan/window_10k");
    group.throughput(Throughput::Elements(SCAN_TX));
    group.bench_function("scan_newest_10k_txs", |b| {
        b.iter(|| {
            let mut count = 0u64;
            storage.wal_scanner().scan(
                0,
                SCAN_TX * RECORDS_PER_TX,
                |e| matches!(e, WalEntryRef::Entry(te) if te.account_id == TARGET_ACCOUNT),
                |_tx| {
                    count += 1;
                    true
                },
            );
            assert_eq!(count, SCAN_TX, "bounded scan must examine exactly 10k txs");
            criterion::black_box(count)
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = scan_match_all, scan_match_none, scan_window_10k
}
criterion_main!(benches);
