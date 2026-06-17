//! Benchmark `WalTailer::new` (the locate step) — the hot path that
//! `replication_driver::run_peer_sender` calls every time a new peer
//! session starts. The locate walks back through sealed segments and
//! scans the chosen segment forward to find the first `TxMetadata`
//! with `tx_id >= from_tx_id`.
//!
//! Layout the suite uses:
//!   - 10 000 000 logical transactions total
//!   - 1 000 000 transactions per segment ⇒ 9 sealed segments + 1 active
//!   - Each "transaction" is 1 follower + its trailing meta = 2 records =
//!     80 bytes (trailer layout, ADR-020) ⇒ ~80 MB per segment, ~800 MB total
//!
//! Four locate positions, each chosen to exercise a different code path:
//!   - `active`: `tx_id = 9_500_000` — inside the active wal.bin
//!   - `near_beginning`: `tx_id = 100` — first sealed segment, very
//!     near the start of the file
//!   - `middle`: `tx_id = 5_000_000` — middle sealed segment
//!   - `end_before_active`: `tx_id = 8_500_000` — last sealed segment,
//!     close to the boundary with the active one
//!
//! The dataset is built once (lazy `OnceLock`) and shared across all
//! benches. Build time is non-trivial (~minutes for 800 MB); the
//! `BUILD_INFO`/`LOCATE_INFO` env-var hooks let an investigator skip
//! the build by pointing at an existing data dir.
//!
//! Run with:
//!     cargo bench -p storage --bench wal_tail_bench

use std::sync::{Arc, OnceLock};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use storage::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};
use storage::{Storage, StorageConfig, WalTailer};

const TOTAL_TX: u64 = 10_000_000;
const TX_PER_SEGMENT: u64 = 1_000_000;
/// Records per logical transaction: TxMetadata + one TxEntry.
const RECORDS_PER_TX: u64 = 2;
/// Total 40-byte records in the dataset.
const TOTAL_RECORDS: u64 = TOTAL_TX * RECORDS_PER_TX;
/// Flush every N records to keep the write buffer bounded during the
/// bulk fill. Picked at ~32 MB which fits comfortably in page cache.
const FLUSH_EVERY_RECORDS: u64 = 800_000;
/// Buffer size for the `tail` throughput bench — matches the
/// production default for `append_entries_max_bytes` so the result
/// is directly comparable to the replication driver's hot path.
const TAIL_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// Cached dataset. Building 800 MB of WAL records takes a while, so
/// `dataset()` returns the same `Storage` across all benches and the
/// `TempDir` is retained for the lifetime of the process.
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
    // 9 segments hit the rotate threshold and seal; the 10th holds the
    // remaining 1M txs as the active segment.
    let expected_segments = TOTAL_TX.div_ceil(TX_PER_SEGMENT);
    eprintln!(
        "wal_tail_bench: building {} txs across {} segments at {:?}",
        TOTAL_TX,
        expected_segments,
        temp.path()
    );

    let cfg = StorageConfig {
        data_dir: temp.path().to_string_lossy().into_owned(),
        transaction_count_per_segment: TX_PER_SEGMENT,
        ..StorageConfig::default()
    };
    let storage = Arc::new(Storage::new(cfg).expect("storage open"));

    // We write straight into the active segment via `write_entries`
    // (which fsyncs implicitly via the file append). When a segment
    // hits its tx limit, we close it and let `Storage` advance to the
    // next active segment id — mirroring what `Wal::rotate` does.
    let started = std::time::Instant::now();
    let mut segment = storage.active_segment().expect("open active");
    let mut buf: Vec<WalEntry> = Vec::with_capacity(FLUSH_EVERY_RECORDS as usize);
    let mut tx_in_segment: u64 = 0;

    for tx_id in 1..=TOTAL_TX {
        push_tx(&mut buf, tx_id);
        tx_in_segment += 1;

        let buf_records = buf.len() as u64;
        let need_flush = buf_records >= FLUSH_EVERY_RECORDS || tx_in_segment == TX_PER_SEGMENT;
        if need_flush {
            segment.write_entries(&buf);
            buf.clear();
        }

        if tx_in_segment == TX_PER_SEGMENT && tx_id < TOTAL_TX {
            // Rotate: close the active segment, bump the storage's
            // segment id, open the next active segment.
            segment.close().expect("close active segment");
            storage.next_segment();
            segment = storage.active_segment().expect("open next active");
            tx_in_segment = 0;
            let pct = (tx_id as f64 / TOTAL_TX as f64) * 100.0;
            eprintln!(
                "wal_tail_bench:   built {} txs ({:.1}%) in {:.1}s",
                tx_id,
                pct,
                started.elapsed().as_secs_f64()
            );
        }
    }

    if !buf.is_empty() {
        segment.write_entries(&buf);
    }

    eprintln!(
        "wal_tail_bench: build complete in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    drop(segment);

    Dataset {
        storage,
        _data_dir_guard: temp,
    }
}

fn push_tx(buf: &mut Vec<WalEntry>, tx_id: u64) {
    // Trailer layout (ADR-020): the follower(s) precede the closing
    // TxMetadata, which is the commit record. `tail()`/`tail_transactions`
    // group on this order, so the fixture must match it — meta-first would
    // mis-group and the partial-tail trim would drop the final follower.
    buf.push(WalEntry::Entry(TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::CREDIT,
        _pad0: [0; 6],
        _pad1: [0; 8],
        account_id: 1,
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

/// Hand-rolled bench helper: build a tailer from `from_tx_id` and
/// time the construction. Criterion's iterator drives the timing —
/// the tailer is dropped each iteration so the next call pays the
/// full locate cost (page-cache hit is fine; we want to measure the
/// algorithmic cost, not cold-disk IO).
fn bench_locate(c: &mut Criterion, name: &str, from_tx_id: u64) {
    let ds = dataset();
    let storage = ds.storage.clone();
    c.bench_function(name, |b| {
        b.iter(|| {
            let t = WalTailer::new(storage.clone(), from_tx_id);
            // Keep the tailer alive past `iter`'s body so the
            // optimizer can't see it as unused and drop the locate.
            criterion::black_box(&t);
        });
    });
}

fn locate_active(c: &mut Criterion) {
    bench_locate(c, "locate/active(9_500_000)", 9_500_000);
}

fn locate_near_beginning(c: &mut Criterion) {
    bench_locate(c, "locate/near_beginning(100)", 100);
}

fn locate_middle(c: &mut Criterion) {
    bench_locate(c, "locate/middle(5_000_000)", 5_000_000);
}

fn locate_end_before_active(c: &mut Criterion) {
    bench_locate(c, "locate/end_before_active(8_500_000)", 8_500_000);
}

/// Drain the entire 10M-tx WAL from `from_tx_id = 1` and measure
/// per-record throughput. Each iteration rebuilds the tailer (so the
/// locate cost is included) and pumps `tail()` until EOF.
///
/// `Throughput::Elements(TOTAL_RECORDS)` tells criterion how many
/// 40-byte records one iteration processes — it reports the result as
/// `elem/s` directly. The bytes-per-second number can be derived as
/// `elem/s × 40`.
///
/// Buffer size is fixed at [`TAIL_BUFFER_BYTES`] (4 MiB, matching the
/// production default for `append_entries_max_bytes`).
fn tail_full_drain(c: &mut Criterion) {
    let ds = dataset();
    let storage = ds.storage.clone();
    let mut group = c.benchmark_group("tail/full_drain");
    group.throughput(Throughput::Elements(TOTAL_RECORDS));
    // 800 MB drain per iter — keep the sample count low so the wall
    // time stays bounded but criterion still gets stable measurements.
    group.sample_size(10);
    group.bench_function("from_tx_id=1", |b| {
        let mut buf = vec![0u8; TAIL_BUFFER_BYTES];
        b.iter(|| {
            let mut tailer = WalTailer::new(storage.clone(), 1);
            let mut bytes_total: u64 = 0;
            loop {
                let n = tailer.tail(&mut buf) as u64;
                if n == 0 {
                    break;
                }
                bytes_total += n;
            }
            // Sanity: every record must come out — if the tailer
            // mis-trims or skips a segment boundary, this assertion
            // will fire and the result number becomes meaningless.
            // Cheap check, runs once per iter.
            assert_eq!(
                bytes_total,
                TOTAL_RECORDS * 40,
                "tailer drained {} bytes; expected {} ({} records × 40)",
                bytes_total,
                TOTAL_RECORDS * 40,
                TOTAL_RECORDS
            );
            criterion::black_box(bytes_total)
        });
    });
    group.finish();
}

/// Drain the entire 10M-tx WAL from `from_tx_id = 1` via the
/// transaction-oriented [`WalTailer::tail_transactions`] path and measure
/// per-record throughput. Each iteration rebuilds the tailer (locate cost
/// included) and counts every record delivered (meta + followers), so the
/// number is directly comparable to [`tail_full_drain`], which counts the
/// same raw records off `tail()`.
///
/// `tail_transactions` is fully zero-copy — the closing `TxMetadata` and the
/// `EntryBuf` follower view both borrow the 4 MiB read buffer, no per-tx
/// allocation — so this isolates the per-transaction grouping/decode cost
/// layered on top of `tail()`'s raw byte streaming.
fn tail_transactions_full_drain(c: &mut Criterion) {
    let ds = dataset();
    let storage = ds.storage.clone();
    let mut group = c.benchmark_group("tail_transactions/full_drain");
    group.throughput(Throughput::Elements(TOTAL_RECORDS));
    // 800 MB drain per iter — match `tail_full_drain`'s low sample count.
    group.sample_size(10);
    group.bench_function("from_tx_id=1", |b| {
        b.iter(|| {
            let mut tailer = WalTailer::new(storage.clone(), 1);
            let mut txs: u64 = 0;
            let mut records: u64 = 0;
            // One read per call (like the snapshot loop); pump until caught up.
            // The gate always accepts (`true`), so this drains the whole WAL.
            while tailer.tail_transactions(|tx| {
                txs += 1;
                records += 1 + tx.entries.len() as u64;
                criterion::black_box(tx.meta.tx_id);
                true
            }) {}
            // Sanity: every transaction (and thus every record) must surface;
            // a mis-grouped boundary would make the throughput meaningless.
            assert_eq!(
                txs, TOTAL_TX,
                "tail_transactions delivered {txs} txs; expected {TOTAL_TX}"
            );
            assert_eq!(
                records, TOTAL_RECORDS,
                "tail_transactions delivered {records} records; expected {TOTAL_RECORDS}"
            );
            criterion::black_box(records)
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(30);
    targets =
        locate_active,
        locate_near_beginning,
        locate_middle,
        locate_end_before_active,
        tail_full_drain,
        tail_transactions_full_drain
}
criterion_main!(benches);
