//! Benchmarks for `WalTailer` — the ADR-015 leader-side raw WAL reader.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use storage::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};
use ledger::ledger::{Ledger, LedgerConfig, StorageConfig};
use std::time::Duration;

const WAL_RECORD_SIZE: usize = 40;

fn deposit_entries(tx_id: u64, account: u64, amount: u64) -> Vec<WalEntry> {
    let meta = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        entry_count: 1,
        link_count: 0,
        fail_reason: FailReason::NONE,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id: account,
        amount,
        computed_balance: amount as i64,
    };
    vec![WalEntry::Metadata(meta), WalEntry::Entry(entry)]
}

/// Prefill a ledger with `tx_count` committed transactions via the follower
/// path so there is no transactor overhead in the setup.
fn prefilled_ledger(tx_count: u64, tx_per_segment: u64) -> Ledger {
    let mut cfg = LedgerConfig::bench();
    cfg.storage = StorageConfig {
        transaction_count_per_segment: tx_per_segment,
        ..cfg.storage
    };
    let mut ledger = Ledger::new(cfg);
    ledger.start().unwrap();

    // Batch into ~1000-tx chunks so the Multi slots stay reasonable.
    let chunk = 1024u64;
    let mut next = 1u64;
    while next <= tx_count {
        let end = (next + chunk - 1).min(tx_count);
        let mut batch = Vec::with_capacity(((end - next + 1) * 2) as usize);
        for tx_id in next..=end {
            batch.extend(deposit_entries(tx_id, tx_id, 10));
        }
        ledger.append_wal_entries(batch).unwrap();
        next = end + 1;
    }

    // Wait for commit + drain so everything is on disk before benching.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while ledger.last_commit_id() < tx_count {
        if std::time::Instant::now() > deadline {
            panic!("prefill timed out at commit={}", ledger.last_commit_id());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    ledger
}

/// Passive full-history tail: one large buffer, one call. Measures raw
/// segment-scan throughput.
fn bench_tail_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_tail/full_scan");
    group.measurement_time(Duration::from_secs(8));

    for &tx_count in &[1_000u64, 10_000, 100_000] {
        // Keep everything in one segment for this scenario.
        let ledger = prefilled_ledger(tx_count, 10_000_000);
        // 3 records per deposit (meta + 0 entries here: follower Multi has 1 entry).
        let total_records = tx_count as usize * 2;
        let buf_size = total_records * WAL_RECORD_SIZE;

        group.throughput(Throughput::Elements(total_records as u64));
        group.bench_with_input(BenchmarkId::from_parameter(tx_count), &tx_count, |b, _| {
            let mut buf = vec![0u8; buf_size];
            b.iter(|| {
                let mut tailer = ledger.wal_tailer();
                let n = tailer.tail(1, &mut buf);
                criterion::black_box(n);
            });
        });
    }

    group.finish();
}

/// Resumed streaming: many small-buffer calls feeding on the cached cursor.
/// Shows the win from not rescanning the prefix.
fn bench_tail_resumed(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_tail/resumed");
    group.measurement_time(Duration::from_secs(8));

    let tx_count: u64 = 50_000;
    let ledger = prefilled_ledger(tx_count, 10_000_000);
    let records = tx_count as usize * 2;
    group.throughput(Throughput::Elements(records as u64));

    for &buf_records in &[16usize, 256, 4096] {
        group.bench_with_input(
            BenchmarkId::from_parameter(buf_records),
            &buf_records,
            |b, _| {
                let mut buf = vec![0u8; buf_records * WAL_RECORD_SIZE];
                b.iter(|| {
                    let mut tailer = ledger.wal_tailer();
                    loop {
                        let n = tailer.tail(1, &mut buf);
                        if n == 0 {
                            break;
                        }
                        criterion::black_box(n);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Tail across a rotated WAL: small `transaction_count_per_segment`
/// forces many sealed segments to open during the scan.
fn bench_tail_rotated(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_tail/rotated");
    group.measurement_time(Duration::from_secs(8));

    for &(tx_count, tx_per_seg) in &[(10_000u64, 100u64), (10_000, 1_000), (10_000, 10_000)] {
        let ledger = prefilled_ledger(tx_count, tx_per_seg);
        let records = tx_count as usize * 2;
        let buf_size = records * WAL_RECORD_SIZE;
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(
            BenchmarkId::new("tx_per_seg", tx_per_seg),
            &tx_per_seg,
            |b, _| {
                let mut buf = vec![0u8; buf_size];
                b.iter(|| {
                    let mut tailer = ledger.wal_tailer();
                    let n = tailer.tail(1, &mut buf);
                    criterion::black_box(n);
                });
            },
        );
    }

    group.finish();
}

/// Active tailing: new records appended between each `tail()` call. Measures
/// the per-call overhead when only a small tail of fresh bytes is unread.
fn bench_tail_active_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_tail/active");
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(1));

    let ledger = prefilled_ledger(10_000, 10_000_000);
    let mut tailer = ledger.wal_tailer();
    // Drain the prefilled history once so the tailer is caught up.
    let mut drain_buf = vec![0u8; 40 * 40_000];
    loop {
        let n = tailer.tail(1, &mut drain_buf);
        if n == 0 {
            break;
        }
    }

    let mut next_tx = 10_001u64;
    let mut buf = vec![0u8; 40 * 16];
    group.bench_function("append_then_tail", |b| {
        b.iter(|| {
            ledger
                .append_wal_entries(deposit_entries(next_tx, 1, 10))
                .unwrap();
            next_tx += 1;
            // Spin briefly until the fresh record is committed, then tail.
            while ledger.last_commit_id() < next_tx - 1 {
                std::hint::spin_loop();
            }
            let n = tailer.tail(1, &mut buf);
            criterion::black_box(n);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_tail_full_scan,
    bench_tail_resumed,
    bench_tail_rotated,
    bench_tail_active_append,
);
criterion_main!(benches);
