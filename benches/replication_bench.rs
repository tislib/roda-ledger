//! Follower-side replication benchmark (ADR-015).
//!
//! Measures `Replication::process` throughput on a pre-built WAL byte
//! range. Mirrors the wal_bench setup: a Wal runner + a drain thread on
//! the snapshot queue, so commit watermark advances naturally.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::config::LedgerConfig;
use roda_ledger::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntryKind};
use roda_ledger::ledger::WaitStrategy;
use roda_ledger::pipeline::Pipeline;
use roda_ledger::replication::{AppendEntries, Replication};
use roda_ledger::storage::Storage;
use roda_ledger::wal::Wal;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Build a 40-byte TxMetadata + 40-byte TxEntry pair with a correct
/// CRC. Same shape as the one used in `wal_bench.rs` so byte sizes
/// are comparable.
fn build_tx_bytes(tx_id: u64, account_id: u64, amount: u64) -> Vec<u8> {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id,
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    let mut meta = TxMetadata {
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
    let mut d = crc32c::crc32c(bytemuck::bytes_of(&meta));
    d = crc32c::crc32c_append(d, bytemuck::bytes_of(&entry));
    meta.crc32c = d;

    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(bytemuck::bytes_of(&meta));
    out.extend_from_slice(bytemuck::bytes_of(&entry));
    out
}

fn build_range_bytes(first_tx_id: u64, count: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity((count as usize) * 80);
    for i in 0..count {
        out.extend(build_tx_bytes(first_tx_id + i, (i % 1000) + 1, 100));
    }
    out
}

fn replication_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication");
    group.measurement_time(Duration::from_secs(10));

    // Shared Wal + drain setup, reused across parameter sizes.
    let config = LedgerConfig::bench();
    let storage = Arc::new(Storage::new(config.storage.clone()).unwrap());
    let pipeline = Pipeline::with_sizes(10_240_000, 10_240_000, WaitStrategy::Balanced);

    let wal = Wal::new(storage);
    let handles = wal.start(pipeline.wal_context()).unwrap();

    // Drain the wal→snapshot queue so commit progresses.
    let drain_ctx = pipeline.snapshot_context();
    let drain_handle = thread::spawn(move || {
        let mut retry = 0u64;
        while drain_ctx.is_running() || !drain_ctx.input().is_empty() {
            while drain_ctx.input().pop().is_some() {
                retry = 0;
            }
            retry = retry.saturating_add(1);
            drain_ctx.wait_strategy().retry(retry);
        }
    });

    let replication = Replication::new(pipeline.ledger_context(), 2, 1);

    let mut next_first = 1u64;

    for &batch_size in &[1u64, 16, 256, 4096] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::new("process", batch_size),
            &batch_size,
            |b, &n| {
                b.iter(|| {
                    let from = next_first;
                    let to = next_first + n - 1;
                    let bytes = build_range_bytes(from, n);
                    let req = AppendEntries {
                        term: 1,
                        prev_tx_id: if from == 1 { 0 } else { from - 1 },
                        prev_term: if from == 1 { 0 } else { 1 },
                        from_tx_id: from,
                        to_tx_id: to,
                        wal_bytes: &bytes,
                        leader_commit_tx_id: to,
                    };
                    // last_local_tx_id is advertised as the prev; the
                    // replication stage enforces prev_tx_id ==
                    // last_local_tx_id on every call after the first.
                    let last_local = if from == 1 { 0 } else { from - 1 };
                    let res = replication.process(req, last_local).expect("ok");
                    assert_eq!(res.last_tx_id, to);
                    next_first = to + 1;
                });
            },
        );
    }

    group.finish();
    pipeline.shutdown();
    for h in handles {
        let _ = h.join();
    }
    let _ = drain_handle.join();
}

criterion_group!(benches, replication_bench);
criterion_main!(benches);
