use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::config::LedgerConfig;
use ledger::ledger::WaitStrategy;
use ledger::snapshot::Snapshot;
use ledger::test_support::{ring_pipeline, ring_push};
use ledger::wal::Wal;
use std::sync::Arc;
use std::time::Duration;
use storage::Storage;
use storage::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};

/// Trailer layout: the follower(s) come first, the closing `TxMetadata` last — that
/// is the order the WAL persists and the snapshot tailer groups by.
fn make_deposit_entries(tx_id: u64, account_id: u64, amount: u64) -> [WalEntry; 2] {
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        _pad1: [0; 8],
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    let metadata = TxMetadata {
        entry_type: WalEntryKind::TxMetadata as u8,
        fail_reason: FailReason::NONE,
        sub_item_count: 1,
        crc32c: 0,
        tx_id,
        timestamp: 0,
        user_ref: 0,
        tag: [0; 8],
    };
    [WalEntry::Entry(entry), WalEntry::Metadata(metadata)]
}

/// End-to-end snapshot throughput. The snapshot now tails the durable WAL rather than
/// the ring, so it can't be benched in isolation: a real WAL stage persists the
/// entry+meta groups to disk and advances `commit_index`, and the snapshot stage tails
/// the file and indexes them. Measures the feed→WAL→snapshot pipeline (minus the
/// transactor).
fn snapshot_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let config = LedgerConfig {
        initial_account_size: 1_000_000,
        ..LedgerConfig::bench()
    };
    let storage = Arc::new(Storage::new(config.storage.clone()).unwrap());

    let (pipeline, mut writer, reader) = ring_pipeline(1 << 20, 1 << 16, WaitStrategy::Balanced);

    // Real WAL: persists groups to disk and advances commit_index; owns the ring reader.
    let mut wal = Wal::new(storage.clone(), reader);
    let wal_handles = wal.start(pipeline.wal_context()).unwrap();

    // Snapshot tails the durable WAL (no ring, no releaser) and indexes committed groups.
    let mut snapshot = Snapshot::new(&config, storage.clone());
    let snap_handle = snapshot.start(pipeline.snapshot_context()).unwrap();

    let mut current_id = 0u64;
    group.bench_function("process", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 1_000_000;
            let [entry, meta] = make_deposit_entries(current_id, account_id, 100);
            ring_push(&mut writer, entry);
            ring_push(&mut writer, meta);
            writer.commit(); // publish the completed tx group
        });
    });

    group.finish();
    pipeline.shutdown();
    for h in wal_handles {
        let _ = h.join();
    }
    let _ = snap_handle.join();
}

criterion_group!(benches, snapshot_bench);
criterion_main!(benches);
