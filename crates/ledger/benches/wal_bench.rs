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

fn make_deposit_entries(tx_id: u64, account_id: u64, amount: u64) -> [WalEntry; 2] {
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
    let entry = TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        _pad1: [0; 8],
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    [WalEntry::Metadata(metadata), WalEntry::Entry(entry)]
}

/// Drives the WAL persistence path: the bench feeds meta+entry pairs into the ring,
/// the WAL stage reads them, writes segments and advances `commit_index`, and the
/// snapshot stage (the releaser) reads behind it and frees ring slots — the same
/// wiring as the production pipeline, minus the transactor.
fn wal_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let config = LedgerConfig {
        max_accounts: 1_000_000,
        ..LedgerConfig::bench()
    };
    let storage = Arc::new(Storage::new(config.storage.clone()).unwrap());

    let (pipeline, mut writer, releaser) = ring_pipeline(1 << 20, 1 << 16, WaitStrategy::Balanced);

    let wal = Wal::new(storage.clone());
    let wal_handles = wal.start(pipeline.wal_context()).unwrap();

    // Snapshot stage acts as the releaser: it reads behind `commit_index` (set by
    // the WAL) and advances the ring releaser, so the feeder never blocks forever.
    let mut snapshot = Snapshot::new(&config, storage.clone(), releaser);
    let snap_handle = snapshot.start(pipeline.snapshot_context()).unwrap();

    let mut current_id = 0u64;
    group.bench_function("write", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 1_000_000;
            let [meta, entry] = make_deposit_entries(current_id, account_id, 100);
            ring_push(&mut writer, meta);
            ring_push(&mut writer, entry);
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

criterion_group!(benches, wal_bench);
criterion_main!(benches);
