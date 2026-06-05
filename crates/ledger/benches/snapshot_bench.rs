use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::config::{LedgerConfig, StorageConfig};
use ledger::ledger::WaitStrategy;
use ledger::snapshot::Snapshot;
use ledger::test_support::{ring_pipeline, ring_push};
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
        tx_id,
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    [WalEntry::Metadata(metadata), WalEntry::Entry(entry)]
}

/// Isolates the snapshot stage: the bench feeds meta+entry pairs into the ring and
/// advances `commit_index` directly (standing in for the WAL), so the snapshot
/// stage indexes the entries and drives the ring releaser.
fn snapshot_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let config = LedgerConfig {
        max_accounts: 1_000_000,
        ..LedgerConfig::default()
    };

    // The Snapshot constructor needs an Arc<Storage>, but its run loop only touches
    // the in-memory indexer + releaser — a throwaway temp storage suffices.
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let storage_cfg = StorageConfig {
        data_dir: tmp_dir.path().to_string_lossy().into_owned(),
        temporary: true,
        ..StorageConfig::default()
    };
    let storage = Arc::new(Storage::new(storage_cfg).unwrap());

    let (pipeline, mut writer, releaser) = ring_pipeline(1 << 20, 1 << 16, WaitStrategy::Balanced);

    let mut snapshot = Snapshot::new(&config, storage, releaser);
    let handle = snapshot.start(pipeline.snapshot_context()).unwrap();

    // Stands in for the WAL: lets the snapshot's `tx_id > commit_index` gate pass.
    let wal_ctx = pipeline.wal_context();
    let mut current_id = 0u64;

    group.bench_function("process", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 1_000_000;
            let [meta, entry] = make_deposit_entries(current_id, account_id, 100);
            ring_push(&mut writer, meta);
            ring_push(&mut writer, entry);
            writer.commit();
            wal_ctx.set_commit_index(current_id);
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
}

criterion_group!(benches, snapshot_bench);
criterion_main!(benches);
