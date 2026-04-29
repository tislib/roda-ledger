use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::config::{LedgerConfig, StorageConfig};
use storage::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};
use ledger::ledger::WaitStrategy;
use ledger::pipeline::Pipeline;
use ledger::snapshot::{Snapshot, SnapshotMessage};
use storage::Storage;
use ledger::wasm_runtime::WasmRuntime;
use std::hint::spin_loop;
use std::sync::Arc;
use std::time::Duration;

fn make_snapshot_messages(tx_id: u64, account_id: u64, amount: u64) -> [SnapshotMessage; 2] {
    let metadata = TxMetadata {
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
        account_id,
        amount,
        computed_balance: amount as i64,
    };
    [
        SnapshotMessage::Entry(WalEntry::Metadata(metadata)),
        SnapshotMessage::Entry(WalEntry::Entry(entry)),
    ]
}

fn snapshot_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let config = LedgerConfig {
        max_accounts: 1_000_000,
        ..LedgerConfig::default()
    };
    let pipeline = Pipeline::with_sizes(10_240_000, 10_240_000, WaitStrategy::Balanced);

    // The Snapshot stage needs an Arc<WasmRuntime> + Arc<Storage> to
    // load/unload WASM handlers on FunctionRegistered commits. This
    // bench never emits such records; a fresh storage in a tempdir and
    // an empty WasmRuntime suffice.
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let storage_cfg = StorageConfig {
        data_dir: tmp_dir.path().to_string_lossy().into_owned(),
        temporary: true,
        ..StorageConfig::default()
    };
    let storage = Arc::new(Storage::new(storage_cfg).unwrap());
    let wasm_runtime = Arc::new(WasmRuntime::new());

    let mut snapshot = Snapshot::new(&config, wasm_runtime, storage);
    let handle = snapshot.start(pipeline.snapshot_context()).unwrap();

    let snapshot_ctx = pipeline.snapshot_context();
    let mut current_id = 0u64;

    group.bench_function("process", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 1_000_000;
            let messages = make_snapshot_messages(current_id, account_id, 100);
            for msg in messages {
                let mut m = msg;
                while let Err(returned) = snapshot_ctx.input().push(m) {
                    m = returned;
                    spin_loop();
                }
            }
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
}

criterion_group!(benches, snapshot_bench);
criterion_main!(benches);
