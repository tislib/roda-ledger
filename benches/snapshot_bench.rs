use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry};
use roda_ledger::ledger::{LedgerConfig, WaitStrategy};
use roda_ledger::pipeline::Pipeline;
use roda_ledger::snapshot::{Snapshot, SnapshotMessage};

fn snapshot_bench(c: &mut Criterion) {
    let batch_size = 10_000;

    let mut group = c.benchmark_group("snapshot");
    group.throughput(Throughput::Elements(batch_size));
    group.measurement_time(std::time::Duration::from_secs(10));

    // The snapshot stage only consumes from wal_to_snapshot, so the
    // transactor→wal queue can be small. We size wal_to_snapshot via the
    // small-queue parameter.
    let pipeline = Pipeline::with_sizes(batch_size as usize * 10, 1, WaitStrategy::LowLatency);
    let config = LedgerConfig {
        max_accounts: 1_000_000,
        index_circle1_size: 1 << 20,
        index_circle2_size: 1 << 21,
        ..LedgerConfig::default()
    };
    let mut snapshot = Snapshot::new(&config);

    let snapshot_ctx = pipeline.snapshot_context();
    let push_ctx = pipeline.snapshot_context();
    let progress_ctx = pipeline.snapshot_context();

    let handle = snapshot.start(snapshot_ctx).unwrap();
    let mut current_id = 0;

    group.bench_function("process", |b| {
        b.iter(|| {
            let start_id = current_id;
            for _ in 0..batch_size {
                current_id += 1;
                let mut metadata = TxMetadata {
                    entry_type: 0,
                    tx_id: current_id,
                    timestamp: 0,
                    user_ref: 0,
                    entry_count: 2,
                    link_count: 0,
                    fail_reason: FailReason::NONE,
                    crc32c: 0,
                    tag: [0; 8],
                };
                metadata.crc32c = crc32c::crc32c(bytemuck::bytes_of(&metadata));
                while push_ctx
                    .input()
                    .push(SnapshotMessage::Entry(WalEntry::Metadata(metadata)))
                    .is_err()
                {
                    std::thread::yield_now();
                }

                for account_id in 0..2 {
                    let entry = TxEntry {
                        entry_type: 1,
                        tx_id: current_id,
                        account_id: account_id as u64,
                        amount: 100,
                        kind: EntryKind::Credit,
                        _pad0: [0; 6],
                        computed_balance: 100,
                    };
                    while push_ctx
                        .input()
                        .push(SnapshotMessage::Entry(WalEntry::Entry(entry)))
                        .is_err()
                    {
                        std::thread::yield_now();
                    }
                }
            }
            // Wait for all to be processed
            while progress_ctx.get_processed_index() < start_id + batch_size {
                std::thread::yield_now();
            }
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
}

criterion_group!(benches, snapshot_bench);
criterion_main!(benches);
