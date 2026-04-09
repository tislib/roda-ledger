use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::ledger::WaitStrategy;
use roda_ledger::pipeline::Pipeline;
use roda_ledger::storage::{Storage, StorageConfig};
use roda_ledger::wal::Wal;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

fn wal_bench(c: &mut Criterion) {
    let batch_size = 10_000;

    let path = "bench_wal_data".to_string();
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();

    let mut group = c.benchmark_group("wal".to_string());
    group.throughput(Throughput::Elements(batch_size as u64));
    group.measurement_time(Duration::from_secs(10));

    let pipeline = Pipeline::with_sizes(
        batch_size as usize * 10,
        batch_size as usize * 10,
        WaitStrategy::LowLatency,
    );

    let storage = Arc::new(
        Storage::new(StorageConfig {
            wal_segment_size_mb: 100,
            ..Default::default()
        })
        .unwrap(),
    );
    let wal = Wal::new(storage);

    let handle = wal.start(pipeline.wal_context()).unwrap();
    let push_ctx = pipeline.wal_context();
    let drain_ctx = pipeline.wal_context();
    let mut current_id = 0;

    group.bench_function("append", |b| {
        b.iter(|| {
            for _ in 0..batch_size {
                current_id += 1;
                let metadata = roda_ledger::entities::TxMetadata {
                    entry_type: 0,
                    tx_id: current_id,
                    timestamp: 0,
                    user_ref: 0,
                    entry_count: 0,
                    link_count: 0,
                    fail_reason: roda_ledger::entities::FailReason::NONE,
                    crc32c: 0,
                    tag: [0; 8],
                };
                while push_ctx
                    .input()
                    .push(roda_ledger::entities::WalEntry::Metadata(metadata))
                    .is_err()
                {
                    std::thread::yield_now();
                }
            }
            // Wait for all to be processed by checking outbound
            while drain_ctx.output().len() < batch_size as usize {
                std::thread::yield_now();
            }
            // Drain outbound for next iteration
            while drain_ctx.output().pop().is_some() {}
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
    let _ = fs::remove_dir_all(&path);
}

criterion_group!(benches, wal_bench);
criterion_main!(benches);
