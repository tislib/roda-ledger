use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::ArrayQueue;
use roda_ledger::ledger::PipelineMode;
use roda_ledger::wal::Wal;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn wal_bench(c: &mut Criterion) {
    let batch_size = 10_000;

    for in_memory in [true, false] {
        let mode = if in_memory { "in_memory" } else { "on_disk" };
        let path = format!("bench_wal_data_{}", mode);
        if !in_memory {
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
        }

        let mut group = c.benchmark_group(format!("wal_{}", mode));
        group.throughput(Throughput::Elements(batch_size as u64));
        group.measurement_time(Duration::from_secs(10));

        let inbound = Arc::new(ArrayQueue::new(batch_size as usize * 10));
        let outbound = Arc::new(ArrayQueue::new(batch_size as usize * 10));
        let running = Arc::new(AtomicBool::new(true));

        let wal = Wal::new(
            inbound.clone(),
            outbound.clone(),
            if in_memory { None } else { Some(&path) },
            in_memory,
            running.clone(),
            PipelineMode::LowLatency,
        );

        let handle = wal.start();
        let mut current_id = 0;

        group.bench_function("append", |b| {
            b.iter(|| {
                for _ in 0..batch_size {
                    current_id += 1;
                    let metadata = roda_ledger::entities::TxMetadata {
                        tx_id: current_id,
                        timestamp: 0,
                        user_ref: 0,
                        entry_count: 0,
                        fail_reason: roda_ledger::entities::FailReason::NONE,
                        _pad: [0; 6],
                    };
                    while inbound
                        .push(roda_ledger::entities::WalEntry::Metadata(metadata))
                        .is_err()
                    {
                        std::thread::yield_now();
                    }
                }
                // Wait for all to be processed by checking outbound
                while outbound.len() < batch_size as usize {
                    std::thread::yield_now();
                }
                // Drain outbound for next iteration
                while outbound.pop().is_some() {}
            });
        });

        group.finish();
        running.store(false, Ordering::Relaxed);
        let _ = handle.join();
        if !in_memory {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

criterion_group!(benches, wal_bench);
criterion_main!(benches);
