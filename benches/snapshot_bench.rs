use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::ArrayQueue;
use roda_ledger::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry};
use roda_ledger::ledger::PipelineMode;
use roda_ledger::snapshot::{Snapshot, SnapshotMessage};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn snapshot_bench(c: &mut Criterion) {
    let batch_size = 10_000;

    let mut group = c.benchmark_group("snapshot");
    group.throughput(Throughput::Elements(batch_size));
    group.measurement_time(std::time::Duration::from_secs(10));

    let inbound: Arc<ArrayQueue<SnapshotMessage>> =
        Arc::new(ArrayQueue::new(batch_size as usize * 10));
    let running = Arc::new(AtomicBool::new(true));
    let mut snapshot = Snapshot::new(
        inbound.clone(),
        1_000_000,
        running.clone(),
        PipelineMode::LowLatency,
        1 << 20,
        1 << 21,
    );

    let handle = snapshot.start().unwrap();
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
                    fail_reason: FailReason::NONE,
                    flags: 0,
                    crc32c: 0,
                    tag: [0; 8],
                };
                // compute CRC for correctness
                metadata.crc32c = crc32c::crc32c(bytemuck::bytes_of(&metadata));
                while inbound
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
                    while inbound
                        .push(SnapshotMessage::Entry(WalEntry::Entry(entry)))
                        .is_err()
                    {
                        std::thread::yield_now();
                    }
                }
            }
            // Wait for all to be processed
            while snapshot.last_processed_transaction_id() < start_id + batch_size {
                std::thread::yield_now();
            }
        });
    });

    group.finish();
    running.store(false, Ordering::Relaxed);
    let _ = handle.join();
}

criterion_group!(benches, snapshot_bench);
criterion_main!(benches);
