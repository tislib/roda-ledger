use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::config::LedgerConfig;
use roda_ledger::entities::{EntryKind, FailReason, TxEntry, TxMetadata, WalEntry, WalEntryKind};
use roda_ledger::ledger::WaitStrategy;
use roda_ledger::pipeline::Pipeline;
use roda_ledger::storage::Storage;
use roda_ledger::wal::Wal;
use std::hint::spin_loop;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn make_deposit_entries(tx_id: u64, account_id: u64, amount: u64) -> [WalEntry; 2] {
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
    [WalEntry::Metadata(metadata), WalEntry::Entry(entry)]
}

fn wal_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let config = LedgerConfig::bench();
    let storage = Arc::new(Storage::new(config.storage.clone()).unwrap());
    let pipeline = Pipeline::with_sizes(10_240_000, 10_240_000, WaitStrategy::Balanced);

    let wal = Wal::new(storage);
    let handles = wal.start(pipeline.wal_context()).unwrap();

    // Drain the snapshot output queue
    let drain_ctx = pipeline.snapshot_context();
    let drain_handle = thread::spawn(move || {
        let mut retry_count = 0;
        while drain_ctx.is_running() || !drain_ctx.input().is_empty() {
            while drain_ctx.input().pop().is_some() {
                retry_count = 0;
            }
            retry_count += 1;
            drain_ctx.wait_strategy().retry(retry_count);
        }
    });

    let wal_ctx = pipeline.wal_context();
    let mut current_id = 0u64;

    group.bench_function("write", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 1_000_000;
            let entries = make_deposit_entries(current_id, account_id, 100);
            for entry in entries {
                let mut e = entry;
                while let Err(returned) = wal_ctx.input().push(e) {
                    e = returned;
                    spin_loop();
                }
            }
        });
    });

    group.finish();
    pipeline.shutdown();
    for h in handles {
        let _ = h.join();
    }
    let _ = drain_handle.join();
}

criterion_group!(benches, wal_bench);
criterion_main!(benches);
