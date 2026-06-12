use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::ledger::WaitStrategy;
use ledger::test_support::mock_pipeline;
use ledger::transactor::runner::Runner;
use ledger::transactor::transaction::{Operation, Transaction};
use ledger::transactor::wasm_runtime::WasmRuntime;
use std::sync::Arc;
use std::time::Duration;
use storage::{Storage, StorageConfig};

const BATCH_SIZE: u64 = 1_000;

fn transaction_runner_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_runner");
    group.measurement_time(Duration::from_secs(10));

    // Every TransactorRunner takes a WasmRuntime. The built-in bench
    // never registers anything, so an empty runtime backed by a temp
    // storage is enough.
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        Storage::new(StorageConfig {
            data_dir: tmp.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        })
        .expect("storage"),
    );
    let runtime = Arc::new(WasmRuntime::new(storage));

    // Ring writer + context + background drain so process_direct never blocks.
    let (pipeline, writer, _drain) = mock_pipeline(1024, 1 << 16, WaitStrategy::Balanced);
    let ctx = pipeline.transactor_context();
    let mut runner = Runner::new(10_000_000, runtime, writer);
    let mut current_id = 0u64;

    group.throughput(Throughput::Elements(1));
    group.bench_function("process_direct", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 10_000_000;
            let mut tx = Transaction::new(Operation::Deposit {
                account: account_id,
                amount: 100,
                user_ref: 0,
            });
            tx.id = current_id;
            runner.process_direct(&ctx, tx);
        });
    });

    group.throughput(Throughput::Elements(BATCH_SIZE));
    group.bench_function("process_direct_batch", |b| {
        b.iter(|| {
            let batch: Vec<Transaction> = (0..BATCH_SIZE)
                .map(|_| {
                    current_id += 1;
                    let account_id = rand::random::<u64>() % 10_000_000;
                    let mut tx = Transaction::new(Operation::Deposit {
                        account: account_id,
                        amount: 100,
                        user_ref: 0,
                    });
                    tx.id = current_id;
                    tx
                })
                .collect();
            runner.process_direct_batch(&ctx, batch);
        });
    });

    group.finish();
}

criterion_group!(benches, transaction_runner_bench);
criterion_main!(benches);
