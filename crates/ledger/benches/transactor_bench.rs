use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::ledger::{LedgerConfig, WaitStrategy};
use ledger::test_support::mock_pipeline;
use ledger::transaction::{Operation, Transaction, TransactionInput};
use ledger::transactor::Transactor;
use ledger::wasm_runtime::WasmRuntime;
use std::hint::spin_loop;
use std::sync::Arc;
use std::time::Duration;
use storage::{Storage, StorageConfig};

fn transactor_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactor");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    // Standalone pipeline + ring writer + a background drain that keeps the ring
    // empty so the transactor never blocks on a full ring during the bench.
    let (pipeline, writer, _drain) = mock_pipeline(1 << 20, 1 << 16, WaitStrategy::Balanced);

    let config = LedgerConfig {
        max_accounts: 10_000_000,
        ..LedgerConfig::default()
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        Storage::new(StorageConfig {
            data_dir: tmp.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        })
        .expect("storage"),
    );
    let runtime = Arc::new(WasmRuntime::new(storage));
    let mut transactor = Transactor::new(&config, runtime, writer);

    let handle = transactor.start(pipeline.transactor_context()).unwrap();

    let push_ctx = pipeline.transactor_context();
    let mut current_id = 0;

    group.bench_function("process", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 10_000_000;
            let mut tx = Transaction::new(Operation::Deposit {
                account: account_id,
                amount: 100,
                user_ref: 0,
            });
            tx.id = current_id;
            while let Err(returned_tx) = push_ctx.input().push(TransactionInput::Single(tx)) {
                tx = returned_tx.single();
                spin_loop();
            }
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
    // `_drain` is dropped here, stopping the background ring drain.
}

criterion_group!(benches, transactor_bench);
criterion_main!(benches);
