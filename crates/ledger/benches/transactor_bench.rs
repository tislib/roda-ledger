use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::ledger::{LedgerConfig, WaitStrategy};
use ledger::recover::ActiveSnapshot;
use ledger::test_support::mock_pipeline;
use ledger::transactor;
use ledger::transactor::transaction::{Operation, Transaction, TransactionInput};
use ledger::transactor::wasm_runtime::WasmRuntime;
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
        initial_account_size: 10_000_000,
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
    let active_snapshot = ActiveSnapshot::empty();
    let handle = transactor::Transactor {
        ctx: pipeline.transactor_context(),
        active_snapshot: &active_snapshot,
        config: &config,
        wasm_runtime: runtime,
        ring_writer: writer,
    }
    .start()
    .unwrap();

    let push_ctx = pipeline.transactor_context();
    let mut current_id = 0;

    // Existence enforcement (ADR-022): open the accounts before depositing.
    current_id += 1;
    let mut open_tx = Transaction::new(Operation::OpenAccount {
        count: 10_000_000,
        user_ref: 0,
    });
    open_tx.id = current_id;
    while let Err(returned) = push_ctx.input().push(TransactionInput::Single(open_tx)) {
        open_tx = returned.single();
        spin_loop();
    }
    while push_ctx.get_processed_index() < current_id {
        spin_loop();
    }

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
