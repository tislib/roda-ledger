use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::transaction::{Operation, Transaction};
use roda_ledger::transactor::TransactorRunner;
use roda_ledger::wasm_runtime::WasmRuntime;
use std::sync::Arc;
use std::time::Duration;

const BATCH_SIZE: u64 = 1_000;

fn transaction_runner_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_runner");
    group.measurement_time(Duration::from_secs(10));

    // ADR-014: every TransactorRunner takes a WasmRuntime. The built-in bench
    // never registers anything, so an empty runtime is enough.
    let runtime = Arc::new(WasmRuntime::new());
    let mut runner = TransactorRunner::new(10_000_000, runtime);
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
            runner.process_direct(tx);
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
            runner.process_direct_batch(batch);
        });
    });

    group.finish();
}

criterion_group!(benches, transaction_runner_bench);
criterion_main!(benches);
