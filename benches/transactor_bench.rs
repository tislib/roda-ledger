use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::ledger::{LedgerConfig, WaitStrategy};
use roda_ledger::pipeline::Pipeline;
use roda_ledger::transaction::{Operation, Transaction};
use roda_ledger::transactor::Transactor;
use std::hint::spin_loop;
use std::thread;
use std::time::Duration;

fn transactor_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactor");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let pipeline = Pipeline::with_sizes(10_240_000, 10_240_000, WaitStrategy::Balanced);

    let config = LedgerConfig {
        max_accounts: 10_000_000,
        dedup_enabled: true,
        dedup_window_ms: 10_000,
        ..LedgerConfig::default()
    };
    let mut transactor = Transactor::new(&config);

    let handle = transactor.start(pipeline.transactor_context()).unwrap();

    let drain_ctx = pipeline.transactor_context();
    let drain_handle = thread::spawn(move || {
        let mut retry_count = 0;
        while drain_ctx.is_running() || !drain_ctx.output().is_empty() {
            while drain_ctx.output().pop().is_some() {
                retry_count = 0;
            }
            retry_count += 1;
            drain_ctx.wait_strategy().retry(retry_count);
        }
    });

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
            while let Err(returned_tx) = push_ctx.input().push(tx) {
                tx = returned_tx;
                spin_loop();
            }
        });
    });

    group.finish();
    pipeline.shutdown();
    let _ = handle.join();
    let _ = drain_handle.join();
}

criterion_group!(benches, transactor_bench);
criterion_main!(benches);
