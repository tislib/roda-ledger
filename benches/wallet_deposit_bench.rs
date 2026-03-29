use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;
use std::fs;

fn wallet_deposit_bench(c: &mut Criterion) {
    let _ = fs::remove_dir_all("data");
    let batch_size = 100_000;
    let operation_count = 2;

    let mut group = c.benchmark_group("ledger_deposit");
    group.throughput(Throughput::Elements(operation_count * batch_size as u64));

    group.bench_function("deposit", |b| {
        let mut ledger = Ledger::new(LedgerConfig {
            queue_size: 1024,
            temporary: true,
            ..Default::default()
        });
        ledger.start();

        b.iter(|| {
            for _ in 0..batch_size {
                ledger.submit(Operation::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 0,
                });
                ledger.submit(Operation::Withdrawal {
                    account: 1,
                    amount: 100,
                    user_ref: 0,
                });
            }
        });
    });

    group.finish();
}

criterion_group!(benches, wallet_deposit_bench);
criterion_main!(benches);
