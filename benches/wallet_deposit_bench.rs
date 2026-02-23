use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::wallet::Wallet;
use std::fs;

fn wallet_deposit_bench(c: &mut Criterion) {
    let _ = fs::remove_dir_all("data");
    let batch_size = 100_000;
    let operation_count = 2;

    let mut group = c.benchmark_group("wallet_deposit");
    group.throughput(Throughput::Elements(operation_count * batch_size as u64));

    let mut wallet = Wallet::new(1024);
    wallet.start();

    group.bench_function("deposit", |b| {
        b.iter(|| {
            for _ in 0..batch_size {
                wallet.deposit(1, 100);
                wallet.withdraw(1, 100);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, wallet_deposit_bench);
criterion_main!(benches);
