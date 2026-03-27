use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::random;
use roda_ledger::wallet::{Wallet, WalletConfig};
use std::time::Duration;

fn wallet_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wallet");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(60));
    let mut i = 0;

    group.bench_function("deposit", |b| {
        let mut wallet = Wallet::new_with_config(WalletConfig {
            in_memory: false,
            queue_size: 1024,
            ..Default::default()
        });
        wallet.start();

        b.iter(|| {
            i += 1;
            wallet.deposit(i % 1000, 100);
        });

        wallet.destroy();
    });

    group.bench_function("transfer", |b| {
        let mut wallet = Wallet::new_with_config(WalletConfig {
            in_memory: false,
            queue_size: 1024,
            ..Default::default()
        });
        wallet.start();

        // Pre-fill some balances
        for i in 0..1000 {
            wallet.deposit(i, 10000);
        }
        wallet.wait_pending_operations();

        b.iter(|| {
            i += 1;
            wallet.transfer(i % 1000, (i + 1) % 1000, 10);
        });

        wallet.destroy();
    });

    group.bench_function("transfer_mix", |b| {
        let mut wallet = Wallet::new_with_config(WalletConfig {
            in_memory: false,
            queue_size: 1024,
            ..Default::default()
        });
        wallet.start();

        wallet.wait_pending_operations();

        b.iter(|| {
            let from_account_id = random::<u64>() % 100_000_000;
            let to_account_id = random::<u64>() % 100_000_000;

            wallet.deposit(from_account_id, 10000);
            wallet.transfer(from_account_id, to_account_id, 10);
        });

        wallet.destroy();
    });

    group.finish();
}

criterion_group!(benches, wallet_bench);
criterion_main!(benches);
