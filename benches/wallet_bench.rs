use criterion::{Criterion, Throughput, criterion_group, criterion_main};
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

    for account_count in [1000, 1_000_000, 10_000_000, 50_000_000] {
        let mut wallet = Wallet::new_with_config(WalletConfig {
            in_memory: false,
            queue_size: 1024,
            ..Default::default()
        });
        wallet.start();

        // Pre-fill some balances
        for i in 0..account_count {
            wallet.deposit(i, 10000);
        }

        group.bench_function(format!("transfer_{}", account_count), |b| {
            wallet.wait_pending_operations();

            b.iter(|| {
                i += 1;
                let from_account = i % account_count;
                let to_account = (i + account_count / 2) % account_count;
                wallet.transfer(from_account, to_account, 10);
            });
        });
        wallet.destroy();
    }

    group.finish();
}

criterion_group!(benches, wallet_bench);
criterion_main!(benches);
