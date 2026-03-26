use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::client::Client;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::server::{Server, ServerConfig};
use roda_ledger::wallet::transaction::WalletTransaction;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

fn wallet_server_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let addr = "127.0.0.1:8083".to_string();

    // Start server
    let server_config = ServerConfig {
        addr: addr.clone(),
        worker_threads: 1,
        ledger_config: LedgerConfig {
            in_memory: true,
            queue_size: 1000000,
            ..Default::default()
        },
    };
    let server = Server::<WalletTransaction>::new(server_config);
    rt.spawn(async move {
        if let Err(e) = server.run_async().await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    std::thread::sleep(Duration::from_millis(500));

    let mut group = c.benchmark_group("wallet_server");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));
    let i = Arc::new(AtomicU64::new(0));

    group.bench_function("deposit", |b| {
        let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
            addr.clone(),
        )));
        let i = i.clone();
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let i = i.clone();
            async move {
                let idx = i.fetch_add(1, Ordering::Relaxed);
                client
                    .lock()
                    .await
                    .register_transaction(WalletTransaction::deposit(idx % 1000, 100))
                    .await
                    .unwrap();
            }
        });
    });

    group.bench_function("transfer", |b| {
        let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
            addr.clone(),
        )));

        // Pre-fill some balances
        {
            let client = client.clone();
            rt.block_on(async move {
                let mut client = client.lock().await;
                for i in 0..1000 {
                    client
                        .register_transaction(WalletTransaction::deposit(i, 10000))
                        .await
                        .unwrap();
                }
            });
        }

        let i = i.clone();
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let i = i.clone();
            async move {
                let idx = i.fetch_add(1, Ordering::Relaxed);
                client
                    .lock()
                    .await
                    .register_transaction(WalletTransaction::transfer(
                        idx % 1000,
                        (idx + 1) % 1000,
                        10,
                    ))
                    .await
                    .unwrap();
            }
        });
    });

    group.bench_function("get_balance", |b| {
        let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
            addr.clone(),
        )));
        let i = i.clone();
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let i = i.clone();
            async move {
                let idx = i.fetch_add(1, Ordering::Relaxed);
                client.lock().await.get_balance(idx % 1000).await.unwrap();
            }
        });
    });

    group.finish();

    for batch_size in [10, 100] {
        let mut batch_group = c.benchmark_group("wallet_server_batch");
        batch_group.throughput(Throughput::Elements(batch_size));
        batch_group.measurement_time(Duration::from_secs(10));

        batch_group.bench_function(format!("deposit_{}", batch_size), |b| {
            let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
                addr.clone(),
            )));
            let i = i.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let i = i.clone();
                async move {
                    let mut txs = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let idx = i.fetch_add(1, Ordering::Relaxed);
                        txs.push(WalletTransaction::deposit(idx % batch_size, 100));
                    }
                    client
                        .lock()
                        .await
                        .register_transactions_batch(txs)
                        .await
                        .unwrap();
                }
            });
        });

        batch_group.bench_function(format!("transfer_{}", batch_size), |b| {
            let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
                addr.clone(),
            )));
            let i = i.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let i = i.clone();
                async move {
                    let mut txs = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let idx = i.fetch_add(1, Ordering::Relaxed);
                        txs.push(WalletTransaction::transfer(
                            idx % batch_size,
                            (idx + 1) % batch_size,
                            10,
                        ));
                    }
                    client
                        .lock()
                        .await
                        .register_transactions_batch(txs)
                        .await
                        .unwrap();
                }
            });
        });

        batch_group.bench_function(format!("get_balance_{}", batch_size), |b| {
            let client = Arc::new(Mutex::new(Client::<WalletTransaction>::new(
                addr.clone(),
            )));
            let i = i.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let i = i.clone();
                async move {
                    let mut ids = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let idx = i.fetch_add(1, Ordering::Relaxed);
                        ids.push(idx % batch_size);
                    }
                    client.lock().await.get_balances_batch(ids).await.unwrap();
                }
            });
        });

        batch_group.finish();
    }
}

criterion_group!(benches, wallet_server_bench);
criterion_main!(benches);
