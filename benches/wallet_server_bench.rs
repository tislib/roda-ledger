use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::client::Client;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::server::{Server, ServerConfig};
use roda_ledger::transaction::Operation;
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
            temporary: true,
            queue_size: 1000000,
            ..Default::default()
        },
    };
    let server = Server::new(server_config);
    rt.spawn(async move {
        if let Err(e) = server.run_async().await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    std::thread::sleep(Duration::from_millis(500));

    let mut group = c.benchmark_group("ledger_server");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));
    let i = Arc::new(AtomicU64::new(0));

    group.bench_function("deposit", |b| {
        let client = Arc::new(Mutex::new(Client::new(addr.clone())));
        let i = i.clone();
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let i = i.clone();
            async move {
                let idx = i.fetch_add(1, Ordering::Relaxed);
                client
                    .lock()
                    .await
                    .register_transaction(Operation::Deposit {
                        account: idx % 1000,
                        amount: 100,
                        user_ref: 0,
                    })
                    .await
                    .unwrap();
            }
        });
    });

    group.bench_function("transfer", |b| {
        let client = Arc::new(Mutex::new(Client::new(addr.clone())));

        // Pre-fill some balances
        {
            let client = client.clone();
            rt.block_on(async move {
                let mut client = client.lock().await;
                for i in 0..1000 {
                    client
                        .register_transaction(Operation::Deposit {
                            account: i,
                            amount: 10000,
                            user_ref: 0,
                        })
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
                    .register_transaction(Operation::Transfer {
                        from: idx % 1000,
                        to: (idx + 1) % 1000,
                        amount: 10,
                        user_ref: 0,
                    })
                    .await
                    .unwrap();
            }
        });
    });

    group.bench_function("get_balance", |b| {
        let client = Arc::new(Mutex::new(Client::new(addr.clone())));
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
        let mut batch_group = c.benchmark_group("ledger_server_batch");
        batch_group.throughput(Throughput::Elements(batch_size));
        batch_group.measurement_time(Duration::from_secs(10));

        batch_group.bench_function(format!("deposit_{}", batch_size), |b| {
            let client = Arc::new(Mutex::new(Client::new(addr.clone())));
            let i = i.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let i = i.clone();
                async move {
                    let mut txs = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let idx = i.fetch_add(1, Ordering::Relaxed);
                        txs.push(Operation::Deposit {
                            account: idx % batch_size,
                            amount: 100,
                            user_ref: 0,
                        });
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
            let client = Arc::new(Mutex::new(Client::new(addr.clone())));
            let i = i.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let i = i.clone();
                async move {
                    let mut txs = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let idx = i.fetch_add(1, Ordering::Relaxed);
                        txs.push(Operation::Transfer {
                            from: idx % batch_size,
                            to: (idx + 1) % batch_size,
                            amount: 10,
                            user_ref: 0,
                        });
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
            let client = Arc::new(Mutex::new(Client::new(addr.clone())));
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
