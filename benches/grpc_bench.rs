#[cfg(feature = "grpc")]
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "grpc")]
use roda_ledger::grpc::GrpcServer;
#[cfg(feature = "grpc")]
use roda_ledger::grpc::proto::ledger_client::LedgerClient;
#[cfg(feature = "grpc")]
use roda_ledger::grpc::proto::{
    Deposit, SubmitAndWaitRequest, SubmitBatchAndWaitRequest, SubmitBatchRequest,
    SubmitOperationRequest, WaitLevel,
};
#[cfg(feature = "grpc")]
use roda_ledger::ledger::{Ledger, LedgerConfig};
#[cfg(feature = "grpc")]
use std::net::SocketAddr;
#[cfg(feature = "grpc")]
use std::sync::Arc;
#[cfg(feature = "grpc")]
use tokio::runtime::Runtime;

#[cfg(feature = "grpc")]
async fn setup_grpc_server() -> (Arc<Ledger>, SocketAddr) {
    let mut ledger = Ledger::new(LedgerConfig::bench());
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server_ledger = ledger.clone();
    tokio::spawn(async move {
        let server = GrpcServer::new(server_ledger, addr);
        server.run().await.unwrap();
    });

    // Wait for the server to be ready
    let mut client_ready = false;
    for _ in 0..100 {
        if LedgerClient::connect(format!("http://{}", addr))
            .await
            .is_ok()
        {
            client_ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    if !client_ready {
        panic!("gRPC server failed to start");
    }

    (ledger, addr)
}

#[cfg(feature = "grpc")]
fn bench_grpc_submit_operation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("grpc_submit_operation");
    group.bench_function("unary_deposit", |b| {
        let (_ledger, addr) = rt.block_on(setup_grpc_server());
        let client = rt.block_on(async {
            LedgerClient::connect(format!("http://{}", addr))
                .await
                .unwrap()
        });

        let client = client.clone();
        b.to_async(&rt).iter(|| {
            let mut client = client.clone();
            async move {
                let request = SubmitOperationRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 0,
                            },
                        ),
                    ),
                };
                client.submit_operation(request).await.unwrap();
            }
        });
    });
    group.finish();
}

#[cfg(feature = "grpc")]
fn bench_grpc_submit_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("grpc_submit_batch");
    for size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let (_ledger, addr) = rt.block_on(setup_grpc_server());
            let client = rt.block_on(async {
                LedgerClient::connect(format!("http://{}", addr))
                    .await
                    .unwrap()
            });
            let operations = (0..size)
                .map(|_| SubmitOperationRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_operation_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 0,
                            },
                        ),
                    ),
                })
                .collect::<Vec<_>>();

            let client = client.clone();
            b.to_async(&rt).iter(|| {
                let request = SubmitBatchRequest {
                    operations: operations.clone(),
                };
                let mut client = client.clone();
                async move {
                    client.submit_batch(request).await.unwrap();
                }
            });
        });
    }
    group.finish();
}

#[cfg(feature = "grpc")]
fn bench_grpc_submit_and_wait(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("grpc_submit_and_wait");

    for (name, level) in [
        ("processed", WaitLevel::Processed),
        ("committed", WaitLevel::Committed),
        ("snapshotted", WaitLevel::Snapshot),
    ] {
        group.bench_function(name, |b| {
            let (_ledger, addr) = rt.block_on(setup_grpc_server());
            let client = rt.block_on(async {
                LedgerClient::connect(format!("http://{}", addr))
                    .await
                    .unwrap()
            });

            let client = client.clone();
            b.to_async(&rt).iter(|| {
                let mut client = client.clone();
                async move {
                    let request = SubmitAndWaitRequest {
                        operation: Some(
                            roda_ledger::grpc::proto::submit_and_wait_request::Operation::Deposit(
                                Deposit {
                                    account: 1,
                                    amount: 100,
                                    user_ref: 0,
                                },
                            ),
                        ),
                        wait_level: level as i32,
                    };
                    client.submit_and_wait(request).await.unwrap();
                }
            });
        });
    }
    group.finish();
}

#[cfg(feature = "grpc")]
fn bench_grpc_submit_batch_and_wait(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("grpc_submit_batch_and_wait");
    for size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let (_ledger, addr) = rt.block_on(setup_grpc_server());
            let client = rt.block_on(async {
                LedgerClient::connect(format!("http://{}", addr))
                    .await
                    .unwrap()
            });

            let operations: Vec<_> = (0..size)
                .map(|_| SubmitAndWaitRequest {
                    operation: Some(
                        roda_ledger::grpc::proto::submit_and_wait_request::Operation::Deposit(
                            Deposit {
                                account: 1,
                                amount: 100,
                                user_ref: 0,
                            },
                        ),
                    ),
                    wait_level: 0,
                })
                .collect();

            let client = client.clone();
            b.to_async(&rt).iter(|| {
                let request = SubmitBatchAndWaitRequest {
                    operations: operations.clone(),
                    wait_level: WaitLevel::Committed as i32,
                };
                let mut client = client.clone();
                async move {
                    client.submit_batch_and_wait(request).await.unwrap();
                }
            });
        });
    }
    group.finish();
}

#[cfg(feature = "grpc")]
criterion_group!(
    benches,
    bench_grpc_submit_operation,
    bench_grpc_submit_batch,
    bench_grpc_submit_and_wait,
    bench_grpc_submit_batch_and_wait,
);

#[cfg(not(feature = "grpc"))]
fn no_bench(_c: &mut Criterion) {}

#[cfg(not(feature = "grpc"))]
criterion_group!(benches, no_bench);

criterion_main!(benches);
