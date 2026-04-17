//! ADR-014 micro-benchmark: drives `Operation::Named` through `TransactorRunner`
//! using a tiny WAT function compiled by `WasmRuntime`. Mirrors the shape of
//! [`transaction_runner_bench`] so the two can be compared side-by-side.
//!
//! The benchmark is intentionally minimal: it loads ONE function once, then
//! submits randomized `Named` operations against it. The function performs a
//! credit + debit of `param0`/`param2` so the entry path exercised is the
//! same shape as `Operation::Transfer` (one credit + one debit per tx).

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::transaction::{Operation, Transaction};
use roda_ledger::transactor::TransactorRunner;
use roda_ledger::wasm_runtime::WasmRuntime;
use std::sync::Arc;
use std::time::Duration;

const BATCH_SIZE: u64 = 1_000;
const TRANSFER_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        local.get 0 local.get 1 call $credit
        local.get 2 local.get 1 call $debit
        i32.const 0))
"#;

fn transaction_runner_bench_wasm(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_runner_wasm");
    group.measurement_time(Duration::from_secs(10));

    let runtime = Arc::new(WasmRuntime::new());
    let binary = wat::parse_str(TRANSFER_WAT).expect("transfer wat");
    let crc = crc32c::crc32c(&binary);
    runtime
        .load_function("wasm_transfer", &binary, crc)
        .expect("load wasm_transfer");

    let mut runner = TransactorRunner::new(10_000_000, runtime.clone());
    let mut current_id = 0u64;

    group.throughput(Throughput::Elements(1));
    group.bench_function("process_direct", |b| {
        b.iter(|| {
            current_id += 1;
            let src = rand::random::<u64>() % 10_000_000;
            let dst = rand::random::<u64>() % 10_000_000;
            let mut tx = Transaction::new(Operation::Named {
                name: "wasm_transfer".into(),
                params: [src as i64, 100, dst as i64, 0, 0, 0, 0, 0],
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
                    let src = rand::random::<u64>() % 10_000_000;
                    let dst = rand::random::<u64>() % 10_000_000;
                    let mut tx = Transaction::new(Operation::Named {
                        name: "wasm_transfer".into(),
                        params: [src as i64, 100, dst as i64, 0, 0, 0, 0, 0],
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

criterion_group!(benches, transaction_runner_bench_wasm);
criterion_main!(benches);
