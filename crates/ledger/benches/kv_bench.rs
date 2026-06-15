//! Programmable KV state micro-benchmark (ADR-023).
//!
//! For every host verb, a tiny WAT module runs the op in a counted loop, driven
//! through `Runner::process_direct` (transactor-level — no WAL durability wait,
//! so this isolates host-call + store cost). Two variants per op:
//!   - `/1`    — one op per WASM invocation (per-call overhead floor)
//!   - `/1000` — 1000 ops per invocation (amortized steady-state throughput)
//!
//! `Throughput::Elements(count)` makes Criterion report the figure as op/s.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::ledger::WaitStrategy;
use ledger::pipeline::TransactorContext;
use ledger::test_support::mock_pipeline;
use ledger::transactor::runner::Runner;
use ledger::transactor::transaction::{Operation, Transaction};
use ledger::transactor::wasm_runtime::WasmRuntime;
use std::sync::Arc;
use std::time::Duration;
use storage::{Storage, StorageConfig};

/// Wrap `body` in `for i in 0..param0 { body }` — `$i` is the loop counter.
fn loop_wat(imports: &str, body: &str) -> String {
    format!(
        r#"
    (module
      {imports}
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        (local $i i64)
        (block $done
          (loop $lp
            (br_if $done (i64.ge_u (local.get $i) (local.get 0)))
            {body}
            (local.set $i (i64.add (local.get $i) (i64.const 1)))
            (br $lp)))
        (i32.const 0)))
    "#
    )
}

/// (op_name, import_decl, loop_body). Mutating ops use distinct keys per
/// iteration; counters/reads reuse keys the warm-up populated.
fn ops() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "kv_set",
            r#"(import "ledger" "kv_set" (func $f (param i32 i32 i32 i32 i64)))"#,
            r#"(call $f (i32.wrap_i64 (local.get $i)) (i32.const 0) (i32.const 0) (i32.const 0) (i64.add (local.get $i) (i64.const 1)))"#,
        ),
        (
            "kv_get",
            r#"(import "ledger" "kv_get" (func $f (param i32 i32 i32 i32) (result i64)))"#,
            r#"(drop (call $f (i32.wrap_i64 (local.get $i)) (i32.const 0) (i32.const 0) (i32.const 0)))"#,
        ),
        (
            "kv_add",
            r#"(import "ledger" "kv_add" (func $f (param i64 i32 i32 i32 i32 i64) (result i64)))"#,
            r#"(drop (call $f (i64.const 0) (i32.const 1) (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 1)))"#,
        ),
        (
            "kv_set_scoped",
            r#"(import "ledger" "kv_set_scoped" (func $f (param i64 i32 i32 i32 i32 i64)))"#,
            r#"(call $f (i64.const 5) (i32.wrap_i64 (local.get $i)) (i32.const 0) (i32.const 0) (i32.const 0) (i64.add (local.get $i) (i64.const 1)))"#,
        ),
        (
            "kv_get_scoped",
            r#"(import "ledger" "kv_get_scoped" (func $f (param i64 i32 i32 i32 i32) (result i64)))"#,
            r#"(drop (call $f (i64.const 5) (i32.wrap_i64 (local.get $i)) (i32.const 0) (i32.const 0) (i32.const 0)))"#,
        ),
        (
            "tree_set",
            r#"(import "ledger" "tree_set" (func $f (param i64 i32 i32 i32 i32 i64)))"#,
            r#"(call $f (i64.const 0) (i32.const 1) (i32.const 0) (i32.const 0) (i32.wrap_i64 (local.get $i)) (i64.add (local.get $i) (i64.const 1)))"#,
        ),
        (
            "tree_get",
            r#"(import "ledger" "tree_get" (func $f (param i64 i32 i32 i32 i32) (result i64)))"#,
            r#"(drop (call $f (i64.const 0) (i32.const 1) (i32.const 0) (i32.const 0) (i32.wrap_i64 (local.get $i))))"#,
        ),
        (
            "register_write",
            r#"(import "ledger" "register_write" (func $f (param i32 i64)))"#,
            r#"(call $f (i32.wrap_i64 (local.get $i)) (i64.add (local.get $i) (i64.const 1)))"#,
        ),
        (
            "register_read",
            r#"(import "ledger" "register_read" (func $f (param i32) (result i64)))"#,
            r#"(drop (call $f (i32.wrap_i64 (local.get $i))))"#,
        ),
    ]
}

fn invoke(runner: &mut Runner, ctx: &TransactorContext, id: &mut u64, name: &str, count: u64) {
    *id += 1;
    let mut tx = Transaction::new(Operation::Function {
        name: name.to_string(),
        params: [count as i64, 0, 0, 0, 0, 0, 0, 0],
        user_ref: 0,
    });
    tx.id = *id;
    runner.process_direct(ctx, tx);
}

fn kv_bench(c: &mut Criterion) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        Storage::new(StorageConfig {
            data_dir: tmp.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        })
        .expect("storage"),
    );
    let runtime = Arc::new(WasmRuntime::new(storage));

    let ops = ops();
    for (name, imports, body) in &ops {
        let binary = wat::parse_str(loop_wat(imports, body)).expect("wat parse");
        runtime.register(name, &binary, false).expect("register");
    }

    // Large ring + background drain so a 1000-op tx never exhausts its grant.
    let (pipeline, writer, _drain) = mock_pipeline(1024, 1 << 18, WaitStrategy::Balanced);
    let ctx = pipeline.transactor_context();
    let mut runner = Runner::new(1 << 16, runtime.clone(), writer);
    let mut id = 0u64;

    // Populate the stores so the read benchmarks hit instead of missing.
    for w in ["kv_set", "kv_set_scoped", "tree_set", "register_write"] {
        invoke(&mut runner, &ctx, &mut id, w, 1000);
    }

    let mut group = c.benchmark_group("kv_ops");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);

    for (name, _, _) in &ops {
        for &count in &[1u64, 1000u64] {
            group.throughput(Throughput::Elements(count));
            group.bench_function(format!("{name}/{count}"), |b| {
                b.iter(|| invoke(&mut runner, &ctx, &mut id, name, count));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, kv_bench);
criterion_main!(benches);
