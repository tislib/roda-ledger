//! WASM-overhead load generator.
//!
//! Identical to `src/bin/load.rs` in every respect — same account-count,
//! same 10_000-unit amount, same sampling cadence, same output format —
//! except that instead of submitting `Operation::Deposit`, it registers
//! a tiny WASM function `deposit_wasm(account, amount)` and submits
//! `Operation::Function { name: "deposit_wasm", ... }` per iteration.
//!
//! The WAT below performs exactly the same state transitions the
//! built-in `Operation::Deposit` arm performs in the Transactor
//! (`debit(account, amount)` + `credit(SYSTEM_ACCOUNT_ID=0, amount)`), so
//! any TPS / latency delta between the two binaries is pure WASM
//! overhead end-to-end (instantiate once, hot-path `TypedFunc::call`
//! per Named op, host→WASM→host round-trip for credit/debit).

use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{Operation, WaitLevel};
use spdlog::Level::Info;
use std::time::{Duration, Instant};

/// WASM implementation of `Operation::Deposit`.
///
/// Parameters:
/// - `param0` — target account id
/// - `param1` — amount
///
/// Host-call order mirrors the built-in Deposit arm:
/// `debit(account, amount)` then `credit(SYSTEM_ACCOUNT, amount)` (the
/// Transactor convention: `credit` *subtracts*, `debit` *adds*).
/// `SYSTEM_ACCOUNT_ID` is `0` per `entities.rs`.
const DEPOSIT_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        ;; debit(account, amount)
        local.get 0 local.get 1 call $debit
        ;; credit(SYSTEM_ACCOUNT_ID=0, amount)
        i64.const 0 local.get 1 call $credit
        i32.const 0))
"#;

#[derive(Parser, Debug)]
#[command(name = "load_wasm", about = "WASM-overhead load generator for roda-ledger")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    #[arg(short, long, default_value_t = false)]
    wait: bool,
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count;
    let wait_mode = args.wait;
    let mut ledger = Ledger::new(LedgerConfig {
        max_accounts: account_count as usize,
        log_level: Info,
        ..LedgerConfig::bench()
    });
    ledger.start().unwrap();

    // Register the WASM deposit once before the loop starts.
    let binary = wat::parse_str(DEPOSIT_WAT).expect("compile deposit wat");
    ledger
        .register_function("deposit_wasm", &binary, false)
        .expect("register_function deposit_wasm");

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut i = 0u64;

    // Per-second measurers + 1 global
    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    let mut last_tick = start_time;
    let mut last_committed = 0u64;
    let mut second = 0u32;

    // Table header
    println!();
    println!("  +-----+--------+------------+------------+----------+----------+------------+");
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} |",
        "#", "time", "TPS", "TPC", "P50", "P99", "in-flight"
    );
    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    loop {
        let account = 1 + rand::random::<u64>() % account_count;
        let op = Operation::Function {
            name: "deposit_wasm".into(),
            // [account, amount, 0, 0, 0, 0, 0, 0]
            params: [account as i64, 10000, 0, 0, 0, 0, 0, 0],
            user_ref: 0,
        };

        let sample = i.is_multiple_of(10000);

        if sample {
            let t0 = Instant::now();
            if wait_mode {
                ledger.submit_and_wait(op, WaitLevel::Committed);
            } else {
                ledger.submit(op);
            }
            let elapsed = t0.elapsed();
            let bucket = start_time.elapsed().as_secs() as usize;
            if bucket < per_second.len() {
                per_second[bucket].measure(elapsed);
            }
            global.measure(elapsed);
        } else {
            ledger.submit(op);
        }

        i += 1;

        if sample {
            // Print a row every second
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                second += 1;
                let wall = start_time.elapsed();
                let committed = ledger.last_commit_id();
                let delta = committed - last_committed;
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = delta as f64 / interval;
                let in_flight = i.saturating_sub(committed);

                let bucket = (second as usize).saturating_sub(1);
                let stats = if bucket < per_second.len() {
                    per_second[bucket].get_stats()
                } else {
                    global.get_stats()
                };

                println!(
                    "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} |",
                    second,
                    wall.as_secs(),
                    format!("{:.0}", tps),
                    committed,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    in_flight,
                );

                last_tick = now;
                last_committed = committed;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    // Final summary
    let elapsed = start_time.elapsed();
    let avg_tps = i as f64 / elapsed.as_secs_f64();
    let stats = global.get_stats();

    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║         WASM LOAD TEST SUMMARY               ║");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration      : {:>10.2}s                 ║",
        elapsed.as_secs_f64()
    );
    println!("  ║  Submitted     : {:>10}                  ║", i);
    println!("  ║  Avg TPS       : {:>10.0}                  ║", avg_tps);
    println!(
        "  ║  Mode          : {:>10}                  ║",
        if wait_mode { "wait" } else { "async" }
    );
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  P50  Latency  : {:>10}                  ║",
        fmt_ns(stats.p50)
    );
    println!(
        "  ║  P99  Latency  : {:>10}                  ║",
        fmt_ns(stats.p99)
    );
    println!(
        "  ║  P999 Latency  : {:>10}                  ║",
        fmt_ns(stats.p999)
    );
    println!(
        "  ║  Min  Latency  : {:>10}                  ║",
        fmt_ns(stats.min)
    );
    println!(
        "  ║  Max  Latency  : {:>10}                  ║",
        fmt_ns(stats.max)
    );
    println!(
        "  ║  Samples       : {:>10}                  ║",
        stats.count
    );
    println!("  ╚══════════════════════════════════════════════╝");
    println!();
}
