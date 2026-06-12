use clap::Parser;
use ledger::test_support::mock_pipeline;
use ledger::transactor::TransactorRunner;
use ledger::transactor::transaction::{Operation, Transaction};
use ledger::transactor::wasm_runtime::WasmRuntime;
use ledger::wait_strategy::WaitStrategy;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::{Storage, StorageConfig};

#[derive(Parser, Debug)]
#[command(
    name = "load_transactor_runner",
    about = "Load generator that drives TransactorRunner directly (no input queue / no thread)"
)]
struct Args {
    /// Distinct user accounts to spread deposits across.
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    /// How long to drive load, in seconds.
    #[arg(short, long, default_value_t = 10)]
    duration: u64,

    /// Transactions per `process_direct_batch` call. 1 uses `process_direct`.
    #[arg(short, long, default_value_t = 1)]
    batch_size: u64,

    /// Output tx_ring capacity; a background reader keeps it drained.
    #[arg(short, long, default_value_t = 1 << 16)]
    ring_size: usize,
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

fn make_tx(id: u64, account_count: u64) -> Transaction {
    let account = 1 + rand::random::<u64>() % account_count;
    let mut tx = Transaction::new(Operation::Deposit {
        account,
        amount: 10000,
        user_ref: 0,
    });
    tx.id = id;
    tx
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);
    let batch_size = args.batch_size.max(1);
    let batch_div = batch_size.min(u32::MAX as u64) as u32;

    // Self-cleaning temp storage (removed on drop). The deposit path never runs
    // WASM, but the runner still needs a runtime handle to construct.
    let mut data_dir = std::env::temp_dir();
    data_dir.push(format!("roda_load_runner_{}", rand::random::<u64>()));
    let storage = Arc::new(
        Storage::new(StorageConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            temporary: true,
            ..StorageConfig::default()
        })
        .expect("init storage"),
    );
    let runtime = Arc::new(WasmRuntime::new(storage));

    // Ring writer + context + a background drain that keeps the ring empty so
    // process_direct never blocks. The pipeline's queue is unused here.
    let (_pipeline, writer, _drain) = mock_pipeline(1024, args.ring_size, WaitStrategy::Balanced);
    let ctx = _pipeline.transactor_context();
    // +1 so SYSTEM_ACCOUNT_ID (0) and accounts 1..=account_count all fit.
    let mut runner = TransactorRunner::new(account_count as usize + 1, runtime, writer);

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut processed = 0u64;
    let mut current_id = 0u64;
    let mut unit = 0u64;
    // Sample roughly every 10k transactions regardless of batch size.
    let sample_every = (10_000 / batch_size).max(1);

    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    let mut last_tick = start_time;
    let mut last_processed = 0u64;
    let mut second = 0u32;

    println!();
    println!(
        "  runner load | accounts={} ring={} batch={} mode={}",
        account_count,
        args.ring_size,
        batch_size,
        if batch_size == 1 {
            "process_direct"
        } else {
            "process_direct_batch"
        },
    );

    // Table header (latency is per-transaction; amortized within a batch).
    println!();
    println!("  +-----+--------+------------+------------+----------+----------+");
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>8} | {:>8} |",
        "#", "time", "TPS", "processed", "P50", "P99"
    );
    println!("  +-----+--------+------------+------------+----------+----------+");

    // Existence enforcement (ADR-022): open accounts 1..=account_count first.
    current_id += 1;
    let mut open_tx = Transaction::new(Operation::OpenAccount {
        count: account_count as u32,
        user_ref: 0,
    });
    open_tx.id = current_id;
    runner.process_direct(&ctx, open_tx);

    loop {
        let sample = unit.is_multiple_of(sample_every);

        // Build this unit's work, then time only the processing call.
        let sampled: Option<Duration> = if batch_size == 1 {
            current_id += 1;
            let tx = make_tx(current_id, account_count);
            if sample {
                let t0 = Instant::now();
                runner.process_direct(&ctx, tx);
                Some(t0.elapsed())
            } else {
                runner.process_direct(&ctx, tx);
                None
            }
        } else {
            let batch: Vec<Transaction> = (0..batch_size)
                .map(|_| {
                    current_id += 1;
                    make_tx(current_id, account_count)
                })
                .collect();
            if sample {
                let t0 = Instant::now();
                runner.process_direct_batch(&ctx, batch);
                Some(t0.elapsed() / batch_div)
            } else {
                runner.process_direct_batch(&ctx, batch);
                None
            }
        };
        processed += batch_size;
        unit += 1;

        if let Some(elapsed) = sampled {
            let bucket = start_time.elapsed().as_secs() as usize;
            if bucket < per_second.len() {
                per_second[bucket].measure(elapsed);
            }
            global.measure(elapsed);

            // Print a row every second.
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                second += 1;
                let wall = start_time.elapsed();
                let delta = processed - last_processed;
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = delta as f64 / interval;

                let b = (second as usize).saturating_sub(1);
                let stats = if b < per_second.len() {
                    per_second[b].get_stats()
                } else {
                    global.get_stats()
                };

                println!(
                    "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>8} | {:>8} |",
                    second,
                    wall.as_secs(),
                    format!("{:.0}", tps),
                    processed,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                );

                last_tick = now;
                last_processed = processed;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!("  +-----+--------+------------+------------+----------+----------+");

    let elapsed = start_time.elapsed();
    let avg_tps = processed as f64 / elapsed.as_secs_f64();
    let stats = global.get_stats();

    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║{:^46}║", "TRANSACTOR RUNNER LOAD");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration      : {:>10.2}s                 ║",
        elapsed.as_secs_f64()
    );
    println!("  ║  Processed     : {:>10}                  ║", processed);
    println!("  ║  Avg TPS       : {:>10.0}                  ║", avg_tps);
    println!("  ║  Batch size    : {:>10}                  ║", batch_size);
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  P50  per-tx   : {:>10}                  ║",
        fmt_ns(stats.p50)
    );
    println!(
        "  ║  P99  per-tx   : {:>10}                  ║",
        fmt_ns(stats.p99)
    );
    println!(
        "  ║  P999 per-tx   : {:>10}                  ║",
        fmt_ns(stats.p999)
    );
    println!(
        "  ║  Min  per-tx   : {:>10}                  ║",
        fmt_ns(stats.min)
    );
    println!(
        "  ║  Max  per-tx   : {:>10}                  ║",
        fmt_ns(stats.max)
    );
    println!(
        "  ║  Samples       : {:>10}                  ║",
        stats.count
    );
    println!("  ╚══════════════════════════════════════════════╝");
    println!();
    // `runner` (its TxRingWriter) and `_drain` drop here; storage self-cleans.
}
