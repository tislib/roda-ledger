use clap::{Parser, ValueEnum};
use ledger::config::LedgerConfig;
use ledger::pipeline::TransactorContext;
use ledger::test_support::mock_pipeline;
use ledger::transaction::{Operation, Transaction, TransactionInput};
use ledger::transactor::Transactor;
use ledger::wait_strategy::WaitStrategy;
use ledger::wasm_runtime::WasmRuntime;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use std::hint::spin_loop;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::{Storage, StorageConfig};

#[derive(Parser, Debug)]
#[command(
    name = "load_transactor",
    about = "Load generator for the Transactor stage in isolation (no WAL / snapshot / seal)"
)]
struct Args {
    /// Distinct user accounts to spread deposits across.
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    /// How long to drive load, in seconds.
    #[arg(short, long, default_value_t = 10)]
    duration: u64,

    /// Sample end-to-end latency (submit → processed) instead of submit-only.
    #[arg(short, long, default_value_t = false)]
    wait: bool,

    /// Sequencer→transactor queue capacity (the producer's backpressure point).
    #[arg(short, long, default_value_t = 1 << 20)]
    queue_size: usize,

    /// tx_ring capacity; a background reader keeps it drained.
    #[arg(short, long, default_value_t = 1 << 16)]
    ring_size: usize,

    /// Idle / backpressure spin policy.
    #[arg(short = 's', long, value_enum, default_value_t = WaitStrategyArg::Balanced)]
    wait_strategy: WaitStrategyArg,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum WaitStrategyArg {
    LowLatency,
    Balanced,
    LowCpu,
}

impl From<WaitStrategyArg> for WaitStrategy {
    fn from(arg: WaitStrategyArg) -> Self {
        match arg {
            WaitStrategyArg::LowLatency => WaitStrategy::LowLatency,
            WaitStrategyArg::Balanced => WaitStrategy::Balanced,
            WaitStrategyArg::LowCpu => WaitStrategy::LowCpu,
        }
    }
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

/// Push one transaction, spinning on backpressure until the queue accepts it.
fn submit(ctx: &TransactorContext, mut tx: Transaction) {
    while let Err(returned) = ctx.input().push(TransactionInput::Single(tx)) {
        tx = returned.single();
        spin_loop();
    }
}

/// Push, then spin until the transactor has processed through `tx.id`.
fn submit_and_wait(ctx: &TransactorContext, tx: Transaction) {
    let id = tx.id;
    submit(ctx, tx);
    while ctx.get_processed_index() < id {
        spin_loop();
    }
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);
    let wait_mode = args.wait;
    let wait_strategy: WaitStrategy = args.wait_strategy.into();

    // Standalone transactor: pipeline + ring writer + a background drain that
    // keeps the ring empty so the transactor never blocks on a full ring.
    let (pipeline, writer, _drain) = mock_pipeline(args.queue_size, args.ring_size, wait_strategy);

    // Self-cleaning temp storage (removed on drop). The deposit path never runs
    // WASM, but the Transactor still needs a runtime handle to construct.
    let mut data_dir = std::env::temp_dir();
    data_dir.push(format!("roda_load_transactor_{}", rand::random::<u64>()));
    let config = LedgerConfig {
        // +1 so SYSTEM_ACCOUNT_ID (0) and accounts 1..=account_count all fit.
        storage: StorageConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            temporary: true,
            ..StorageConfig::default()
        },
        ..LedgerConfig::default()
    };
    let storage = Arc::new(Storage::new(config.storage.clone()).expect("init storage"));
    let runtime = Arc::new(WasmRuntime::new(storage));
    let mut transactor = Transactor::new(&config, runtime, writer);
    let handle = transactor
        .start(pipeline.transactor_context())
        .expect("start transactor");

    let push_ctx = pipeline.transactor_context();
    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut i = 0u64;
    let mut current_id = 0u64;

    // Per-second measurers + 1 global.
    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    let mut last_tick = start_time;
    let mut last_processed = 0u64;
    let mut second = 0u32;

    println!();
    println!(
        "  transactor load | accounts={} queue={} ring={} wait={:?} mode={}",
        account_count,
        args.queue_size,
        args.ring_size,
        wait_strategy,
        if wait_mode { "wait" } else { "async" },
    );

    // Table header
    println!();
    println!("  +-----+--------+------------+------------+----------+----------+------------+");
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} |",
        "#", "time", "TPS", "proc'd", "P50", "P99", "in-flight"
    );
    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    // Existence enforcement (ADR-022): open accounts 1..=account_count first.
    current_id += 1;
    let mut open_tx = Transaction::new(Operation::OpenAccount {
        count: account_count as u32,
        user_ref: 0,
    });
    open_tx.id = current_id;
    submit_and_wait(&push_ctx, open_tx);

    loop {
        current_id += 1;
        let account = 1 + rand::random::<u64>() % account_count;
        let mut tx = Transaction::new(Operation::Deposit {
            account,
            amount: 10000,
            user_ref: 0,
        });
        tx.id = current_id;

        let sample = i.is_multiple_of(10000);

        if sample {
            let t0 = Instant::now();
            if wait_mode {
                submit_and_wait(&push_ctx, tx);
            } else {
                submit(&push_ctx, tx);
            }
            let elapsed = t0.elapsed();
            let bucket = start_time.elapsed().as_secs() as usize;
            if bucket < per_second.len() {
                per_second[bucket].measure(elapsed);
            }
            global.measure(elapsed);
        } else {
            submit(&push_ctx, tx);
        }

        i += 1;

        if sample {
            // Print a row every second
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                second += 1;
                let wall = start_time.elapsed();
                let processed = push_ctx.get_processed_index();
                let delta = processed - last_processed;
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = delta as f64 / interval;
                let in_flight = i.saturating_sub(processed);

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
                    processed,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    in_flight,
                );

                last_tick = now;
                last_processed = processed;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    // Let the transactor drain the queue so the tally reflects all work submitted.
    let submitted = i;
    let drain_deadline = start_time.elapsed() + Duration::from_secs(5);
    while push_ctx.get_processed_index() < submitted && start_time.elapsed() < drain_deadline {
        spin_loop();
    }

    let elapsed = start_time.elapsed();
    let processed = push_ctx.get_processed_index();
    let avg_tps = processed as f64 / elapsed.as_secs_f64();
    let stats = global.get_stats();

    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║{:^46}║", "TRANSACTOR LOAD SUMMARY");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration      : {:>10.2}s                 ║",
        elapsed.as_secs_f64()
    );
    println!("  ║  Submitted     : {:>10}                  ║", submitted);
    println!("  ║  Processed     : {:>10}                  ║", processed);
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

    pipeline.shutdown();
    let _ = handle.join();
    // `_drain` is dropped here, stopping the background ring drain.
}
