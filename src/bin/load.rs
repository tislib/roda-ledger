use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{Operation, WaitLevel};
use spdlog::Level::Info;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "load", about = "Load generator for roda-ledger")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 10)]
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
        let op = Operation::Deposit {
            account,
            amount: 10000,
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
                let committed = ledger.last_committed_id();
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
    println!("  ║              LOAD TEST SUMMARY               ║");
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
