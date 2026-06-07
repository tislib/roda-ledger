//! End-to-end latency under load. A probe submits a deposit, then spins on
//! `last_snapshot_id` until that tx clears the final pipeline stage, timing the
//! round-trip. Repeated `--samples` times per scenario to get P50/P99/P999,
//! while a background thread sustains a target throughput (or runs flat-out).

use clap::Parser;
use ledger::config::LedgerConfig;
use ledger::ledger::Ledger;
use ledger::transaction::Operation;
use roda_latency_tracker::latency_measurer::{LatencyMeasurer, LatencyStats};
use spdlog::Level::Info;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Unmeasured probes before each scenario's sampled run, to warm caches/paths.
const WARMUP_PROBES: u64 = 10;

#[derive(Parser, Debug)]
#[command(
    name = "load_latency",
    about = "End-to-end latency under load for roda-ledger"
)]
struct Args {
    /// Distinct accounts to spread deposits across.
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    /// Probe samples per scenario (each is one deposit → snapshot round-trip).
    #[arg(short, long, default_value_t = 1000)]
    samples: u64,

    /// Settle time after starting background load before sampling, in ms.
    #[arg(short, long, default_value_t = 500)]
    warmup_ms: u64,
}

/// A load scenario: `None` = no background load, `Some(u64::MAX)` = flat-out.
struct Scenario {
    name: &'static str,
    load: Option<u64>,
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

fn fmt_tps(tps: f64) -> String {
    if tps >= 1_000_000.0 {
        format!("{:.2}M", tps / 1_000_000.0)
    } else if tps >= 1_000.0 {
        format!("{:.1}k", tps / 1_000.0)
    } else {
        format!("{:.0}", tps)
    }
}

/// Background load: pace submissions to `target_tps`, or run flat-out when it is
/// `u64::MAX`. Deadline pacing keeps the long-run average on target; when ahead
/// of schedule we yield the core rather than burn it.
fn generate_load(ledger: &Ledger, target_tps: u64, account_count: u64, stop: &AtomicBool) {
    let unbounded = target_tps == u64::MAX;
    let start = Instant::now();
    let mut sent = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if !unbounded {
            let allowed = (start.elapsed().as_secs_f64() * target_tps as f64) as u64;
            if sent >= allowed {
                thread::yield_now();
                continue;
            }
        }
        let account = 1 + rand::random::<u64>() % account_count;
        ledger.submit(Operation::Deposit {
            account,
            amount: 10000,
            user_ref: 0,
        });
        sent += 1;
    }
}

/// Submit one deposit and spin until it reaches the snapshot stage; return the
/// end-to-end latency (admission + full pipeline transit).
fn probe_once(ledger: &Ledger, account_count: u64) -> Duration {
    let account = 1 + rand::random::<u64>() % account_count;
    let start = Instant::now();
    let tx_id = ledger.submit(Operation::Deposit {
        account,
        amount: 10000,
        user_ref: 0,
    });
    let mut spins = 0u64;
    while ledger.last_snapshot_id() < tx_id {
        spins += 1;
        if spins.is_multiple_of(512) {
            thread::yield_now();
        } else {
            spin_loop();
        }
    }
    start.elapsed()
}

/// Wait until the snapshot stage has caught up with everything sequenced. The
/// caller must have already stopped + joined any load thread so the sequenced
/// high-water mark is stable.
fn drain(ledger: &Ledger) {
    loop {
        let sequenced = ledger.last_sequenced_id();
        if ledger.last_snapshot_id() >= sequenced {
            return;
        }
        spin_loop();
    }
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);
    let samples = args.samples.max(1);

    let mut ledger = Ledger::new(LedgerConfig {
        // +1 so SYSTEM_ACCOUNT_ID (0) and accounts 1..=account_count all fit.
        max_accounts: account_count as usize + 1,
        log_level: Info,
        ..LedgerConfig::bench()
    });
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    let scenarios = [
        Scenario {
            name: "No Load",
            load: None,
        },
        Scenario {
            name: "Sustain 10k",
            load: Some(10_000),
        },
        Scenario {
            name: "Sustain 100k",
            load: Some(100_000),
        },
        Scenario {
            name: "Sustain 1M",
            load: Some(1_000_000),
        },
        Scenario {
            name: "Full (Spike)",
            load: Some(u64::MAX),
        },
    ];

    println!();
    println!(
        "  latency under load | accounts={} samples={} probe=deposit→snapshot",
        account_count, samples,
    );
    println!();
    let border =
        "  +---------------+--------+--------------+----------+----------+----------+----------+";
    println!("{border}");
    println!(
        "  | {:<13} | {:^6} | {:^12} | {:^8} | {:^8} | {:^8} | {:^8} |",
        "Scenario", "Target", "Achieved TPS", "P50", "P99", "P999", "Max",
    );
    println!("{border}");

    let progress_step = (samples / 20).max(1);

    for sc in &scenarios {
        // Spawn the background load (if any) and let it reach steady state.
        let stop = Arc::new(AtomicBool::new(false));
        let load_handle = sc.load.map(|tps| {
            let l = ledger.clone();
            let stop = stop.clone();
            thread::spawn(move || generate_load(&l, tps, account_count, &stop))
        });
        if sc.load.is_some() {
            thread::sleep(Duration::from_millis(args.warmup_ms));
        }

        for _ in 0..WARMUP_PROBES {
            probe_once(&ledger, account_count);
        }

        // Sampled run. Snapshot-id delta over the window gives achieved TPS.
        let mut measurer = LatencyMeasurer::new(1);
        let snap_start = ledger.last_snapshot_id();
        let t_start = Instant::now();
        for i in 0..samples {
            measurer.measure(probe_once(&ledger, account_count));
            if (i + 1).is_multiple_of(progress_step) {
                eprint!("\r  {} … {}/{}", sc.name, i + 1, samples);
            }
        }
        let elapsed = t_start.elapsed();
        let snap_end = ledger.last_snapshot_id();
        eprint!("\r{:60}\r", "");

        // Stop + drain so the next scenario starts from an empty pipeline.
        if let Some(handle) = load_handle {
            stop.store(true, Ordering::Relaxed);
            let _ = handle.join();
        }
        drain(&ledger);

        let achieved = (snap_end - snap_start) as f64 / elapsed.as_secs_f64();
        let target = match sc.load {
            None => "-".to_string(),
            Some(t) if t == u64::MAX => "max".to_string(),
            Some(t) => fmt_tps(t as f64),
        };
        let s = measurer.get_stats();
        print_row(sc.name, &target, achieved, &s);
    }

    println!("{border}");
    println!();
}

fn print_row(name: &str, target: &str, achieved: f64, s: &LatencyStats) {
    println!(
        "  | {:<13} | {:>6} | {:>12} | {:>8} | {:>8} | {:>8} | {:>8} |",
        name,
        target,
        fmt_tps(achieved),
        fmt_ns(s.p50),
        fmt_ns(s.p99),
        fmt_ns(s.p999),
        fmt_ns(s.max),
    );
}
