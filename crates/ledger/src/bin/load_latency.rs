//! Latency under load. A probe submits a deposit, then spins on the chosen
//! pipeline index (`--wait-level`: compute / write / commit / snapshot) until
//! that tx arrives, timing the round-trip. Repeated `--samples` times per
//! scenario for P50/P99/P999, while a background thread sustains a target
//! throughput (or runs flat-out).

use clap::{Parser, ValueEnum};
use ledger::config::LedgerConfig;
use ledger::ledger::{Ledger, PipelineIndexKind};
use ledger::transactor::transaction::Operation;
use roda_latency_tracker::latency_measurer::{LatencyMeasurer, LatencyStats};
use spdlog::Level::Info;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    /// Probe samples per scenario (each is one deposit → wait-level round-trip).
    #[arg(short, long, default_value_t = 1000)]
    samples: u64,

    /// Settle time after starting background load before sampling, in ms.
    #[arg(short, long, default_value_t = 500)]
    warmup_ms: u64,

    /// Pipeline stage the probe waits for before stopping the timer.
    #[arg(short = 'l', long, value_enum, default_value_t = Stage::Commit)]
    wait_level: Stage,
}

/// A load scenario: `None` = no background load, `Some(u64::MAX)` = flat-out.
struct Scenario {
    name: &'static str,
    load: Option<u64>,
}

/// The pipeline index a probe waits for — the "wait level".
#[derive(Copy, Clone, Debug, ValueEnum)]
enum Stage {
    /// Executed by the transactor (in-memory).
    Compute,
    /// Written to the WAL page cache (buffered, pre-fsync).
    Write,
    /// fdatasync'd to disk — durable.
    Commit,
    /// Applied to the snapshot / indexer — readable.
    Snapshot,
}

impl Stage {
    /// Current high-water mark of this stage's pipeline index.
    #[inline]
    fn index(self, ledger: &Ledger) -> u64 {
        match self {
            Stage::Compute => ledger.last_compute_id(),
            Stage::Write => ledger.last_write_id(),
            Stage::Commit => ledger.last_commit_id(),
            Stage::Snapshot => ledger.last_snapshot_id(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Stage::Compute => "compute (executed)",
            Stage::Write => "write (page cache, pre-fsync)",
            Stage::Commit => "commit (durable, fsync)",
            Stage::Snapshot => "snapshot (readable)",
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

/// Submit one deposit and spin until it reaches `stage`; return the round-trip
/// latency (admission + pipeline transit up to that stage).
fn probe_once(ledger: &Ledger, account_count: u64, stage: Stage) -> Duration {
    let account = 1 + rand::random::<u64>() % account_count;
    let start = Instant::now();
    let tx_id = ledger.submit(Operation::Deposit {
        account,
        amount: 10000,
        user_ref: 0,
    });
    let mut spins = 0u64;
    while stage.index(ledger) < tx_id {
        spins += 1;
        if spins.is_multiple_of(512) {
            thread::yield_now();
        } else {
            spin_loop();
        }
    }
    start.elapsed()
}

/// Wait until `stage` has caught up with everything sequenced. The caller must
/// have already stopped + joined any load thread so the sequenced high-water
/// mark is stable.
fn drain(ledger: &Ledger, stage: Stage) {
    loop {
        let sequenced = ledger.last_sequenced_id();
        if stage.index(ledger) >= sequenced {
            return;
        }
        spin_loop();
    }
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);
    let samples = args.samples.max(1);
    let wait_level = args.wait_level;

    let mut ledger = Ledger::new(LedgerConfig {
        // +1 so SYSTEM_ACCOUNT_ID (0) and accounts 1..=account_count all fit.
        log_level: Info,
        ..LedgerConfig::bench()
    });
    // Realistic mutex-free consumer: mirror each index into a local atomic.
    let hook_state: Arc<[AtomicU64; 3]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    {
        let s = hook_state.clone();
        ledger.set_index_hook(Arc::new(move |kind: PipelineIndexKind, value: u64| {
            let slot = match kind {
                PipelineIndexKind::Compute => 0,
                PipelineIndexKind::Commit => 1,
                PipelineIndexKind::Snapshot => 2,
            };
            s[slot].store(value, Ordering::Relaxed);
        }));
    }
    ledger.start().unwrap();
    // Existence enforcement (ADR-022): open the accounts the load hits (1..=N).
    ledger.open_accounts(account_count as u32);
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
        "  latency under load | accounts={} samples={} probe=deposit→{}",
        account_count,
        samples,
        wait_level.label(),
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
            probe_once(&ledger, account_count, wait_level);
        }

        // Sampled run. wait-level index delta over the window gives achieved TPS.
        let mut measurer = LatencyMeasurer::new(1);
        let snap_start = wait_level.index(&ledger);
        let t_start = Instant::now();
        for _ in 0..samples {
            measurer.measure(probe_once(&ledger, account_count, wait_level));
        }
        let elapsed = t_start.elapsed();
        let snap_end = wait_level.index(&ledger);

        // Stop + drain so the next scenario starts from an empty pipeline.
        if let Some(handle) = load_handle {
            stop.store(true, Ordering::Relaxed);
            let _ = handle.join();
        }
        drain(&ledger, wait_level);

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
