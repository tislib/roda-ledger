//! Latency under load for a single-node cluster, in-process (no gRPC hop).
//! The cluster analog of `ledger`'s `load_latency` bin: a probe submits a
//! deposit and waits — via the cluster's own `Waiter` — until it reaches the
//! chosen wait level, while a background thread sustains load. Adds the
//! `cluster-commit` wait level, which has no ledger-side equivalent.

use clap::{Parser, ValueEnum};
use cluster::{ClusterNode, Config, ServerSection};
use ledger::config::LedgerConfig;
use ledger::ledger::PipelineIndexKind;
use ledger::transactor::transaction::Operation;
use proto::ledger::WaitLevel as ProtoWaitLevel;
use roda_latency_tracker::latency_measurer::{LatencyMeasurer, LatencyStats};
use spdlog::Level::Info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Unmeasured probes before each scenario's sampled run, to warm caches/paths.
const WARMUP_PROBES: u64 = 10;

#[derive(Parser, Debug)]
#[command(
    name = "cluster_load_latency",
    about = "End-to-end latency under load for a single-node cluster"
)]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 1000)]
    samples: u64,

    #[arg(short, long, default_value_t = 500)]
    warmup_ms: u64,

    /// Wait level the probe blocks on (via the cluster `Waiter`).
    #[arg(short = 'l', long, value_enum, default_value_t = Stage::Commit)]
    wait_level: Stage,
}

struct Scenario {
    name: &'static str,
    load: Option<u64>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Stage {
    Compute,
    Commit,
    Snapshot,
    /// Quorum-committed (singleton: self-quorum, driven by the waiter's
    /// per-poll self-advance). No ledger-side equivalent.
    ClusterCommit,
}

impl Stage {
    fn proto(self) -> ProtoWaitLevel {
        match self {
            Stage::Compute => ProtoWaitLevel::Computed,
            Stage::Commit => ProtoWaitLevel::Committed,
            Stage::Snapshot => ProtoWaitLevel::Snapshot,
            Stage::ClusterCommit => ProtoWaitLevel::ClusterCommit,
        }
    }

    fn index(self, node: &ClusterNode) -> u64 {
        match self {
            Stage::Compute => node.last_compute_id(),
            Stage::Commit => node.last_commit_id(),
            Stage::Snapshot => node.last_snapshot_id(),
            Stage::ClusterCommit => node.cluster_commit_index(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Stage::Compute => "compute (executed)",
            Stage::Commit => "commit (durable, fsync)",
            Stage::Snapshot => "snapshot (readable)",
            Stage::ClusterCommit => "cluster-commit (quorum)",
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

fn standalone_config() -> Config {
    Config {
        cluster: None,
        server: ServerSection {
            port: 0,
            ..Default::default()
        },
        ledger: LedgerConfig {
            log_level: Info,
            ..LedgerConfig::bench()
        },
    }
}

/// Background load: pace to `target_tps`, or flat-out when `u64::MAX`. Submits
/// in-process; runs on a plain OS thread (not the async runtime).
fn generate_load(node: &ClusterNode, target_tps: u64, account_count: u64, stop: &AtomicBool) {
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
        node.submit(Operation::Deposit {
            account,
            amount: 10000,
            user_ref: 0,
        });
        sent += 1;
    }
}

/// Submit one deposit and await its arrival at `stage` via the cluster waiter.
async fn probe_once(node: &ClusterNode, account_count: u64, stage: Stage) -> Duration {
    let account = 1 + rand::random::<u64>() % account_count;
    let start = Instant::now();
    let tx_id = node.submit(Operation::Deposit {
        account,
        amount: 10000,
        user_ref: 0,
    });
    node.wait_for_level(tx_id, stage.proto()).await;
    start.elapsed()
}

/// Await the stage catching up to everything sequenced (waiter drives the
/// singleton self-advance for the cluster-commit level).
async fn drain(node: &ClusterNode, stage: Stage) {
    let target = node.last_sequenced_id();
    node.wait_for_level(target, stage.proto()).await;
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

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);
    let samples = args.samples.max(1);
    let wait_level = args.wait_level;

    let node = ClusterNode::new(standalone_config()).expect("build node");
    let node = node.run().expect("run node");

    // Hook parity with the ledger bins.
    let hook_state: Arc<[AtomicU64; 3]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    {
        let s = hook_state.clone();
        node.set_index_hook(Arc::new(move |kind: PipelineIndexKind, value: u64| {
            let slot = match kind {
                PipelineIndexKind::Compute => 0,
                PipelineIndexKind::Commit => 1,
                PipelineIndexKind::Snapshot => 2,
            };
            s[slot].store(value, Ordering::Relaxed);
        }));
    }

    node.open_accounts(account_count as u32);
    let node = Arc::new(node);

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

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build probe runtime");

    println!();
    println!(
        "  single-node cluster latency under load | accounts={} samples={} probe=deposit→{}",
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

    rt.block_on(async {
        for sc in &scenarios {
            // Spawn background load (if any) on a plain thread; let it settle.
            let stop = Arc::new(AtomicBool::new(false));
            let load_handle = sc.load.map(|tps| {
                let n = node.clone();
                let stop = stop.clone();
                thread::spawn(move || generate_load(&n, tps, account_count, &stop))
            });
            if sc.load.is_some() {
                tokio::time::sleep(Duration::from_millis(args.warmup_ms)).await;
            }

            for _ in 0..WARMUP_PROBES {
                probe_once(&node, account_count, wait_level).await;
            }

            let mut measurer = LatencyMeasurer::new(1);
            let snap_start = wait_level.index(&node);
            let t_start = Instant::now();
            for _ in 0..samples {
                measurer.measure(probe_once(&node, account_count, wait_level).await);
            }
            let elapsed = t_start.elapsed();
            let snap_end = wait_level.index(&node);

            if let Some(handle) = load_handle {
                stop.store(true, Ordering::Relaxed);
                let _ = handle.join();
            }
            drain(&node, wait_level).await;

            let achieved = (snap_end - snap_start) as f64 / elapsed.as_secs_f64();
            let target = match sc.load {
                None => "-".to_string(),
                Some(t) if t == u64::MAX => "max".to_string(),
                Some(t) => fmt_tps(t as f64),
            };
            let s = measurer.get_stats();
            print_row(sc.name, &target, achieved, &s);
        }
    });

    println!("{border}");
    println!();
}
