//! Throughput load for a single-node cluster, in-process (no gRPC hop).
//! The cluster analog of `ledger`'s `load` bin: same submit loop, but driven
//! through a standalone `ClusterNode` (singleton consensus + waiter on top of
//! the ledger). Lets us isolate the cluster layer's per-submit overhead.

use clap::Parser;
use cluster::{ClusterNode, Config, ServerSection};
use ledger::config::LedgerConfig;
use ledger::ledger::PipelineIndexKind;
use ledger::transactor::transaction::Operation;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use spdlog::Level::Info;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "cluster_load", about = "Single-node cluster throughput load")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 20)]
    duration: u64,
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

/// Standalone (no `[cluster]`) single-node config on an ephemeral client port
/// (we submit in-process, so the gRPC bind is unused).
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

fn main() {
    let args = Args::parse();
    let account_count = args.account_count.max(1);

    let node = ClusterNode::new(standalone_config()).expect("build node");
    let node = node.run().expect("run node");

    // Hook parity with the ledger bins: mutex-free mirror into local atomics.
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

    let start = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut global = LatencyMeasurer::new(1);
    let mut i = 0u64;
    let mut last_tick = start;
    let mut last_committed = 0u64;

    println!();
    println!(
        "  single-node cluster throughput | accounts={account_count} dur={}s",
        args.duration
    );
    println!(
        "  (cluster_commit advances on the 50ms singleton self-advance when no waiter is active)"
    );
    println!();

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
            node.submit(op);
            global.measure(t0.elapsed());
        } else {
            node.submit(op);
        }
        i += 1;

        if sample {
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                let committed = node.last_commit_id();
                let cc = node.cluster_commit_index();
                let tps = (committed - last_committed) as f64
                    / now.duration_since(last_tick).as_secs_f64();
                println!(
                    "  t={:>2}s  TPS={:>10.0}  commit={:>11}  cluster_commit={:>11}  cc_lag={:>9}",
                    start.elapsed().as_secs(),
                    tps,
                    committed,
                    cc,
                    committed.saturating_sub(cc),
                );
                last_tick = now;
                last_committed = committed;
            }
            if start.elapsed() > duration {
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let avg_tps = i as f64 / elapsed.as_secs_f64();
    let s = global.get_stats();
    println!();
    println!(
        "  SUMMARY  submitted={i}  avg_tps={avg_tps:.0}  dur={:.2}s",
        elapsed.as_secs_f64()
    );
    println!(
        "  submit latency  P50={}  P99={}  P999={}",
        fmt_ns(s.p50),
        fmt_ns(s.p99),
        fmt_ns(s.p999),
    );
    println!();
}
