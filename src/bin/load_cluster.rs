//! Load generator for a live cluster: boots one leader + one follower
//! in-process and drives writes against the leader's embedded ledger
//! while the replication thread ships WAL bytes to the follower.
//!
//! Columns:
//! - TPS          — submitted tx/s
//! - leader       — leader commit_tx_id
//! - follower     — follower commit_tx_id (through replication)
//! - in-flight    — gap between sent_tx_id and leader_commit_tx_id
//! - repl-lag     — gap between leader_commit_tx_id and follower_commit_tx_id

use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::cluster::config::NodeServerSection;
use roda_ledger::cluster::{Cluster, ClusterConfig, ClusterMode, PeerConfig};
use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::grpc::GrpcServerSection;
use roda_ledger::transaction::Operation;
use spdlog::Level::Critical;
use std::net::TcpListener;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "load_cluster", about = "Cluster load generator for roda-ledger")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Max bytes the leader ships per AppendEntries RPC.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    append_entries_max_bytes: usize,

    /// Replication loop idle-poll interval (ms).
    #[arg(long, default_value_t = 1)]
    replication_poll_ms: u64,

    /// Transactions per WAL segment on the follower.
    #[arg(long, default_value_t = 10_000_000)]
    follower_segment_tx_count: u64,
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

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn tmp_dir(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut d = std::env::current_dir().unwrap();
    d.push(format!("temp_load_cluster_{}_{}", name, nanos));
    d.to_string_lossy().into_owned()
}

fn ledger_cfg(account_count: u64, data_dir: &str, tx_per_seg: u64) -> LedgerConfig {
    LedgerConfig {
        max_accounts: account_count as usize,
        log_level: Critical,
        storage: StorageConfig {
            data_dir: data_dir.to_string(),
            temporary: true,
            transaction_count_per_segment: tx_per_seg,
            snapshot_frequency: u32::MAX,
        },
        // Avoid seal I/O for bench-like behavior.
        disable_seal: true,
        seal_check_internal: Duration::from_secs(600),
        ..LedgerConfig::default()
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let account_count = args.account_count;

    // ── Leader/follower on unique ports, temp dirs ──────────────────────
    let leader_client_port = free_port();
    let leader_node_port = free_port();
    let follower_client_port = free_port();
    let follower_node_port = free_port();

    let leader_cfg = ClusterConfig {
        mode: ClusterMode::Leader,
        node_id: 1,
        term: 1,
        peers: vec![PeerConfig {
            id: 2,
            node_addr: format!("http://127.0.0.1:{}", follower_node_port),
        }],
        server: GrpcServerSection {
            host: "127.0.0.1".into(),
            port: leader_client_port,
            ..Default::default()
        },
        node: NodeServerSection {
            host: "127.0.0.1".into(),
            port: leader_node_port,
        },
        ledger: ledger_cfg(account_count, &tmp_dir("leader"), 10_000_000),
        replication_poll_ms: args.replication_poll_ms,
        append_entries_max_bytes: args.append_entries_max_bytes,
    };

    let follower_cfg = ClusterConfig {
        mode: ClusterMode::Follower,
        node_id: 2,
        term: 1,
        peers: Vec::new(),
        server: GrpcServerSection {
            host: "127.0.0.1".into(),
            port: follower_client_port,
            ..Default::default()
        },
        node: NodeServerSection {
            host: "127.0.0.1".into(),
            port: follower_node_port,
        },
        ledger: ledger_cfg(
            account_count,
            &tmp_dir("follower"),
            args.follower_segment_tx_count,
        ),
        replication_poll_ms: args.replication_poll_ms,
        append_entries_max_bytes: args.append_entries_max_bytes,
    };

    // Follower first so its node port is listening when the leader's
    // replication task connects.
    let follower = Cluster::new(follower_cfg).expect("follower ledger");
    let follower_handles = follower.run().await.expect("follower run");

    let leader = Cluster::new(leader_cfg).expect("leader ledger");
    let leader_handles = leader.run().await.expect("leader run");

    // Drop the Cluster wrappers' handles (gRPC servers) — we drive writes
    // directly against the embedded leader ledger for load.rs-like throughput.
    let leader_ledger = leader.ledger();
    let follower_ledger = follower.ledger();

    // Give the replication thread a beat to connect before we start writing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let duration = Duration::from_secs(args.duration);
    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    // Drive writes on a blocking thread so the tokio runtime keeps
    // servicing replication (which is async).
    let writer = tokio::task::spawn_blocking(move || {
        run_writer(
            leader_ledger,
            follower_ledger,
            account_count,
            duration,
            &mut per_second,
            &mut global,
        )
    });

    let (submitted, global_stats, elapsed) = writer.await.expect("writer thread");

    let avg_tps = submitted as f64 / elapsed.as_secs_f64();
    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║          CLUSTER LOAD TEST SUMMARY           ║");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration      : {:>10.2}s                 ║",
        elapsed.as_secs_f64()
    );
    println!(
        "  ║  Submitted     : {:>10}                  ║",
        submitted
    );
    println!("  ║  Avg TPS       : {:>10.0}                  ║", avg_tps);
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  P50  Latency  : {:>10}                  ║",
        fmt_ns(global_stats.p50)
    );
    println!(
        "  ║  P99  Latency  : {:>10}                  ║",
        fmt_ns(global_stats.p99)
    );
    println!(
        "  ║  P999 Latency  : {:>10}                  ║",
        fmt_ns(global_stats.p999)
    );
    println!(
        "  ║  Min  Latency  : {:>10}                  ║",
        fmt_ns(global_stats.min)
    );
    println!(
        "  ║  Max  Latency  : {:>10}                  ║",
        fmt_ns(global_stats.max)
    );
    println!(
        "  ║  Samples       : {:>10}                  ║",
        global_stats.count
    );
    println!("  ╚══════════════════════════════════════════════╝");
    println!();

    leader_handles.abort();
    follower_handles.abort();
}

/// Blocking write loop. Mirrors the per-second table in `load.rs` but with
/// a dedicated `follower` column and a `repl-lag` column (leader_commit –
/// follower_commit).
fn run_writer(
    leader_ledger: std::sync::Arc<roda_ledger::ledger::Ledger>,
    follower_ledger: std::sync::Arc<roda_ledger::ledger::Ledger>,
    account_count: u64,
    duration: Duration,
    per_second: &mut [LatencyMeasurer],
    global: &mut LatencyMeasurer,
) -> (u64, roda_latency_tracker::latency_measurer::LatencyStats, Duration) {
    let start_time = Instant::now();
    let mut i = 0u64;
    let mut last_tick = start_time;
    let mut last_leader = 0u64;
    let mut second = 0u32;

    println!();
    println!(
        "  +-----+--------+------------+------------+------------+----------+----------+------------+----------+"
    );
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} | {:>8} |",
        "#", "time", "TPS", "leader", "follower", "P50", "P99", "in-flight", "repl-lag"
    );
    println!(
        "  +-----+--------+------------+------------+------------+----------+----------+------------+----------+"
    );

    loop {
        let account = 1 + rand::random::<u64>() % account_count;
        let op = Operation::Deposit {
            account,
            amount: 10_000,
            user_ref: 0,
        };

        let sample = i.is_multiple_of(10_000);

        if sample {
            let t0 = Instant::now();
            leader_ledger.submit(op);
            let elapsed = t0.elapsed();
            let bucket = start_time.elapsed().as_secs() as usize;
            if bucket < per_second.len() {
                per_second[bucket].measure(elapsed);
            }
            global.measure(elapsed);
        } else {
            leader_ledger.submit(op);
        }

        i += 1;

        if sample {
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                second += 1;
                let wall = start_time.elapsed();
                let leader_commit = leader_ledger.last_commit_id();
                let follower_commit = follower_ledger.last_commit_id();
                let delta = leader_commit - last_leader;
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = delta as f64 / interval;
                let in_flight = i.saturating_sub(leader_commit);
                let repl_lag = leader_commit.saturating_sub(follower_commit);

                let bucket = (second as usize).saturating_sub(1);
                let stats = if bucket < per_second.len() {
                    per_second[bucket].get_stats()
                } else {
                    global.get_stats()
                };

                println!(
                    "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} | {:>8} |",
                    second,
                    wall.as_secs(),
                    format!("{:.0}", tps),
                    leader_commit,
                    follower_commit,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    in_flight,
                    repl_lag,
                );

                last_tick = now;
                last_leader = leader_commit;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!(
        "  +-----+--------+------------+------------+------------+----------+----------+------------+----------+"
    );

    // Drain: wait briefly for follower to catch up so the summary reflects
    // steady state, capped at 30 s.
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while follower_ledger.last_commit_id() < leader_ledger.last_commit_id()
        && Instant::now() < drain_deadline
    {
        std::thread::sleep(Duration::from_millis(20));
    }

    let leader_commit = leader_ledger.last_commit_id();
    let follower_commit = follower_ledger.last_commit_id();
    println!(
        "  drain: leader_commit={} follower_commit={} repl-lag={}",
        leader_commit,
        follower_commit,
        leader_commit.saturating_sub(follower_commit)
    );

    (i, global.get_stats(), start_time.elapsed())
}
