//! Load generator for a live cluster: boots one leader + N followers
//! in-process and drives writes against the leader's embedded ledger
//! while the replication thread ships WAL bytes to every follower.
//!
//! Columns:
//! - TPS            — submitted tx/s
//! - leader         — leader commit_tx_id
//! - min_follower   — minimum follower commit_tx_id across all followers
//! - in-flight      — gap between sent_tx_id and leader_commit_tx_id
//! - min_repl_lag   — min(leader_commit − follower_commit) across followers
//! - max_repl_lag   — max(leader_commit − follower_commit) across followers

use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::cluster::config::NodeServerSection;
use roda_ledger::cluster::{Cluster, ClusterConfig, ClusterMode, PeerConfig};
use roda_ledger::config::{LedgerConfig, StorageConfig};
use roda_ledger::grpc::GrpcServerSection;
use roda_ledger::ledger::Ledger;
use roda_ledger::transaction::Operation;
use spdlog::Level::Critical;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "load_cluster", about = "Cluster load generator for roda-ledger")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Number of followers to boot and replicate to.
    #[arg(short = 'f', long, default_value_t = 3)]
    follower_count: usize,

    /// Max bytes the leader ships per AppendEntries RPC.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    append_entries_max_bytes: usize,

    /// Replication loop idle-poll interval (ms).
    #[arg(long, default_value_t = 1)]
    replication_poll_ms: u64,

    /// Transactions per WAL segment on each follower.
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

/// Aggregate commit-id stats across every follower ledger.
fn follower_commit_stats(followers: &[Arc<Ledger>]) -> (u64, u64) {
    let mut min = u64::MAX;
    let mut max = 0u64;
    for f in followers {
        let c = f.last_commit_id();
        if c < min {
            min = c;
        }
        if c > max {
            max = c;
        }
    }
    if followers.is_empty() {
        (0, 0)
    } else {
        (min, max)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let account_count = args.account_count;
    let follower_count = args.follower_count.max(1);

    // ── Allocate unique ports for every node ────────────────────────────
    let leader_client_port = free_port();
    let leader_node_port = free_port();
    let mut follower_ports: Vec<(u16, u16)> = Vec::with_capacity(follower_count);
    for _ in 0..follower_count {
        follower_ports.push((free_port(), free_port()));
    }

    // ── Boot followers first so their node ports are bound when the leader
    //    replication tasks try to connect. Keep their Cluster handles + ledgers.
    let mut follower_clusters = Vec::with_capacity(follower_count);
    let mut follower_handles = Vec::with_capacity(follower_count);
    let mut follower_ledgers: Vec<Arc<Ledger>> = Vec::with_capacity(follower_count);
    for (idx, (client_port, node_port)) in follower_ports.iter().enumerate() {
        // node_ids: 2..=follower_count+1 (1 reserved for leader).
        let node_id = (idx as u64) + 2;
        let cfg = ClusterConfig {
            mode: ClusterMode::Follower,
            node_id,
            term: 1,
            peers: Vec::new(),
            server: GrpcServerSection {
                host: "127.0.0.1".into(),
                port: *client_port,
                ..Default::default()
            },
            node: NodeServerSection {
                host: "127.0.0.1".into(),
                port: *node_port,
            },
            ledger: ledger_cfg(
                account_count,
                &tmp_dir(&format!("follower_{}", idx + 1)),
                args.follower_segment_tx_count,
            ),
            replication_poll_ms: args.replication_poll_ms,
            append_entries_max_bytes: args.append_entries_max_bytes,
        };
        let cluster = Cluster::new(cfg).expect("follower ledger");
        let handles = cluster.run().await.expect("follower run");
        follower_ledgers.push(cluster.ledger());
        follower_handles.push(handles);
        follower_clusters.push(cluster);
    }

    // ── Leader config with one PeerConfig per follower ───────────────────
    let peers: Vec<PeerConfig> = follower_ports
        .iter()
        .enumerate()
        .map(|(idx, (_, node_port))| PeerConfig {
            id: (idx as u64) + 2,
            node_addr: format!("http://127.0.0.1:{}", node_port),
        })
        .collect();

    let leader_cfg = ClusterConfig {
        mode: ClusterMode::Leader,
        node_id: 1,
        term: 1,
        peers,
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

    let leader = Cluster::new(leader_cfg).expect("leader ledger");
    let leader_handles = leader.run().await.expect("leader run");
    let leader_ledger = leader.ledger();

    // Give replication tasks a beat to connect before writes start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let duration = Duration::from_secs(args.duration);
    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    // Drive writes on a blocking thread so the tokio runtime keeps
    // servicing replication (which is async).
    let writer_followers = follower_ledgers.clone();
    let writer = tokio::task::spawn_blocking(move || {
        run_writer(
            leader_ledger,
            writer_followers,
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
        "  ║  Followers     : {:>10}                  ║",
        follower_count
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
    for h in follower_handles {
        h.abort();
    }
}

/// Blocking write loop. Tracks min/max follower commit across every
/// follower ledger each tick.
fn run_writer(
    leader_ledger: Arc<Ledger>,
    followers: Vec<Arc<Ledger>>,
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

    let sep = "  +-----+--------+------------+------------+------------+----------+----------+------------+------------+------------+";
    println!();
    println!("{}", sep);
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} | {:>10} | {:>10} |",
        "#", "time", "TPS", "leader", "min_fol", "P50", "P99", "in-flight", "min_lag", "max_lag"
    );
    println!("{}", sep);

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
                let (min_follower, max_follower) = follower_commit_stats(&followers);
                let delta = leader_commit - last_leader;
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = delta as f64 / interval;
                let in_flight = i.saturating_sub(leader_commit);
                let min_repl_lag = leader_commit.saturating_sub(max_follower);
                let max_repl_lag = leader_commit.saturating_sub(min_follower);

                let bucket = (second as usize).saturating_sub(1);
                let stats = if bucket < per_second.len() {
                    per_second[bucket].get_stats()
                } else {
                    global.get_stats()
                };

                println!(
                    "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} | {:>10} | {:>10} |",
                    second,
                    wall.as_secs(),
                    format!("{:.0}", tps),
                    leader_commit,
                    min_follower,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    in_flight,
                    min_repl_lag,
                    max_repl_lag,
                );

                last_tick = now;
                last_leader = leader_commit;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!("{}", sep);

    // Drain: wait briefly for every follower to catch up (cap 30s).
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let leader_commit = leader_ledger.last_commit_id();
        let (min_follower, _) = follower_commit_stats(&followers);
        if min_follower >= leader_commit || Instant::now() >= drain_deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let leader_commit = leader_ledger.last_commit_id();
    let (min_follower, max_follower) = follower_commit_stats(&followers);
    println!(
        "  drain: leader_commit={} min_follower={} max_follower={} min_lag={} max_lag={}",
        leader_commit,
        min_follower,
        max_follower,
        leader_commit.saturating_sub(max_follower),
        leader_commit.saturating_sub(min_follower),
    );

    (i, global.get_stats(), start_time.elapsed())
}
