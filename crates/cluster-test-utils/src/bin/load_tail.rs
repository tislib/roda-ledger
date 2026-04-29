//! Single-ledger load generator that runs a `WalTailer` in a background
//! thread in **discard mode**. Measures the gap between what the writer has
//! submitted and what the tailer has consumed — isolates tailer throughput
//! from replication/network.

use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;
use spdlog::Level::Info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "load_tail",
    about = "Tailer-only load generator for roda-ledger"
)]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Buffer size (bytes) used by the background tailer per tail() call.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    tail_buf_bytes: usize,
}

const WAL_RECORD_SIZE: usize = 40;
const TX_ID_OFFSET: usize = 8;

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}

/// tx_id field at offset 8 of the last 40-byte record in `bytes`.
fn tx_id_of_last_record(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(WAL_RECORD_SIZE) {
        return None;
    }
    let off = bytes.len() - WAL_RECORD_SIZE + TX_ID_OFFSET;
    Some(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?))
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count;
    let tail_buf_bytes = (args.tail_buf_bytes / WAL_RECORD_SIZE).max(1) * WAL_RECORD_SIZE;

    let mut ledger = Ledger::new(LedgerConfig {
        max_accounts: account_count as usize,
        log_level: Info,
        ..LedgerConfig::bench()
    });
    ledger.start().unwrap();
    let ledger = Arc::new(ledger);

    // ── Background tailer (discard mode) ────────────────────────────────
    let running = Arc::new(AtomicBool::new(true));
    let tailed_tx_id = Arc::new(AtomicU64::new(0));
    let tailed_bytes = Arc::new(AtomicU64::new(0));
    {
        let ledger = ledger.clone();
        let running = running.clone();
        let tailed_tx_id = tailed_tx_id.clone();
        let tailed_bytes = tailed_bytes.clone();
        thread::Builder::new()
            .name("tailer".into())
            .spawn(move || {
                let mut tailer = ledger.wal_tailer();
                let mut buf = vec![0u8; tail_buf_bytes];
                let mut from_tx_id: u64 = 1;
                while running.load(Ordering::Relaxed) {
                    let n = tailer.tail(from_tx_id, &mut buf) as usize;
                    if n == 0 {
                        // No sleep — tight loop matches the replication thread.
                        continue;
                    }
                    if let Some(last_tx) = tx_id_of_last_record(&buf[..n]) {
                        from_tx_id = last_tx + 1;
                        tailed_tx_id.store(last_tx, Ordering::Release);
                    }
                    tailed_bytes.fetch_add(n as u64, Ordering::Relaxed);
                }
            })
            .expect("spawn tailer");
    }

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut i = 0u64;

    let num_buckets = args.duration as usize + 10;
    let mut per_second: Vec<LatencyMeasurer> =
        (0..num_buckets).map(|_| LatencyMeasurer::new(1)).collect();
    let mut global = LatencyMeasurer::new(1);

    let mut last_tick = start_time;
    let mut last_committed = 0u64;
    let mut last_tailed_bytes = 0u64;
    let mut second = 0u32;

    println!();
    println!(
        "  +-----+--------+------------+------------+------------+----------+----------+------------+----------+"
    );
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} | {:>8} |",
        "#", "time", "TPS", "leader", "tailed", "P50", "P99", "in-flight", "MB/s"
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
            ledger.submit(op);
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
            let now = Instant::now();
            if now.duration_since(last_tick) >= Duration::from_secs(1) {
                second += 1;
                let wall = start_time.elapsed();
                let committed = ledger.last_commit_id();
                let tailed = tailed_tx_id.load(Ordering::Acquire);
                let total_tail_bytes = tailed_bytes.load(Ordering::Relaxed);
                let interval = now.duration_since(last_tick).as_secs_f64();
                let tps = (committed - last_committed) as f64 / interval;
                let tail_mb =
                    (total_tail_bytes - last_tailed_bytes) as f64 / 1_048_576.0 / interval;
                let in_flight = i.saturating_sub(tailed);

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
                    committed,
                    tailed,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    in_flight,
                    format!("{:.1}", tail_mb),
                );

                last_tick = now;
                last_committed = committed;
                last_tailed_bytes = total_tail_bytes;
            }

            if start_time.elapsed() > duration {
                break;
            }
        }
    }

    println!(
        "  +-----+--------+------------+------------+------------+----------+----------+------------+----------+"
    );

    // Drain: let the tailer catch up before printing the summary (max 30s).
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while tailed_tx_id.load(Ordering::Acquire) < ledger.last_commit_id()
        && Instant::now() < drain_deadline
    {
        thread::sleep(Duration::from_millis(20));
    }

    running.store(false, Ordering::Relaxed);

    let elapsed = start_time.elapsed();
    let avg_tps = i as f64 / elapsed.as_secs_f64();
    let committed_final = ledger.last_commit_id();
    let tailed_final = tailed_tx_id.load(Ordering::Acquire);
    let stats = global.get_stats();

    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║            TAIL-ONLY LOAD SUMMARY            ║");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration      : {:>10.2}s                 ║",
        elapsed.as_secs_f64()
    );
    println!("  ║  Submitted     : {:>10}                  ║", i);
    println!(
        "  ║  Committed     : {:>10}                  ║",
        committed_final
    );
    println!(
        "  ║  Tailed        : {:>10}                  ║",
        tailed_final
    );
    println!("  ║  Avg TPS       : {:>10.0}                  ║", avg_tps);
    println!(
        "  ║  Tail bytes    : {:>10}                  ║",
        tailed_bytes.load(Ordering::Relaxed)
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
        "  ║  Samples       : {:>10}                  ║",
        stats.count
    );
    println!("  ╚══════════════════════════════════════════════╝");
    println!();
}
