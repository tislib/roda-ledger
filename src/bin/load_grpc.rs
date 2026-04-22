use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::client::LedgerClient;
use roda_ledger::cluster::Term;
use roda_ledger::grpc::GrpcServer;
use roda_ledger::grpc::proto::WaitLevel;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(name = "load", about = "gRPC load generator for roda-ledger")]
struct Args {
    /// Server address as host:port. Used as the bind address when the load tool
    /// spins up its own server, or as the connect target with --external-server.
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: String,

    /// Connect to an already-running server instead of spinning one up in-process.
    #[arg(long, default_value_t = false)]
    external_server: bool,

    /// Max account ID — deposits are randomly distributed over 1..=account_count.
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    /// Test duration in seconds.
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Wait mode: use SubmitBatchAndWait (Committed). Default is fire-and-forget.
    #[arg(short, long, default_value_t = false)]
    wait: bool,

    /// Number of operations per batch.
    #[arg(short = 'b', long, default_value_t = 100)]
    batch_size: usize,

    /// Maximum number of concurrent in-flight batches.
    #[arg(short = 'c', long, default_value_t = 100)]
    concurrency: usize,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let addr: SocketAddr = args.addr.parse()?;

    // Optionally spin up an in-process server.
    let _ledger_guard = if args.external_server {
        println!("Using external server at http://{}", addr);
        None
    } else {
        println!("Starting in-process server on {} ...", addr);
        let cfg = LedgerConfig {
            max_accounts: args.account_count as usize,
            ..LedgerConfig::bench()
        };
        let data_dir = cfg.storage.data_dir.clone();
        let mut ledger = Ledger::new(cfg);
        ledger.start()?;
        let ledger = Arc::new(ledger);
        let server_ledger = ledger.clone();
        let term = Arc::new(Term::open_in_dir(&data_dir)?);
        tokio::spawn(async move {
            if let Err(e) = GrpcServer::new(server_ledger, addr, term).run().await {
                eprintln!("server task exited with error: {}", e);
            }
        });
        // Give the server a moment to bind the listener.
        tokio::time::sleep(Duration::from_millis(200)).await;
        Some(ledger)
    };

    let url = format!("http://{}", addr);
    println!("Connecting to {} ...", url);
    let client = LedgerClient::connect_url(&url).await?;
    println!(
        "Connected. batch_size={}, concurrency={}, wait={}, duration={}s",
        args.batch_size, args.concurrency, args.wait, args.duration
    );

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);

    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let submitted_ops = Arc::new(AtomicU64::new(0));
    let completed_ops = Arc::new(AtomicU64::new(0));
    let completed_batches = Arc::new(AtomicU64::new(0));
    let failed_batches = Arc::new(AtomicU64::new(0));

    // Cumulative latency measurer for the final summary.
    let global_measurer = Arc::new(Mutex::new(LatencyMeasurer::new(1)));
    // Rolling latency measurer — drained + reset every second for per-tick P50/P99.
    let rolling_measurer = Arc::new(Mutex::new(LatencyMeasurer::new(1)));

    let stop = Arc::new(AtomicBool::new(false));

    // Table header
    println!();
    println!("  +-----+--------+------------+------------+----------+----------+------------+");
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} |",
        "#", "time", "TPS", "ops", "P50", "P99", "in-flight"
    );
    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    // Stats printer — ticks every second.
    let printer_handle = {
        let completed_ops = completed_ops.clone();
        let rolling_measurer = rolling_measurer.clone();
        let stop = stop.clone();
        let client = client.clone();

        tokio::spawn(async move {
            let mut second: u32 = 0;
            let mut last_completed: u64 = 0;
            let mut last_tick = Instant::now();

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                second += 1;

                let now = Instant::now();
                let interval = now.duration_since(last_tick).as_secs_f64();
                let completed = completed_ops.load(Ordering::Relaxed);
                let delta = completed.saturating_sub(last_completed);
                let tps = delta as f64 / interval;

                // In-flight messages = compute_index - commit_index
                // (txs that have been processed by the Transactor but not yet flushed to WAL).
                let inflight = match client.get_pipeline_index().await {
                    Ok(idx) => idx.compute.saturating_sub(idx.commit),
                    Err(_) => 0,
                };

                let stats = {
                    let mut m = rolling_measurer.lock().unwrap();
                    let s = m.get_stats();
                    m.reset();
                    s
                };

                println!(
                    "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>8} | {:>8} | {:>10} |",
                    second,
                    start_time.elapsed().as_secs(),
                    format!("{:.0}", tps),
                    completed,
                    fmt_ns(stats.p50),
                    fmt_ns(stats.p99),
                    inflight,
                );

                last_tick = now;
                last_completed = completed;
            }
        })
    };

    // Main submission loop — runs until the configured duration elapses.
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let wait_mode = args.wait;
    let batch_size = args.batch_size;
    let account_count = args.account_count;

    while start_time.elapsed() < duration {
        let permit = semaphore.clone().acquire_owned().await?;

        let batch: Vec<(u64, u64, u64)> = (0..batch_size)
            .map(|_| {
                let account = 1 + rand::random::<u64>() % account_count;
                (account, 10_000u64, 0u64)
            })
            .collect();

        let client = client.clone();
        let submitted_ops = submitted_ops.clone();
        let completed_ops = completed_ops.clone();
        let completed_batches = completed_batches.clone();
        let failed_batches = failed_batches.clone();
        let global_measurer = global_measurer.clone();
        let rolling_measurer = rolling_measurer.clone();

        submitted_ops.fetch_add(batch_size as u64, Ordering::Relaxed);

        let handle = tokio::spawn(async move {
            let t0 = Instant::now();
            let result = if wait_mode {
                client
                    .deposit_batch_and_wait(&batch, WaitLevel::Committed)
                    .await
                    .map(|_| ())
            } else {
                client.deposit_batch(&batch).await.map(|_| ())
            };
            let elapsed = t0.elapsed();

            match result {
                Ok(()) => {
                    completed_ops.fetch_add(batch_size as u64, Ordering::Relaxed);
                    completed_batches.fetch_add(1, Ordering::Relaxed);
                    global_measurer.lock().unwrap().measure(elapsed);
                    rolling_measurer.lock().unwrap().measure(elapsed);
                }
                Err(e) => {
                    failed_batches.fetch_add(1, Ordering::Relaxed);
                    eprintln!("batch failed: {}", e);
                }
            }

            drop(permit);
        });
        handles.push(handle);
    }

    // Drain remaining in-flight batches.
    for h in handles {
        let _ = h.await;
    }

    // Stop the printer.
    stop.store(true, Ordering::Relaxed);
    let _ = printer_handle.await;

    println!("  +-----+--------+------------+------------+----------+----------+------------+");

    // Final summary.
    let elapsed = start_time.elapsed();
    let submitted = submitted_ops.load(Ordering::Relaxed);
    let completed = completed_ops.load(Ordering::Relaxed);
    let failed = failed_batches.load(Ordering::Relaxed);
    let batches_done = completed_batches.load(Ordering::Relaxed);
    let avg_tps = completed as f64 / elapsed.as_secs_f64();
    let stats = global_measurer.lock().unwrap().get_stats();

    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║              LOAD TEST SUMMARY               ║");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Duration        : {:>10.2}s               ║",
        elapsed.as_secs_f64()
    );
    println!("  ║  Submitted ops   : {:>10}                ║", submitted);
    println!("  ║  Completed ops   : {:>10}                ║", completed);
    println!(
        "  ║  Batches ok/fail : {:>10}                ║",
        format!("{}/{}", batches_done, failed)
    );
    println!("  ║  Avg TPS (done)  : {:>10.0}                ║", avg_tps);
    println!("  ║  Batch size      : {:>10}                ║", batch_size);
    println!(
        "  ║  Concurrency     : {:>10}                ║",
        args.concurrency
    );
    println!(
        "  ║  Mode            : {:>10}                ║",
        if wait_mode { "wait" } else { "async" }
    );
    println!(
        "  ║  Server          : {:>10}                ║",
        if args.external_server {
            "external"
        } else {
            "in-process"
        }
    );
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Batch P50       : {:>10}                ║",
        fmt_ns(stats.p50)
    );
    println!(
        "  ║  Batch P99       : {:>10}                ║",
        fmt_ns(stats.p99)
    );
    println!(
        "  ║  Batch P999      : {:>10}                ║",
        fmt_ns(stats.p999)
    );
    println!(
        "  ║  Batch Min       : {:>10}                ║",
        fmt_ns(stats.min)
    );
    println!(
        "  ║  Batch Max       : {:>10}                ║",
        fmt_ns(stats.max)
    );
    println!(
        "  ║  Samples         : {:>10}                ║",
        stats.count
    );
    println!("  ╚══════════════════════════════════════════════╝");
    println!();

    Ok(())
}
