use clap::Parser;
use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;
use spdlog::Level::Info;
use spdlog::info;

#[derive(Parser, Debug)]
#[command(name = "load", about = "Load generator for roda-ledger")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    account_count: u64,

    #[arg(short, long, default_value_t = 10)]
    duration: u64,
}

fn main() {
    let args = Args::parse();
    let account_count = args.account_count;
    let mut ledger = Ledger::new(LedgerConfig {
        max_accounts: account_count as usize,
        log_level: Info,
        ..LedgerConfig::bench()
    });
    ledger.start().unwrap();

    let start_time = std::time::Instant::now();
    let mut i = 0u64;
    let duration = std::time::Duration::from_secs(args.duration);
    let mut latency_measurer = LatencyMeasurer::new(128);

    loop {
        let _guard = latency_measurer.measure_with_guard();
        let account = 1 + rand::random::<u64>() % account_count;
        ledger.submit(Operation::Deposit {
            account,
            amount: 10000,
            user_ref: 0,
        });

        i += 1;
        if i.is_multiple_of(10000) && start_time.elapsed() > duration {
            break;
        }
    }

    let elapsed = start_time.elapsed();
    let ops_per_sec = i as f64 / elapsed.as_secs_f64();
    info!("{:.2}m ops/s", ops_per_sec / 1_000_000.0);
    info!("latency stats: {}", latency_measurer.format_stats())
}
