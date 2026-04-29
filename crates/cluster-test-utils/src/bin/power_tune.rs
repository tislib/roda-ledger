use roda_ledger::ledger::{LedgerConfig, WaitStrategy};
use roda_ledger::testing::power_tune::{TuneParameter, TuningStrategy, run_power_tune};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    // Help message
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: power-tune [TOTAL_TXS] [PARAMETER] [MIN] [MAX] [STRATEGY] [DEF_QS] [DEF_SPIN] [DEF_YIELD]"
        );
        println!("Parameters: queue_size (default), spin_until, yield_until");
        println!("Strategies: power2 (default), binary");
        println!("Example: power-tune 5000000 queue_size 16 10000 power2 1024 0 10000");
        return;
    }

    let total_txs = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000);
    let parameter = match args.get(2).map(|s| s.as_str()) {
        Some("spin_until") => TuneParameter::SpinUntil,
        Some("yield_until") => TuneParameter::YieldUntil,
        _ => TuneParameter::QueueSize,
    };

    let (default_min, default_max) = match parameter {
        TuneParameter::QueueSize => (16, 10_000),
        TuneParameter::SpinUntil => (0, 1_000_000),
        TuneParameter::YieldUntil => (0, 1_000_000),
    };

    let min_val = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_min);
    let max_val = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_max);

    let strategy = match args.get(5).map(|s| s.as_str()) {
        Some("binary") => TuningStrategy::BinarySearch,
        _ => TuningStrategy::PowerOfTwo,
    };

    let def_qs = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(2048);
    let def_spin = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(32);
    let def_yield = args.get(8).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let mut base_config = LedgerConfig::temp();
    base_config.log_level = spdlog::Level::Critical;
    base_config.queue_size = def_qs;
    base_config.wait_strategy = WaitStrategy::Custom {
        spin_until: def_spin,
        yield_until: def_yield,
    };

    println!("Starting power_tune with {} transactions", total_txs);
    println!(
        "Tuning {:?} in range [{}..={}] using {:?} strategy...",
        parameter, min_val, max_val, strategy
    );
    println!(
        "Defaults: queue_size={}, spin_until={}, yield_until={}",
        def_qs, def_spin, def_yield
    );

    let results = run_power_tune(
        total_txs,
        parameter,
        min_val,
        max_val,
        strategy,
        base_config,
    );

    println!("\nResults (sorted by TPS):");
    for res in &results {
        println!("{:?}: {}, TPS: {:.2}", parameter, res.value, res.tps);
    }

    if !results.is_empty() {
        println!("\nBest {:?}: {}", parameter, results[0].value);
    }
}
