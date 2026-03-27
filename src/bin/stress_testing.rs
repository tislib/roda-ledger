use roda_ledger::testing::stress::scenarios::account_scale::AccountScaleScenario;
use roda_ledger::testing::stress::scenarios::hot_account_contention::HotAccountContentionScenario;
use roda_ledger::testing::stress::scenarios::load_ramp::LoadRampScenario;
use roda_ledger::testing::stress::scenarios::mixed_workload::MixedWorkloadScenario;
use roda_ledger::testing::stress::scenarios::peak::PeakScenario;
use roda_ledger::testing::stress::scenarios::scenario::Scenario;
use roda_ledger::testing::stress::scenarios::snapshot_impact::SnapshotImpactScenario;
use roda_ledger::testing::stress::scenarios::spike::SpikeScenario;
use roda_ledger::testing::stress::scenarios::spike_recovery::SpikeRecoveryScenario;
use roda_ledger::testing::stress::scenarios::sustain_load::SustainLoadScenario;
use roda_ledger::testing::stress::scenarios::wal_growth::WalGrowthScenario;
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;

fn main() {
    let data_dir = Path::new("data");
    if data_dir.exists() && data_dir.is_dir() {
        if fs::read_dir(data_dir)
            .expect("Failed to read data directory")
            .next()
            .is_some()
        {
            panic!("data directory exists and is not empty");
        }
    }

    let args: Vec<String> = env::args().collect();
    let specific_scenario_name = args.get(1);

    // Order matters: sustain_load should be last
    let scenarios: Vec<Box<dyn Scenario>> = vec![
        Box::new(LoadRampScenario::new(
            Duration::from_mins(2),
            0,
            1_000_000,
            100_000,
        )),
        Box::new(PeakScenario::new(
            Duration::from_mins(2),
            10_000_000,
            100_000,
        )),
        Box::new(SpikeScenario::new(Duration::from_secs(21), 100_000)),
        Box::new(SpikeRecoveryScenario::new(Duration::from_mins(2), 100_000)),
        Box::new(AccountScaleScenario::new(
            Duration::from_mins(10),
            100_000_000,
        )),
        Box::new(MixedWorkloadScenario::new(Duration::from_mins(10), 100_000)),
        Box::new(HotAccountContentionScenario::new(
            Duration::from_mins(10),
            10_000_000,
        )),
        Box::new(SnapshotImpactScenario::new(
            Duration::from_mins(10),
            100_000,
        )),
        Box::new(WalGrowthScenario::new(Duration::from_mins(10), 100_000)),
        Box::new(SustainLoadScenario::new(
            Duration::from_mins(20),
            100_000,
            100_000,
        )),
    ];

    if let Some(name) = specific_scenario_name {
        let scenario = scenarios.iter().find(|s| s.name() == *name);
        match scenario {
            Some(s) => {
                println!("Starting scenario: {}", s.name());
                if let Err(e) = s.run() {
                    eprintln!("Scenario {} failed: {}", s.name(), e);
                }
            }
            None => {
                eprintln!("Scenario '{}' not found", name);
                println!("Available scenarios:");
                for s in &scenarios {
                    println!("  {}", s.name());
                }
                std::process::exit(1);
            }
        }
    } else {
        for scenario in &scenarios {
            println!("Starting scenario: {}", scenario.name());
            if let Err(e) = scenario.run() {
                eprintln!("Scenario {} failed: {}", scenario.name(), e);
            }
        }
    }
}
