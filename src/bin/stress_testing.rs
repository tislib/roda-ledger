use roda_ledger::testing::stress::scenarios::sustain_load::SustainLoadScenario;
use roda_ledger::testing::stress::scenarios::scenario::Scenario;
use std::time::Duration;

fn main() {
    let scenario = SustainLoadScenario::new(Duration::from_secs(21), 100_000, 100_000);
    println!("Starting scenario: {}", scenario.name());
    
    if let Err(e) = scenario.run() {
        eprintln!("Scenario failed: {}", e);
    }
}
