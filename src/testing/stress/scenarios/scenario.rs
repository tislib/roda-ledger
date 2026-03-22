use crate::ledger::Ledger;
use crate::testing::reporting::{Reporter, RunResult, WorkloadMetrics};
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub trait Scenario {
    fn name(&self) -> String;
    fn duration(&self) -> Duration;

    fn execute(&self, client: DirectWorkloadClient, metrics: Arc<WorkloadMetrics>) -> Result<JoinHandle<()>, Box<dyn Error>>;

    fn run(&self) -> Result<RunResult, Box<dyn Error>> {
        let wal_path = std::path::Path::new("data/wal.bin");
        if wal_path.exists() {
            std::fs::remove_file(wal_path)?;
        }

        let mut ledger = Ledger::new(Default::default());
        ledger.start();
        let ledger = Arc::new(ledger);

        let mut reporter = Reporter::new(self.name(), self.duration(), ledger);
        let client = reporter.client();
        let metrics = reporter.metrics();

        let workload_handle = self.execute(client, metrics)?;

        reporter.run_loop();

        let _ = workload_handle.join();

        Ok(reporter.finish())
    }
}
