use crate::ledger::{Ledger, LedgerConfig};
use crate::testing::reporting::{Reporter, RunResult, WorkloadMetrics};
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::testing::stress::scenarios::scenario::Scenario;
use crate::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use crate::wallet::transaction::WalletTransaction;
use std::error::Error;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct SnapshotImpactScenario {
    pub duration: Duration,
    pub accounts: u64,
}

impl SnapshotImpactScenario {
    pub fn new(duration: Duration, accounts: u64) -> Self {
        Self { duration, accounts }
    }
}

impl Scenario for SnapshotImpactScenario {
    fn name(&self) -> String {
        "snapshot_impact".to_string()
    }

    fn duration(&self) -> Duration {
        self.duration
    }

    fn max_accounts(&self) -> u64 {
        self.accounts
    }

    fn run(&self) -> Result<RunResult, Box<dyn Error>> {
        let wal_path = std::path::Path::new("data/wal.bin");
        if wal_path.exists() {
            std::fs::remove_file(wal_path)?;
        }

        // Configure ledger with frequent snapshots
        let config = LedgerConfig {
            max_accounts: self.max_accounts() as usize,
            snapshot_interval: Duration::from_secs(1),
            ..Default::default()
        };

        let mut ledger = Ledger::new(config);
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

    fn execute(
        &self,
        client: DirectWorkloadClient,
        metrics: Arc<WorkloadMetrics>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let mut workload = Workload::new(client).with_metrics(metrics);
        let accounts_size = self.accounts;
        let duration = self.duration;

        let workload_handle = std::thread::spawn(move || {
            let config = RunConfig {
                limit: Limit::Duration(duration),
                power: Power::Full,
            };

            let _ = workload.run(config, |_| {
                let account_id = rand::random::<u64>() % accounts_size;
                WalletTransaction::deposit(account_id, 100)
            });
        });

        Ok(workload_handle)
    }
}
