use roda_ledger::grpc::GrpcServer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use spdlog::Level;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc_addr_str = env::var("RODA_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let grpc_addr: SocketAddr = grpc_addr_str.parse()?;

    let data_dir = env::var("RODA_DATA_DIR").ok();
    let max_accounts = env::var("RODA_MAX_ACCOUNTS")
        .map(|s| s.parse().unwrap_or(1_000_000))
        .unwrap_or(1_000_000);
    // How many WAL segment seals between snapshots (0 = never snapshot)
    let snapshot_frequency = env::var("RODA_SNAPSHOT_FREQUENCY")
        .map(|s| s.parse().unwrap_or(4u32))
        .unwrap_or(4u32);
    let _in_memory = env::var("RODA_IN_MEMORY")
        .map(|s| s.parse().unwrap_or(false))
        .unwrap_or(false);

    let log_level_str = env::var("RODA_LOG_LEVEL").unwrap_or_else(|_| "debug".to_string());
    let log_level = match log_level_str.to_lowercase().as_str() {
        "trace" => Level::Trace,
        "debug" => Level::Debug,
        "info" => Level::Info,
        "warn" => Level::Warn,
        "error" => Level::Error,
        "critical" => Level::Critical,
        _ => Level::Debug,
    };

    let config = LedgerConfig {
        max_accounts,
        log_level,
        storage: StorageConfig {
            data_dir: data_dir.unwrap_or("data/".to_string()),
            snapshot_frequency,
            ..Default::default()
        },
        ..LedgerConfig::default()
    };

    println!("Starting roda-ledger with config: {:?}", config);

    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let ledger_arc = Arc::new(ledger);
    let server = GrpcServer::new(ledger_arc, grpc_addr);

    server.run().await?;

    Ok(())
}
