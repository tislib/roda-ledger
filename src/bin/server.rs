use roda_ledger::grpc::GrpcServer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc_addr_str = env::var("RODA_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let grpc_addr: SocketAddr = grpc_addr_str.parse()?;

    let data_dir = env::var("RODA_DATA_DIR").ok();
    let max_accounts = env::var("RODA_MAX_ACCOUNTS")
        .map(|s| s.parse().unwrap_or(1_000_000))
        .unwrap_or(1_000_000);
    let snapshot_interval_secs = env::var("RODA_SNAPSHOT_INTERVAL")
        .map(|s| s.parse().unwrap_or(600))
        .unwrap_or(600);
    let in_memory = env::var("RODA_IN_MEMORY")
        .map(|s| s.parse().unwrap_or(false))
        .unwrap_or(false);

    let config = LedgerConfig {
        max_accounts,
        location: data_dir,
        in_memory,
        snapshot_interval: Duration::from_secs(snapshot_interval_secs),
        ..LedgerConfig::default()
    };

    println!("Starting roda-ledger with config: {:?}", config);

    let mut ledger = Ledger::new(config);
    ledger.start();

    let ledger_arc = Arc::new(ledger);
    let server = GrpcServer::new(ledger_arc, grpc_addr);

    server.run().await?;

    Ok(())
}
