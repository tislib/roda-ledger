use roda_ledger::grpc::{GrpcServer, ServerConfig};
use roda_ledger::ledger::Ledger;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Config path precedence: CLI arg > RODA_CONFIG env var > ./config.toml
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CONFIG").ok())
        .unwrap_or_else(|| "config.toml".to_string())
        .into();

    let server_config = ServerConfig::from_file(&config_path).map_err(|e| {
        format!(
            "failed to load config from {}: {}",
            config_path.display(),
            e
        )
    })?;

    let grpc_addr = server_config.server.socket_addr()?;

    println!(
        "Starting roda-ledger with config from {}: {:?}",
        config_path.display(),
        server_config
    );

    let mut ledger = Ledger::new(server_config.ledger);
    ledger.start().unwrap();

    let ledger_arc = Arc::new(ledger);
    let server = GrpcServer::new(ledger_arc, grpc_addr);

    server.run().await?;

    Ok(())
}
