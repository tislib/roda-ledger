use roda_ledger::cluster::Term;
use roda_ledger::grpc::{GrpcServer, ServerConfig};
use roda_ledger::ledger::Ledger;
use spdlog::info;
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

    info!(
        "Starting roda-ledger with config from {}: {:?}",
        config_path.display(),
        server_config
    );

    let data_dir = server_config.ledger.storage.data_dir.clone();

    let mut ledger = Ledger::new(server_config.ledger);
    ledger.start().unwrap();

    // Single-node runs are a cluster with zero peers. Open the durable
    // term log under the same data_dir so submit/status responses can
    // stamp a real term rather than a fabricated zero.
    let term = Arc::new(Term::open_in_dir(&data_dir)?);
    let ledger_arc = Arc::new(ledger);
    let server = GrpcServer::new(ledger_arc, grpc_addr, term);

    server.run().await?;

    Ok(())
}
