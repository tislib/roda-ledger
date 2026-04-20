//! Multi-node `cluster` binary (ADR-015).
//!
//! Boots a single Ledger instance and mounts two gRPC services on
//! separate ports:
//!   * `roda.ledger.v1` — client API (50051 by default).
//!   * `roda.node.v1`   — peer-to-peer replication API (50052 by default).
//!
//! The client binary (`roda-ledger` / `src/bin/server.rs`) is left
//! untouched — it continues to serve single-node deployments.

use roda_ledger::cluster::{ClusterServer, ClusterServerConfig};
use roda_ledger::ledger::Ledger;
use spdlog::info;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Config path precedence: CLI arg > RODA_CLUSTER_CONFIG env > ./cluster.toml
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CLUSTER_CONFIG").ok())
        .unwrap_or_else(|| "cluster.toml".to_string())
        .into();

    let cluster_config = ClusterServerConfig::from_file(&config_path)
        .map_err(|e| {
            format!(
                "failed to load cluster config from {}: {}",
                config_path.display(),
                e
            )
        })?
        .finalize();

    let client_addr = cluster_config.server.socket_addr()?;
    let node_addr = cluster_config.cluster.socket_addr()?;

    info!(
        "Starting roda-ledger cluster node with config from {}: {:?}",
        config_path.display(),
        cluster_config
    );

    let mut ledger = Ledger::new(cluster_config.ledger);
    ledger.start()?;
    let ledger_arc = Arc::new(ledger);

    let server = ClusterServer::new(
        ledger_arc,
        client_addr,
        node_addr,
        Arc::new(cluster_config.cluster),
    );

    server.run().await?;

    Ok(())
}
