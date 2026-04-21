//! Entry point for a cluster node (leader or follower).
//!
//! Takes a single positional arg — the cluster.toml path — or falls back
//! to the `RODA_CLUSTER_CONFIG` env var, or `./cluster.toml`.

use roda_ledger::cluster::{Cluster, ClusterConfig};
use spdlog::info;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CLUSTER_CONFIG").ok())
        .unwrap_or_else(|| "cluster.toml".to_string())
        .into();

    let cfg = ClusterConfig::from_file(&config_path).map_err(|e| {
        format!(
            "failed to load cluster config from {}: {}",
            config_path.display(),
            e
        )
    })?;

    info!(
        "starting roda-cluster (node_id={}, mode={:?}, peers={}) from {}",
        cfg.node_id,
        cfg.mode,
        cfg.peers.len(),
        config_path.display()
    );

    let cluster = Cluster::new(cfg)?;
    let handles = cluster.run().await?;

    // Wait on any of the three long-running tasks; abort the rest on exit.
    tokio::signal::ctrl_c().await?;
    info!("received Ctrl+C, shutting down cluster");
    handles.abort();
    Ok(())
}
