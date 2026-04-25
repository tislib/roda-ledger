//! Entry point for the `roda-ledger` binary.
//!
//! Per ADR-015 a "single node" is just a cluster with zero peers, so this
//! binary unconditionally loads a [`cluster::Config`] and dispatches to
//! the leader or follower bring-up path.
//!
//! Config path precedence: CLI arg > `RODA_CONFIG` env > `RODA_CLUSTER_CONFIG`
//! env (legacy) > `./config.toml`.

use roda_ledger::cluster::{self, ClusterNode};
use spdlog::info;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CONFIG").ok())
        .or_else(|| env::var("RODA_CLUSTER_CONFIG").ok())
        .unwrap_or_else(|| "config.toml".to_string())
        .into();

    let cfg = cluster::Config::from_file(&config_path).map_err(|e| {
        format!(
            "failed to load config from {}: {}",
            config_path.display(),
            e
        )
    })?;

    info!(
        "starting roda-ledger (node_id={}, cluster_size={}) from {}",
        cfg.node_id(),
        cfg.cluster_size(),
        config_path.display()
    );

    let cluster = ClusterNode::new(cfg)?;
    let handles = cluster.run().await?;

    tokio::signal::ctrl_c().await?;
    info!("received Ctrl+C, shutting down cluster");
    handles.abort();
    Ok(())
}
