//! Reusable entry point shared by `roda-server` and the e2e cluster-process bin.

use crate::Config;
use crate::node::ClusterNode;
use spdlog::info;
use std::path::Path;

pub async fn run(config_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::from_file(config_path).map_err(|e| {
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

    shutdown_signal().await;
    info!("received shutdown signal, draining cluster");
    drop(handles);
    Ok(())
}

/// Resolves when the process receives SIGINT (Ctrl+C) or SIGTERM
/// (`docker stop`). Both are installed because PID-1 in a container
/// gets no default signal handlers.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl+C"),
        _ = terminate => info!("received SIGTERM"),
    }
}
