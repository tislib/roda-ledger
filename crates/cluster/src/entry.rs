//! Reusable entry point shared by `roda-server` and the e2e cluster-process bin.

use crate::Config;
use crate::node::ClusterNode;
use signal_hook::consts::SIGINT;
#[cfg(unix)]
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;
use spdlog::info;
use std::path::Path;

pub fn run(config_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    // Install before spawning workers so a fast signal is queued, not lost.
    let mut signals = install_shutdown_signals()?;

    let cluster = ClusterNode::new(cfg)?;
    let handles = cluster.run()?;

    wait_for_shutdown(&mut signals);
    info!("received shutdown signal, draining cluster");
    drop(handles);
    Ok(())
}

#[cfg(unix)]
fn install_shutdown_signals() -> std::io::Result<Signals> {
    Signals::new([SIGINT, SIGTERM])
}

#[cfg(not(unix))]
fn install_shutdown_signals() -> std::io::Result<Signals> {
    Signals::new([SIGINT])
}

fn wait_for_shutdown(signals: &mut Signals) {
    for sig in signals.forever() {
        match sig {
            SIGINT => {
                info!("received Ctrl+C");
                return;
            }
            #[cfg(unix)]
            SIGTERM => {
                info!("received SIGTERM");
                return;
            }
            other => info!("ignoring signal {other}"),
        }
    }
}
