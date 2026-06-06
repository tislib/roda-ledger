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
    let mut cfg = Config::from_file(config_path).map_err(|e| {
        format!(
            "failed to load config from {}: {}",
            config_path.display(),
            e
        )
    })?;

    // The TOML schema doesn't carry `log_level` (it's `#[serde(skip)]`
    // on `LedgerConfig`); the e2e harness drives the default through
    // `RODA_LOG_LEVEL` so spawned servers default to debug under
    // `roda-scenario`. `Ledger::new` is what actually calls
    // `set_level_filter`, so overriding the config field here is
    // sufficient.
    if let Some(level) = std::env::var("RODA_LOG_LEVEL")
        .ok()
        .and_then(|s| parse_log_level(&s))
    {
        cfg.ledger.log_level = level;
    }

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

fn parse_log_level(s: &str) -> Option<spdlog::Level> {
    match s.to_ascii_lowercase().as_str() {
        "trace" => Some(spdlog::Level::Trace),
        "debug" => Some(spdlog::Level::Debug),
        "info" => Some(spdlog::Level::Info),
        "warn" | "warning" => Some(spdlog::Level::Warn),
        "error" => Some(spdlog::Level::Error),
        "critical" | "crit" => Some(spdlog::Level::Critical),
        _ => None,
    }
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
