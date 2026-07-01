//! `control` binary — starts the real control plane backed by a live
//! `roda-server` cluster spawned via `ProcessProvisioner`.
//!
//! Defaults: listens on `0.0.0.0:50051`, auto-provisions a 3-node
//! cluster on boot. SIGINT/SIGTERM triggers cooperative shutdown — the
//! HTTP server stops accepting new connections, the cluster handle is
//! dropped, and `ProcessProvisioner`'s Drop kills child processes and
//! cleans up temp dirs before the process exits.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use control::{ClusterHandle, EventStore, server};
use proto::control::ClusterConfig;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "control", about = "roda-ledger real control plane")]
struct Cli {
    /// gRPC listen address (browser clients hit this via tonic-web).
    #[arg(long, env = "RODA_CONTROL_ADDR", default_value = "0.0.0.0:50051")]
    addr: SocketAddr,

    /// How many roda-server children to spawn on boot. The cluster
    /// can be reshaped at runtime via `SetNodeCount` (which tears
    /// down + reprovisions).
    #[arg(long, env = "INITIAL_NODE_COUNT", default_value_t = 3)]
    initial_node_count: u32,

    /// Path to the `roda-server` binary. Defaults to a sibling of
    /// this binary; override here or via `RODA_SERVER_BIN`.
    #[arg(long, env = "RODA_SERVER_BIN")]
    server_bin: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("control=info,tonic=info,tower_http=info")),
        )
        .init();

    let cli = Cli::parse();

    let server_bin = match resolve_server_bin(cli.server_bin.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            error!("{e}");
            return Err(anyhow::anyhow!(e));
        }
    };

    info!(
        addr = %cli.addr,
        nodes = cli.initial_node_count,
        server_bin = %server_bin.display(),
        "control real plane starting"
    );

    let handle =
        ClusterHandle::bootstrap(server_bin, default_cluster_config(), cli.initial_node_count)
            .await?;
    let events = Arc::new(EventStore::new());
    let shutdown = CancellationToken::new();

    // Wire SIGINT/SIGTERM to the cancellation token so `kill` (containers
    // send SIGTERM) tears the cluster down via RAII instead of orphaning it.
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("received shutdown signal; initiating shutdown");
        signal_shutdown.cancel();
    });

    let serve_result =
        server::serve(cli.addr, handle.clone(), events.clone(), shutdown.clone()).await;
    shutdown.cancel();

    // Drop the handle here so the provisioner tears the cluster down
    // before the runtime finishes (cooperative — RAII).
    drop(handle);

    match serve_result {
        Ok(()) => {
            info!("control plane stopped cleanly");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Resolve on the first SIGINT or SIGTERM so shutdown is cooperative
/// whether the process is Ctrl+C'd or `kill`ed.
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("failed to install SIGTERM handler: {e}");
            return wait_for_ctrl_c().await;
        }
    };
    tokio::select! {
        _ = wait_for_ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    wait_for_ctrl_c().await;
}

/// Never resolve if the handler can't be installed, so a failure can't
/// masquerade as a shutdown request.
async fn wait_for_ctrl_c() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!("failed to install SIGINT handler: {e}");
        std::future::pending::<()>().await;
    }
}

/// Sane defaults so a blank cluster has something coherent to start
/// from. Mirrors the cluster's TOML rendering defaults at
/// `provisioner/process.rs::render_config_toml`.
fn default_cluster_config() -> ClusterConfig {
    ClusterConfig {
        max_accounts: 1_000_000,
        queue_size: 16_384,
        transaction_count_per_segment: 1_000_000,
        snapshot_frequency: 2,
        append_entries_max_bytes: 4 * 1024 * 1024,
    }
}

/// Resolve the path to the `roda-server` binary. CLI arg > env var >
/// sibling of this binary. Mirrors `bin/scenario.rs`'s logic so the
/// two binaries can sit next to each other in `target/release/`.
fn resolve_server_bin(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(p) = explicit {
        if !p.exists() {
            return Err(format!("server-bin {} does not exist", p.display()));
        }
        return Ok(p.to_path_buf());
    }

    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let dir = exe
        .parent()
        .ok_or_else(|| "current_exe has no parent directory".to_string())?;
    let candidate = dir.join(if cfg!(windows) {
        "roda-server.exe"
    } else {
        "roda-server"
    });
    if !candidate.exists() {
        return Err(format!(
            "roda-server binary not found at {}.\n\
             Build it with: cargo build -p cluster --bin roda-server\n\
             Or set --server-bin / RODA_SERVER_BIN to override.",
            candidate.display()
        ));
    }
    Ok(candidate)
}
