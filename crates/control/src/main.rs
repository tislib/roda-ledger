//! `control` binary — starts the in-memory control plane mock.
//!
//! Defaults: listens on `0.0.0.0:50051`, seeds 5 nodes. SIGINT/SIGTERM
//! triggers cooperative shutdown — the background task drains, the HTTP
//! server stops accepting new connections, the process exits 0.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use control::{server, state::InMemoryState};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "control", about = "roda-ledger control plane mock")]
struct Cli {
    /// Listen address.
    #[arg(long, default_value = "0.0.0.0:50051")]
    addr: SocketAddr,

    /// Initial node count for the mock cluster.
    #[arg(long, default_value_t = 5)]
    seed_nodes: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("control=info,tonic=info,tower_http=info")
        }))
        .init();

    let cli = Cli::parse();
    info!("control mock starting (addr={}, seed_nodes={})", cli.addr, cli.seed_nodes);

    let state = Arc::new(RwLock::new(InMemoryState::new(cli.seed_nodes)));
    let shutdown = CancellationToken::new();

    // Spawn the periodic background task.
    let bg = control::background::spawn(state.clone(), shutdown.clone());

    // Wire SIGINT / SIGTERM to the cancellation token.
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("received SIGINT; initiating shutdown");
                signal_shutdown.cancel();
            }
            Err(e) => {
                tracing::warn!("failed to install SIGINT handler: {e}");
            }
        }
    });

    server::serve(cli.addr, state.clone(), shutdown.clone()).await?;
    shutdown.cancel();

    // Cooperatively await the background task — RAII rule.
    let _ = bg.await;

    info!("control mock stopped cleanly");
    Ok(())
}
