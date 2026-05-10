//! `control` binary — starts the operational control plane and an
//! in-process Ledger gRPC proxy that forwards calls to the provisioned
//! cluster peers.
//!
//! Defaults: listens on `0.0.0.0:50051`. Peers are provided via
//! repeated `--peer` flags (each `node_id=URL`) or via the `RODA_PEERS`
//! environment variable (comma-separated `node_id=URL` pairs). When no
//! peers are provisioned the server still runs with a mock cluster
//! membership but the Ledger proxy is disabled.
//!
//! SIGINT/SIGTERM triggers cooperative shutdown — the background task
//! drains, the HTTP server stops accepting new connections, the process
//! exits 0.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use control::{LedgerProxy, server, state::InMemoryState};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "control", about = "roda-ledger control plane")]
struct Cli {
    /// Listen address.
    #[arg(long, default_value = "0.0.0.0:50051")]
    addr: SocketAddr,

    /// Initial node count for the in-memory mock cluster. Used only
    /// when no `--peer` flags / `RODA_PEERS` are provided — the
    /// monitoring surface seeds N synthetic nodes so the UI has
    /// something to render in offline demos.
    #[arg(long, default_value_t = 5)]
    seed_nodes: u32,

    /// Provisioned cluster peer. Repeat once per peer. Format
    /// `node_id=URL`, for example `--peer 1=http://127.0.0.1:50051`.
    #[arg(long = "peer", value_parser = parse_peer_arg)]
    peers: Vec<(u64, String)>,
}

fn parse_peer_arg(raw: &str) -> Result<(u64, String), String> {
    let (id_str, url) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected node_id=URL, got '{raw}'"))?;
    let id: u64 = id_str
        .trim()
        .parse()
        .map_err(|_| format!("'{id_str}' is not a u64 node_id"))?;
    let url = url.trim().to_string();
    if url.is_empty() {
        return Err(format!("peer URL must not be empty (got '{raw}')"));
    }
    Ok((id, url))
}

fn peers_from_env() -> Vec<(u64, String)> {
    match env::var("RODA_PEERS") {
        Ok(s) => s
            .split(',')
            .map(|p| p.trim())
            .filter(|p| !p.is_empty())
            .filter_map(|p| match parse_peer_arg(p) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("RODA_PEERS skipping '{p}': {e}");
                    None
                }
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("control=info,tonic=info,tower_http=info")),
        )
        .init();

    let mut cli = Cli::parse();
    if cli.peers.is_empty() {
        cli.peers = peers_from_env();
    }
    info!(
        "control plane starting (addr={}, seed_nodes={}, peers={})",
        cli.addr,
        cli.seed_nodes,
        cli.peers.len()
    );

    let (state, proxy) = if cli.peers.is_empty() {
        warn!(
            "no peers provisioned — Ledger proxy disabled. Pass --peer node_id=URL or set RODA_PEERS to enable."
        );
        let state = Arc::new(RwLock::new(InMemoryState::new(cli.seed_nodes)));
        (state, None)
    } else {
        for (id, url) in &cli.peers {
            info!("ledger proxy peer: node_id={id} url={url}");
        }
        let state = Arc::new(RwLock::new(InMemoryState::from_peers(&cli.peers)));
        let proxy = LedgerProxy::new(cli.peers.clone())
            .map_err(|e| anyhow::anyhow!("failed to build Ledger proxy ClusterClient: {e}"))?;
        (state, Some(proxy))
    };

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

    server::serve(cli.addr, state.clone(), proxy, shutdown.clone()).await?;
    shutdown.cancel();

    // Cooperatively await the background task — RAII rule.
    let _ = bg.await;

    info!("control plane stopped cleanly");
    Ok(())
}
