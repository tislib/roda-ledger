//! Entry point for the `roda-server` binary.
//!
//! Per ADR-015 a "single node" is just a cluster with zero peers, so this
//! binary unconditionally loads a [`cluster::Config`] and dispatches to
//! the leader or follower bring-up path.
//!
//! Config path precedence: CLI arg > `RODA_CONFIG` env > `RODA_CLUSTER_CONFIG`
//! env (legacy) > `./config.toml`.

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

    cluster::run(&config_path).await
}
