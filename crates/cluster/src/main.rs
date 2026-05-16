//! Entry point for the `roda-server` binary.
//!
//! Config path precedence: CLI arg > `RODA_CONFIG` env > `RODA_CLUSTER_CONFIG`
//! env (legacy) > `./config.toml`.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path: PathBuf = env::args()
        .nth(1)
        .or_else(|| env::var("RODA_CONFIG").ok())
        .or_else(|| env::var("RODA_CLUSTER_CONFIG").ok())
        .unwrap_or_else(|| "config.toml".to_string())
        .into();

    cluster::run(&config_path)
}
