//! Server process used by the e2e Process backend.
//!
//! Mirrors the `roda-server` binary in the `cluster` crate, but lives in
//! `testing` so it builds into the same `target/{profile}/` directory as
//! the `e2e` runner. The runner discovers it as a sibling of `current_exe()`.

use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path: PathBuf = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("usage: e2e-cluster-process <config.toml>"))
        .into();

    cluster::run(&config_path).await
}
