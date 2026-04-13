//! Process backend — each node runs as a separate OS process.
//!
//! Spawns the `roda-ledger` server binary with a generated config.toml,
//! then connects a gRPC client. Provides real process isolation — suitable
//! for CI and crash-recovery tests.

use crate::e2e::lib::profile::Profile;
use roda_ledger::grpc::proto::ledger_client::LedgerClient;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use tokio::time::{Duration, sleep};
use tonic::transport::Channel;

/// Handle for a single roda-ledger server running as a child process.
pub struct ProcessNode {
    /// The spawned server process.
    child: Child,
    /// gRPC client connected to this node.
    client: LedgerClient<Channel>,
    /// Listen address.
    pub addr: SocketAddr,
    /// Temp data directory (removed on drop).
    data_dir: PathBuf,
}

impl ProcessNode {
    /// Spawn one roda-ledger server process using config from the profile.
    pub async fn start(profile: &Profile) -> Self {
        // -- Temp directory (same pattern as LedgerConfig::temp) ----------------
        let mut data_dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        data_dir.push(format!("temp_{}", rand));
        std::fs::create_dir_all(&data_dir).unwrap();

        // -- Free port ----------------------------------------------------------
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // -- Write config.toml from profile with runtime overrides --------------
        let config_content =
            profile.render_config_toml("127.0.0.1", addr.port(), &data_dir.to_string_lossy());
        let config_path = data_dir.join("config.toml");
        std::fs::write(&config_path, &config_content).unwrap();

        // -- Spawn server binary ------------------------------------------------
        let binary = env!("CARGO_BIN_EXE_roda-ledger");
        let child = Command::new(binary)
            .arg(&config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn roda-ledger binary at {}: {}", binary, e));

        // -- Wait for server to become ready (poll with retries) ----------------
        let client = Self::wait_for_ready(addr).await;

        Self {
            child,
            client,
            addr,
            data_dir,
        }
    }

    /// Poll until the server accepts a gRPC connection, with timeout.
    async fn wait_for_ready(addr: SocketAddr) -> LedgerClient<Channel> {
        let timeout = Duration::from_secs(10);
        let start = tokio::time::Instant::now();
        let url = format!("http://{}", addr);

        loop {
            match LedgerClient::connect(url.clone()).await {
                Ok(client) => return client,
                Err(_) if start.elapsed() < timeout => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!(
                    "process node failed to become ready at {} within {:?}: {}",
                    addr, timeout, e
                ),
            }
        }
    }

    /// Get a clone of the gRPC client (cheap — shares the underlying channel).
    pub fn client(&self) -> LedgerClient<Channel> {
        self.client.clone()
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        // Kill the server process.
        let _ = self.child.kill();
        let _ = self.child.wait();

        // Clean up temp directory (includes config.toml and data files).
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}
