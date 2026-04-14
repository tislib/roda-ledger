//! Process backend — each node runs as a separate OS process.
//!
//! Spawns the `roda-ledger` server binary with a generated config.toml,
//! then connects a gRPC client. Provides real process isolation — suitable
//! for CI and crash-recovery tests.

use crate::e2e::lib::profile::Profile;
use roda_ledger::client::LedgerClient;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use tokio::time::{Duration, sleep};

/// Handle for a single roda-ledger server running as a child process.
pub struct ProcessNode {
    /// The spawned server process (None after kill, before restart).
    child: Option<Child>,
    /// gRPC client connected to this node.
    client: Option<LedgerClient>,
    /// Listen address (stable across kill/restart — same port reused).
    pub addr: SocketAddr,
    /// Temp data directory (persists across kill/restart, removed on drop).
    data_dir: PathBuf,
    /// Path to config.toml inside data_dir.
    config_path: PathBuf,
}

impl ProcessNode {
    /// Spawn one roda-ledger server process using config from the profile.
    pub async fn start(profile: &Profile) -> Self {
        let mut data_dir = std::env::current_dir().unwrap();
        let rand = rand::random::<u64>() % 1_000_000_000;
        data_dir.push(format!("temp_{}", rand));
        std::fs::create_dir_all(&data_dir).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config_content =
            profile.render_config_toml("127.0.0.1", addr.port(), &data_dir.to_string_lossy());
        let config_path = data_dir.join("config.toml");
        std::fs::write(&config_path, &config_content).unwrap();

        let child = Self::spawn_server(&config_path);
        let client = Self::wait_for_ready(addr).await;

        Self {
            child: Some(child),
            client: Some(client),
            addr,
            data_dir,
            config_path,
        }
    }

    fn spawn_server(config_path: &PathBuf) -> Child {
        let binary = env!("CARGO_BIN_EXE_roda-ledger");
        Command::new(binary)
            .arg(config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn roda-ledger binary at {}: {}", binary, e))
    }

    /// Poll until the server accepts a gRPC connection, with timeout.
    async fn wait_for_ready(addr: SocketAddr) -> LedgerClient {
        let timeout = Duration::from_secs(10);
        let start = tokio::time::Instant::now();

        loop {
            match LedgerClient::connect(addr).await {
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

    /// Get a reference to the client.
    ///
    /// Panics if the node has been killed and not yet restarted.
    pub fn client(&self) -> &LedgerClient {
        self.client
            .as_ref()
            .expect("node is killed — call restart() first")
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.clone()
    }

    /// Kill the server process (SIGKILL). Data directory is preserved.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.client = None;
    }

    /// Restart the server process after a `kill()`. Reuses the same
    /// data directory and config — the server recovers from WAL on startup.
    pub async fn restart(&mut self) {
        assert!(
            self.child.is_none(),
            "restart() called on a running node — call kill() first"
        );

        let child = Self::spawn_server(&self.config_path);
        let client = Self::wait_for_ready(self.addr).await;
        self.child = Some(child);
        self.client = Some(client);
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}
