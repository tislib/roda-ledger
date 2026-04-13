//! Inline backend — nodes run in-process with a gRPC server each.
//!
//! Each node boots a real `Ledger`, wraps it in a `GrpcServer`, and
//! exposes a `LedgerClient` for the test to talk to. Fastest startup,
//! easiest debugging, but cannot test crash recovery or process isolation.

use roda_ledger::grpc::proto::ledger_client::LedgerClient;
use roda_ledger::grpc::server::GrpcServer;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tonic::transport::Channel;

/// Handle for a single in-process ledger node with gRPC front-end.
pub struct InlineNode {
    /// Keeps the ledger alive — dropped last to ensure clean shutdown.
    _ledger: Arc<Ledger>,
    /// The spawned tokio task running `GrpcServer::run()`.
    server_task: JoinHandle<()>,
    /// gRPC client connected to this node. `Clone`-able (shares connection).
    client: LedgerClient<Channel>,
    /// Listen address (for diagnostics / logging).
    pub addr: SocketAddr,
}

impl InlineNode {
    /// Boot one inline node: start ledger, spawn gRPC server, connect client.
    pub async fn start() -> Self {
        let config = LedgerConfig {
            seal_check_internal: std::time::Duration::from_millis(10),
            ..LedgerConfig::temp()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().expect("failed to start inline ledger node");
        let ledger = Arc::new(ledger);

        // Find a free port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Spawn gRPC server in background.
        let server_ledger = ledger.clone();
        let server_task = tokio::spawn(async move {
            let server = GrpcServer::new(server_ledger, addr);
            server.run().await.unwrap();
        });

        // Give the server a moment to bind.
        sleep(Duration::from_millis(100)).await;

        // Connect gRPC client.
        let client = LedgerClient::connect(format!("http://{}", addr))
            .await
            .expect("failed to connect gRPC client to inline node");

        Self {
            _ledger: ledger,
            server_task,
            client,
            addr,
        }
    }

    /// Get a clone of the gRPC client (cheap — shares the underlying channel).
    pub fn client(&self) -> LedgerClient<Channel> {
        self.client.clone()
    }
}

impl Drop for InlineNode {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}
