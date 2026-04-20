//! Combined multi-node bootstrap: brings up the client gRPC service
//! (`roda.ledger.v1`) AND the peer-to-peer Node service
//! (`roda.node.v1`) in the same process, on separate ports.

use crate::cluster::config::ClusterConfig;
use crate::cluster::handler::NodeHandler;
use crate::cluster::proto::node_server::NodeServer;
use crate::grpc::GrpcServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub struct ClusterServer {
    ledger: Arc<Ledger>,
    client_addr: SocketAddr,
    node_addr: SocketAddr,
    cluster: Arc<ClusterConfig>,
}

impl ClusterServer {
    pub fn new(
        ledger: Arc<Ledger>,
        client_addr: SocketAddr,
        node_addr: SocketAddr,
        cluster: Arc<ClusterConfig>,
    ) -> Self {
        Self {
            ledger,
            client_addr,
            node_addr,
            cluster,
        }
    }

    /// Run both gRPC services until the first one fails or a shutdown
    /// signal is received. `tokio::try_join!` propagates the first
    /// error; a clean shutdown resolves both arms with `Ok(())`.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let client_server = GrpcServer::new(self.ledger.clone(), self.client_addr);
        let client_fut = client_server.run();

        let node_handler = NodeHandler::new(self.ledger.clone(), self.cluster.clone());
        let node_addr = self.node_addr;

        info!("Node gRPC (replication) listening on {}", node_addr);

        let node_fut = async move {
            let mut builder = Server::builder().add_service(NodeServer::new(node_handler));

            #[cfg(feature = "grpc")]
            {
                let reflection_service = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(include_bytes!(concat!(
                        env!("OUT_DIR"),
                        "/node_descriptor.bin"
                    )))
                    .build_v1()?;
                builder = builder.add_service(reflection_service);
            }

            builder
                .serve_with_shutdown(node_addr, node_shutdown_signal())
                .await?;

            info!("Node gRPC service shut down cleanly");
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };

        // Run both concurrently; bail out on the first error.
        tokio::try_join!(
            async { client_fut.await.map_err(|e| e.to_string()) },
            async { node_fut.await.map_err(|e| e.to_string()) },
        )?;

        Ok(())
    }
}

/// Matches the shutdown pattern used by the single-node server (SIGINT
/// + SIGTERM).
async fn node_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("node server: received Ctrl+C, shutting down"),
        _ = terminate => info!("node server: received SIGTERM, shutting down"),
    }
}
