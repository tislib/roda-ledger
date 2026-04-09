use crate::grpc::handler::LedgerHandler;
use crate::grpc::proto::ledger_server::LedgerServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub struct GrpcServer {
    ledger: Arc<Ledger>,
    addr: SocketAddr,
}

impl GrpcServer {
    pub fn new(ledger: Arc<Ledger>, addr: SocketAddr) -> Self {
        Self { ledger, addr }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = LedgerHandler::new(self.ledger);

        info!("gRPC server listening on {}", self.addr);

        let mut builder = Server::builder().add_service(LedgerServer::new(handler));

        #[cfg(feature = "grpc")]
        {
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(include_bytes!(concat!(
                    env!("OUT_DIR"),
                    "/ledger_descriptor.bin"
                )))
                .build_v1()?;
            builder = builder.add_service(reflection_service);
        }

        builder
            .serve_with_shutdown(self.addr, shutdown_signal())
            .await?;

        info!("gRPC server shut down cleanly");

        Ok(())
    }
}

/// Resolves when the process receives SIGINT (Ctrl+C) or SIGTERM (`docker
/// stop`). Required because we run as PID 1 in the container, which does NOT
/// install default signal handlers — without this, signals are silently
/// dropped and the container hangs until Docker's grace period expires.
async fn shutdown_signal() {
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
        _ = ctrl_c => info!("received Ctrl+C, shutting down"),
        _ = terminate => info!("received SIGTERM, shutting down"),
    }
}
