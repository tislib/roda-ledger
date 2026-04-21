use crate::cluster::client_handler::LedgerHandler;
use crate::cluster::proto::ledger::ledger_server::LedgerServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub struct GrpcServer {
    ledger: Arc<Ledger>,
    addr: SocketAddr,
    read_only: bool,
    term: u64,
}

impl GrpcServer {
    pub fn new(ledger: Arc<Ledger>, addr: SocketAddr) -> Self {
        Self {
            ledger,
            addr,
            read_only: false,
            term: 0,
        }
    }

    /// Build a server where every write RPC returns `FAILED_PRECONDITION`.
    pub fn new_read_only(ledger: Arc<Ledger>, addr: SocketAddr) -> Self {
        Self {
            ledger,
            addr,
            read_only: true,
            term: 0,
        }
    }

    /// Stamp `term` onto every submit response. Call after `new` /
    /// `new_read_only`. `0` keeps the single-node default.
    pub fn with_term(mut self, term: u64) -> Self {
        self.term = term;
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let read_only = self.read_only;
        let term = self.term;
        let handler = if read_only {
            LedgerHandler::new_read_only(self.ledger).with_term(term)
        } else {
            LedgerHandler::new(self.ledger).with_term(term)
        };

        info!(
            "gRPC server listening on {} (read_only={})",
            self.addr, read_only
        );

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
