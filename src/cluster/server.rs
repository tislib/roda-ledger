//! gRPC server runtimes owned by the cluster.
//!
//! - [`Server`] hosts the client-facing `Ledger` service ([`LedgerHandler`])
//!   on leaders (writable) and followers (read-only). A "single node" runs
//!   the leader variant against a zero-peer cluster.
//! - [`NodeServerRuntime`] hosts the peer-facing `Node` service
//!   ([`NodeHandler`]) that implements `AppendEntries`/`Ping`.

use crate::cluster::Term;
use crate::cluster::handler_ledger::LedgerHandler;
use crate::cluster::handler_node::NodeHandler;
use crate::cluster::proto::ledger::ledger_server::LedgerServer;
use crate::cluster::proto::node::node_server::NodeServer;
use crate::ledger::Ledger;
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server as TonicServer;

// ── Client-facing Ledger server ─────────────────────────────────────────────

pub struct Server {
    ledger: Arc<Ledger>,
    addr: SocketAddr,
    read_only: bool,
    /// Shared term state. Every submit/status/wait response stamps the
    /// appropriate view of this value. Callers always supply one — a
    /// "single-node" server is just a cluster with zero peers and its
    /// own durable term log on disk.
    term: Arc<Term>,
}

impl Server {
    pub fn new(ledger: Arc<Ledger>, addr: SocketAddr, term: Arc<Term>) -> Self {
        Self {
            ledger,
            addr,
            read_only: false,
            term,
        }
    }

    /// Build a server where every write RPC returns `FAILED_PRECONDITION`.
    pub fn new_read_only(ledger: Arc<Ledger>, addr: SocketAddr, term: Arc<Term>) -> Self {
        Self {
            ledger,
            addr,
            read_only: true,
            term,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let read_only = self.read_only;

        let handler = if read_only {
            LedgerHandler::new_read_only(self.ledger, self.term)
        } else {
            LedgerHandler::new(self.ledger, self.term)
        };

        info!(
            "Ledger gRPC server listening on {} (read_only={})",
            self.addr, read_only
        );

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(include_bytes!(concat!(
                env!("OUT_DIR"),
                "/ledger_descriptor.bin"
            )))
            .build_v1()?;

        TonicServer::builder()
            .add_service(LedgerServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(self.addr, shutdown_signal())
            .await?;

        info!("Ledger gRPC server shut down cleanly");

        Ok(())
    }
}

// ── Peer-facing Node server ─────────────────────────────────────────────────

pub struct NodeServerRuntime {
    addr: SocketAddr,
    handler: NodeHandler,
    max_message_bytes: usize,
}

impl NodeServerRuntime {
    /// `max_message_bytes` bounds both inbound `AppendEntries` decoding and
    /// outbound response encoding. Must be at least as large as the leader's
    /// `append_entries_max_bytes` + protobuf framing overhead.
    pub fn new(addr: SocketAddr, handler: NodeHandler, max_message_bytes: usize) -> Self {
        Self {
            addr,
            handler,
            max_message_bytes,
        }
    }

    fn service(handler: NodeHandler, max_bytes: usize) -> NodeServer<NodeHandler> {
        NodeServer::new(handler)
            .max_decoding_message_size(max_bytes)
            .max_encoding_message_size(max_bytes)
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Node gRPC server listening on {}", self.addr);
        TonicServer::builder()
            .add_service(Self::service(self.handler, self.max_message_bytes))
            .serve(self.addr)
            .await?;
        Ok(())
    }

    pub async fn run_with_shutdown<F>(
        self,
        shutdown: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Future<Output = ()>,
    {
        info!("Node gRPC server listening on {}", self.addr);
        TonicServer::builder()
            .add_service(Self::service(self.handler, self.max_message_bytes))
            .serve_with_shutdown(self.addr, shutdown)
            .await?;
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
