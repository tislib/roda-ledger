//! gRPC server runtimes owned by the cluster.
//!
//! - [`Server`] hosts the client-facing `Ledger` service ([`LedgerHandler`])
//!   on leaders (writable) and followers (read-only). A "single node" runs
//!   the leader variant against a zero-peer cluster.
//! - [`NodeServerRuntime`] hosts the peer-facing `Node` service
//!   ([`NodeHandler`]) that implements `AppendEntries`/`Ping`.

use crate::cluster::ledger_handler::LedgerHandler;
use crate::cluster::node_handler::NodeHandler;
use crate::cluster::proto::ledger::ledger_server::LedgerServer;
use crate::cluster::proto::node::node_server::NodeServer;
use crate::cluster::{ClusterCommitIndex, RoleFlag, Term};
use crate::ledger::Ledger;
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server as TonicServer;

// ── Client-facing Ledger server ─────────────────────────────────────────────

pub struct Server {
    ledger: Arc<Ledger>,
    addr: SocketAddr,
    /// Shared role state. The constructed [`LedgerHandler`] reads
    /// this on every RPC to decide write/read permissions; the
    /// supervisor flips it on role transitions, so a single
    /// long-lived `Server` instance serves every role without
    /// restart.
    role: Arc<RoleFlag>,
    /// Shared term state. Every submit/status/wait response stamps
    /// the appropriate view of this value.
    term: Arc<Term>,
    pub cluster_commit_index: Arc<ClusterCommitIndex>,
}

impl Server {
    pub fn new(
        ledger: Arc<Ledger>,
        addr: SocketAddr,
        role: Arc<RoleFlag>,
        term: Arc<Term>,
        cluster_commit_index: Arc<ClusterCommitIndex>,
    ) -> Self {
        Self {
            ledger,
            addr,
            role,
            term,
            cluster_commit_index,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = LedgerHandler::new(
            self.ledger,
            self.role.clone(),
            self.term,
            self.cluster_commit_index,
        );

        info!("Ledger gRPC server listening on {}", self.addr);

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
