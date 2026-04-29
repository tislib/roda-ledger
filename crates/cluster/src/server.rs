//! gRPC server runtimes owned by the cluster.
//!
//! - [`Server`] hosts the client-facing `Ledger` service ([`LedgerHandler`])
//!   on leaders (writable) and followers (read-only). A "single node" runs
//!   the leader variant against a zero-peer cluster.
//! - [`NodeServerRuntime`] hosts the peer-facing `Node` service
//!   ([`NodeHandler`]) that implements `AppendEntries`/`Ping`.
//!
//! Both servers use `serve_with_shutdown` driven by an `Arc<Notify>` owned by
//! the corresponding `*Handles` struct. Dropping the handles fires
//! `notify_waiters()`, which lets tonic stop accepting new connections, drain
//! in-flight handlers, and exit cleanly.

use crate::ledger_handler::LedgerHandler;
use crate::node_handler::NodeHandler;
use ::proto::ledger::ledger_server::LedgerServer;
use ::proto::node::node_server::NodeServer;
use crate::raft::{RoleFlag, Term};
use crate::{ClusterCommitIndex, LedgerSlot};
use spdlog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::transport::Server as TonicServer;

// ── Client-facing Ledger server ─────────────────────────────────────────────

pub struct Server {
    /// Indirection to the live `Arc<Ledger>` (ADR-0016 §9).
    ledger_slot: Arc<LedgerSlot>,
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
    /// Cooperative shutdown trigger. When the owning `*Handles`
    /// drops, it calls `notify_waiters()` and `serve_with_shutdown`
    /// resolves.
    shutdown: Arc<Notify>,
}

impl Server {
    pub fn new(
        ledger_slot: Arc<LedgerSlot>,
        addr: SocketAddr,
        role: Arc<RoleFlag>,
        term: Arc<Term>,
        cluster_commit_index: Arc<ClusterCommitIndex>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            ledger_slot,
            addr,
            role,
            term,
            cluster_commit_index,
            shutdown,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = LedgerHandler::new(
            self.ledger_slot,
            self.role.clone(),
            self.term,
            self.cluster_commit_index,
        );

        info!("Ledger gRPC server listening on {}", self.addr);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::ledger::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        let shutdown = self.shutdown.clone();
        TonicServer::builder()
            .add_service(LedgerServer::new(handler))
            .add_service(reflection_service)
            .serve_with_shutdown(self.addr, async move {
                shutdown.notified().await;
            })
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
    shutdown: Arc<Notify>,
}

impl NodeServerRuntime {
    /// `max_message_bytes` bounds both inbound `AppendEntries` decoding and
    /// outbound response encoding. Must be at least as large as the leader's
    /// `append_entries_max_bytes` + protobuf framing overhead.
    pub fn new(
        addr: SocketAddr,
        handler: NodeHandler,
        max_message_bytes: usize,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            addr,
            handler,
            max_message_bytes,
            shutdown,
        }
    }

    fn service(handler: NodeHandler, max_bytes: usize) -> NodeServer<NodeHandler> {
        NodeServer::new(handler)
            .max_decoding_message_size(max_bytes)
            .max_encoding_message_size(max_bytes)
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Node gRPC server listening on {}", self.addr);
        let shutdown = self.shutdown.clone();
        TonicServer::builder()
            .add_service(Self::service(self.handler, self.max_message_bytes))
            .serve_with_shutdown(self.addr, async move {
                shutdown.notified().await;
            })
            .await?;
        info!("Node gRPC server shut down cleanly");
        Ok(())
    }
}
