//! Tonic server with gRPC-Web support so browsers using
//! `@connectrpc/connect-web`'s `createGrpcWebTransport` can talk to it.
//!
//! Hosts both the operational `Control` surface and a transparent
//! `Ledger` proxy that forwards data-plane RPCs to the live cluster
//! — so a browser can reach the ledger through the same gRPC-Web
//! origin it already uses for control RPCs.

use std::net::SocketAddr;
use std::sync::Arc;

use proto::control::control_server::ControlServer;
use proto::ledger::ledger_server::LedgerServer;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::cluster_handle::ClusterHandle;
use crate::event_store::EventStore;
use crate::ledger_proxy::LedgerProxy;
use crate::service::ControlService;

pub async fn serve(
    addr: SocketAddr,
    handle: Arc<ClusterHandle>,
    events: Arc<EventStore>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let service = ControlService::new(handle.clone(), events);
    let proxy = LedgerProxy::new(handle.clone());

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::control::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(proto::ledger::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    info!("control server listening on {addr}");

    // gRPC-Web is enabled per-service via `tonic_web::enable`. This
    // produces a tower service whose body type works with tower-http's
    // CORS layer (the `Default`-bound problem otherwise prevents using
    // `Server::builder().layer(CorsLayer)` with `GrpcWebLayer`).
    TonicServer::builder()
        .accept_http1(true)
        .layer(CorsLayer::permissive())
        .add_service(tonic_web::enable(ControlServer::new(service)))
        .add_service(tonic_web::enable(LedgerServer::new(proxy)))
        .add_service(tonic_web::enable(reflection))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
            info!("control server shutdown signal received");
        })
        .await?;

    Ok(())
}
