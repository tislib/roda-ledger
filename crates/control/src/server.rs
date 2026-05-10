//! Tonic server with gRPC-Web support so browsers using
//! `@connectrpc/connect-web`'s `createGrpcWebTransport` can talk to it.

use std::net::SocketAddr;
use std::sync::Arc;

use proto::control::control_server::ControlServer;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::cluster_handle::ClusterHandle;
use crate::event_store::EventStore;
use crate::service::ControlService;

pub async fn serve(
    addr: SocketAddr,
    handle: Arc<ClusterHandle>,
    events: Arc<EventStore>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let service = ControlService::new(handle, events);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::control::FILE_DESCRIPTOR_SET)
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
        .add_service(tonic_web::enable(reflection))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
            info!("control server shutdown signal received");
        })
        .await?;

    Ok(())
}
