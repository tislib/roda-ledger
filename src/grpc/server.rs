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

        builder.serve(self.addr).await?;

        Ok(())
    }
}
