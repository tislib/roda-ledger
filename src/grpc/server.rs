use crate::grpc::handler::LedgerHandler;
use crate::grpc::proto::ledger_server::LedgerServer;
use crate::ledger::Ledger;
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

        println!("gRPC server listening on {}", self.addr);

        Server::builder()
            .add_service(LedgerServer::new(handler))
            .serve(self.addr)
            .await?;

        Ok(())
    }
}
