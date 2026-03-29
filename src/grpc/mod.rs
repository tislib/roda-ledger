pub mod handler;
pub mod mapping;
pub mod server;

pub use server::GrpcServer;

pub mod proto {
    tonic::include_proto!("roda.ledger.v1");
}
