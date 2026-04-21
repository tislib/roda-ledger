pub mod balance;
pub mod config;
pub mod ctl;
pub mod dedup;
pub mod entities;
pub mod ledger;
pub mod pipeline;
mod recover;
pub mod seal;
mod sequencer;
pub mod snapshot;
pub mod storage;
pub mod testing;
pub mod transaction;
pub mod transactor;
pub mod wait_strategy;
pub mod wal;
pub mod wasm_runtime;

/// Back-compat re-export — the actual implementation now lives in
/// `storage::wal_tail` (the tailer is a storage-layer concern).
pub mod wal_tail {
    pub use crate::storage::wal_tail::*;
}

#[cfg(feature = "grpc")]
pub mod client;
#[cfg(feature = "grpc")]
pub mod cluster;
mod entries;
pub mod index;

/// Back-compat re-export — the gRPC server files were merged into
/// `cluster`. A "single node" is now just a `Cluster` with zero peers.
/// External call sites that still reference `roda_ledger::grpc::*`
/// resolve through this facade.
#[cfg(feature = "grpc")]
pub mod grpc {
    pub use crate::cluster::{
        GrpcServer, GrpcServerSection, LedgerHandler, ServerConfig, ServerConfigError,
    };
    /// Legacy alias for the client-API proto. New code should use
    /// `crate::cluster::proto::ledger` directly.
    pub mod proto {
        pub use crate::cluster::proto::ledger::*;
    }
}
