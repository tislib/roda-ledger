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

#[cfg(feature = "cluster")]
pub mod client;
#[cfg(feature = "cluster")]
pub mod cluster;
mod entries;
pub mod index;
