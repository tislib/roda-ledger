pub mod balance;
pub mod config;
pub mod dedup;
pub mod ledger;
pub mod pipeline;
mod recover;
pub mod seal;
mod sequencer;
pub mod snapshot;
pub mod tools;
pub mod transaction;
pub mod transactor;
pub mod wait_strategy;
pub mod wal;
pub mod wasm_runtime;

/// Back-compat re-export — the actual implementation now lives in
/// `storage::wal_tail` (the tailer is a storage-layer concern).
pub mod wal_tail {
    pub use storage::wal_tail::*;
}

pub mod index;
