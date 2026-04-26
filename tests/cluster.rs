//! Cluster integration test binary. All test files under `tests/cluster/`
//! are declared here as submodules so Cargo compiles them into a single
//! integration-test binary (keeping Cargo.toml free of per-test `[[test]]`
//! stanzas).
//!
//! Individual files keep their own `#[cfg(feature = "cluster")]` gates
//! where appropriate.

pub mod manual_replication_test;
pub mod surface_test;

mod cluster {
    pub mod append_entries_prev_check_test;
    pub mod basic_test;
    pub mod client_test;
    pub mod divergence_reseed_test;
    pub mod election_test;
    pub mod grpc_test;
    pub mod standalone_test;
    pub mod sync_submit_test;
    pub mod term_behavior_test;
}
