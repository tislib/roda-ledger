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
    pub mod cluster_client_test;
    pub mod config_test;
    pub mod correctness_test;
    pub mod divergence_extended_test;
    pub mod divergence_reseed_test;
    pub mod edge_case_test;
    pub mod election_extended_test;
    pub mod election_test;
    pub mod fault_test;
    pub mod grpc_test;
    pub mod lifecycle_test;
    pub mod quorum_cluster_test;
    pub mod replication_extended_test;
    pub mod role_transition_test;
    pub mod safety_test;
    pub mod standalone_test;
    pub mod sync_submit_test;
    pub mod term_behavior_test;
    pub mod wait_level_extended_test;
}
