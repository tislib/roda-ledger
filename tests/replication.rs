//! Integration tests for ADR-015 follower-side replication.
//!
//! Each submodule below is a separate file under `tests/replication/`.
//! All submodules compile into one integration-test binary
//! (`cargo test --test replication`).

#[path = "replication/common.rs"]
mod common;

#[path = "replication/validation.rs"]
mod validation;

#[path = "replication/end_to_end.rs"]
mod end_to_end;

#[path = "replication/gating.rs"]
mod gating;

#[path = "replication/leader_follower.rs"]
mod leader_follower;
