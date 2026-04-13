#![allow(unused_imports)]
//! E2E test suite for roda-ledger — see ADR-012.

#[macro_use]
pub mod lib;

pub mod correctness;

pub use lib::{E2EBackend, E2EContext, Profile, profile};
