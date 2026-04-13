#![allow(dead_code, unused_imports)]
//! E2E test framework internals — see ADR-012.
//!
//! All non-test infrastructure lives here: backend abstraction, context,
//! profile loading, and DSL macros.

pub mod backend;
pub mod backend_inline;
pub mod context;
#[macro_use]
pub mod macros;
pub mod profile;

pub use backend::E2EBackend;
pub use context::E2EContext;
pub use profile::{Profile, profile};
