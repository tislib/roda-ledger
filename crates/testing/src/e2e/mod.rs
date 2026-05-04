//! E2E test framework + scenarios — see ADR-012.
//!
//! Harness modules expose backends, the test context, profile loading,
//! and DSL macros. Scenario modules (`correctness`, `crash`, `wasm`) hold
//! the `async fn` bodies that the `e2e` binary registers as libtest-mimic
//! trials.

// ── harness ──────────────────────────────────────────────────────────────
pub mod backend;
pub mod backend_inline;
pub mod backend_process;
pub mod context;
#[macro_use]
pub mod macros;
pub mod matrix_grid;
pub mod profile;

pub use backend::E2EBackend;
pub use context::E2EContext;
pub use profile::{Profile, profile};

// ── scenarios ────────────────────────────────────────────────────────────
pub mod correctness;
pub mod crash;
pub mod wasm;
