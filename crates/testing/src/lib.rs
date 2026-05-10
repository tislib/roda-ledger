//! roda-ledger testing crate.
//!
//! Two layers live here:
//!
//! - **`scenario`** — pure-Rust scenario primitives (steps, actions,
//!   types). No deps beyond stdlib. Always available.
//! - **`scenarios`** — concrete scenarios built on those primitives,
//!   exposed via [`scenarios::list`] for the runner to enumerate.
//!   Always available.
//! - **`e2e`** (feature `harness`) — the legacy libtest-mimic harness
//!   plus its imperative DSL macros. Compiled only when `harness` is
//!   enabled, so the scenario layer can be consumed by `control`
//!   without dragging in cluster-spawning code.

#![allow(dead_code, unused_imports)]

pub mod scenario;
pub mod scenarios;

#[cfg(feature = "harness")]
#[macro_use]
pub mod e2e;
