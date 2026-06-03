//! roda-ledger testing crate.
//!
//! - **`scenario`** — pure-Rust scenario primitives (steps, actions,
//!   types). No deps beyond stdlib.
//! - **`scenarios`** — concrete scenarios built on those primitives,
//!   exposed via [`scenarios::list`] for the runner to enumerate.

#![allow(dead_code, unused_imports)]

pub mod scenario;
pub mod scenarios;
