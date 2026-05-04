//! roda-ledger testing harness. Currently exposes the e2e harness and
//! scenarios under `crate::e2e`; the `e2e` and `e2e-cluster-process`
//! binaries in `src/bin/` consume them.

#![allow(dead_code, unused_imports)]

#[macro_use]
pub mod e2e;
