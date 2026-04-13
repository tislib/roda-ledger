//! E2E backend abstraction.
//!
//! Controls how roda-ledger nodes are started and managed during a test run.
//! Selected at runtime via `E2E_BACKEND` env var (default: `Inline`).

/// Backend determines the execution environment for E2E test nodes.
///
/// The same test scenario runs against any backend without modification.
/// Backend is specified at runtime — via `E2E_BACKEND` environment variable —
/// not hardcoded in the test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum E2EBackend {
    /// Nodes run in-process alongside the test.
    /// Fastest startup, easiest debugging.
    /// For local development and bug reproduction only.
    Inline,

    /// Each node runs as a separate OS process on the same machine as the test.
    /// Default mode — fast setup, no Docker required, suitable for CI.
    Process,

    /// Each node runs in a Docker container on the same machine.
    /// Closer to production — real network stack, real filesystem isolation.
    Docker,

    /// Each node runs on a separate physical/cloud server.
    /// Planned for future use — not implemented.
    /// Intended for Raft testing under real network conditions.
    Cloud,
}

impl E2EBackend {
    /// Read the backend from `E2E_BACKEND` env var. Defaults to `Inline`.
    pub fn from_env() -> Self {
        match std::env::var("E2E_BACKEND").as_deref() {
            Ok("inline") | Ok("Inline") => E2EBackend::Inline,
            Ok("process") | Ok("Process") => E2EBackend::Process,
            Ok("docker") | Ok("Docker") => E2EBackend::Docker,
            Ok("cloud") | Ok("Cloud") => E2EBackend::Cloud,
            Ok(other) => panic!("unknown E2E_BACKEND: {other}"),
            Err(_) => E2EBackend::Inline,
        }
    }
}
