//! Inline backend — nodes run in-process via `cluster-test-utils`.
//!
//! Each `InlineNode` owns a `ClusterTestingControl` configured for a single
//! standalone node. Fastest startup, easiest debugging, but cannot exercise
//! crash recovery or real process isolation. For those, use the Process
//! backend.

use crate::e2e::profile::Profile;
use client::NodeClient;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use std::net::SocketAddr;

/// Handle for a single in-process ledger node with gRPC front-end.
pub struct InlineNode {
    /// Owns the running node + data dir. Drop performs cooperative
    /// shutdown via `cluster-test-utils`'s RAII teardown.
    control: ClusterTestingControl,
    /// gRPC client connected to this node.
    client: NodeClient,
    /// Listen address (for diagnostics / logging).
    pub addr: SocketAddr,
}

impl InlineNode {
    /// Boot one inline node using config from the profile.
    pub async fn start(profile: &Profile) -> Self {
        let cfg = ClusterTestingConfig {
            label: format!("e2e_inline_{}", profile.name),
            transaction_count_per_segment: profile.ledger.storage.transaction_count_per_segment,
            snapshot_frequency: profile.ledger.storage.snapshot_frequency,
            ..ClusterTestingConfig::standalone()
        };

        let control = ClusterTestingControl::start(cfg)
            .await
            .expect("inline harness failed to start");

        let port = control
            .client_port(0)
            .expect("inline harness has no client port for slot 0");
        let addr: SocketAddr = format!("127.0.0.1:{port}")
            .parse()
            .expect("malformed inline addr");

        let client = NodeClient::connect(addr)
            .await
            .expect("failed to connect to inline node");

        Self {
            control,
            client,
            addr,
        }
    }

    /// Get a reference to the client.
    pub fn client(&self) -> &NodeClient {
        &self.client
    }

    /// Borrow the underlying harness — exposed for tests that need
    /// direct in-process access (ledger, ledger_slot, etc.).
    #[allow(dead_code)]
    pub fn control(&self) -> &ClusterTestingControl {
        &self.control
    }
}
