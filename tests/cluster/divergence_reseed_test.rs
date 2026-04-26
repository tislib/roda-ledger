//! End-to-end test of the supervisor's divergence-driven Ledger
//! reseed (ADR-0016 §9, Stage 3c).
//!
//! Setup:
//!   1. Spin up a single multi-node-shaped cluster (one real node
//!      + one phantom peer so the supervisor boots in
//!      `Initializing` rather than `Leader`).
//!   2. Seed the local ledger with deposits BEFORE running the
//!      supervisor — this is the only way to populate a ledger
//!      that's about to enter Initializing-only mode. Because
//!      Initializing rejects writes, no client API exists for this
//!      seeding step; we must drive `Ledger::submit` directly. We
//!      use `autostart: false` + `build_node` + `pre_run_ledger` so
//!      the harness creates the Ledger but doesn't run the
//!      supervisor yet.
//!   3. Send a crafted `AppendEntries` RPC against the supervisor's
//!      Node gRPC port that disagrees with our log at `prev_tx_id`
//!      under a different `prev_term`. This stashes a divergence
//!      watermark in `NodeHandlerCore`.
//!   4. Wait for the supervisor's divergence watcher to consume the
//!      watermark and run `start_with_recovery_until(watermark)` on
//!      a new Ledger.
//!   5. Assert via the (still-running) client gRPC server that the
//!      live ledger now has `commit == watermark` and balances
//!      reflect only the surviving prefix.

#![cfg(feature = "cluster")]

use roda_ledger::cluster::proto::node as proto;
use roda_ledger::cluster::proto::node::node_client::NodeClient;
use roda_ledger::cluster::{ClusterTestingConfig, ClusterTestingControl};
use roda_ledger::entities::WalEntry;
use roda_ledger::transaction::Operation;
use std::time::Duration;
use tonic::transport::Channel;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_reseeds_on_divergence_detected_via_node_grpc() {
    // Multi-node-shaped config so the supervisor boots in
    // Initializing (avoids the seed-leader bring-up path which
    // would try to rotate term + spawn replication; we just need a
    // long-lived Node gRPC server that accepts AppendEntries and a
    // running watcher loop).
    //
    // `autostart: false` lets us build the ClusterNode (and its
    // Ledger) without running the supervisor yet — so we can seed
    // the ledger in between.
    let mut ctl = ClusterTestingControl::start(ClusterTestingConfig {
        label: "divergence_reseed".to_string(),
        phantom_peer_count: 1, // peer_id = 2, port reserved, never started
        replication_poll_ms: 5,
        append_entries_max_bytes: 4 * 1024 * 1024,
        transaction_count_per_segment: 10_000_000,
        snapshot_frequency: 4,
        autostart: false,
        ..ClusterTestingConfig::cluster(1)
    })
    .await
    .expect("build");

    // Build node 0 (creates the Ledger) but don't run() yet.
    ctl.build_node(0).expect("build_node");

    // Pre-run seeding: cluster isn't running yet, so there's no
    // client API. Drive `Ledger::submit` directly to get 50
    // deposits onto account 1 (segments 1..=50) and 50 onto
    // account 2 (segments 51..=100).
    let pre = ctl.pre_run_ledger(0).expect("pre_run_ledger");
    pre.set_seal_watermark(50); // permit segments 1..=50 to seal
    for _ in 0..50 {
        pre.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
    }
    pre.wait_for_pass();
    for _ in 0..50 {
        pre.submit(Operation::Deposit {
            account: 2,
            amount: 1,
            user_ref: 0,
        });
    }
    pre.wait_for_pass();
    assert_eq!(pre.last_commit_id(), 100);
    assert_eq!(pre.get_balance(1), 50);
    assert_eq!(pre.get_balance(2), 50);
    drop(pre); // release the pre-run handle before run_node

    // Start the supervisor — long-lived gRPC servers + watcher loop
    // come up here.
    ctl.run_node(0).await.expect("run_node");
    ctl.wait_for_bind(0, Duration::from_secs(5))
        .await
        .expect("bind");

    // Connect a Node-service client to the supervisor's gRPC port
    // and send a crafted divergent AppendEntries: same prev_tx_id
    // (50) but a different prev_term than the one we wrote under
    // (terms always start at 1 in tests, so claim 7).
    let node_port = ctl.node_port(0).unwrap();
    let mut node_client: NodeClient<Channel> =
        NodeClient::connect(format!("http://127.0.0.1:{}", node_port))
            .await
            .expect("connect node service");

    let leader_commit = 50u64;
    let resp = node_client
        .append_entries(proto::AppendEntriesRequest {
            leader_id: 2,
            term: 7,
            prev_tx_id: 50,
            prev_term: 7, // wrong — local prev_term is 1
            from_tx_id: 51,
            to_tx_id: 50,
            wal_bytes: Vec::new(),
            leader_commit_tx_id: leader_commit,
        })
        .await
        .expect("rpc")
        .into_inner();
    assert!(!resp.success);
    assert_eq!(
        resp.reject_reason,
        proto::RejectReason::RejectPrevMismatch as u32
    );

    // Watcher's poll cadence is 10 ms; reseed builds a Ledger
    // (sync I/O via spawn_blocking) which can take a few hundred
    // ms in release. Give it a generous deadline. After reseed,
    // the gRPC server's `LedgerSlot` points at the new Ledger; the
    // existing client picks up the swap transparently because all
    // RPCs go through `LedgerSlot::ledger()` on every call.
    //
    // The cluster is in Initializing post-reseed, so reads work
    // (`get_pipeline_index` doesn't go through the writable gate).
    let client = ctl.client(0).await.expect("client");
    ctl.wait_for(
        Duration::from_secs(10),
        "supervisor reseed to land at watermark",
        || {
            let c = client.clone();
            async move {
                c.get_pipeline_index()
                    .await
                    .map(|i| i.commit == leader_commit)
                    .unwrap_or(false)
            }
        },
    )
    .await
    .expect("reseed");

    // ── Verification: balances reflect only the surviving prefix ────
    let idx = client.get_pipeline_index().await.expect("pipeline index");
    assert_eq!(
        idx.commit, leader_commit,
        "ledger should be reseeded to the leader's watermark"
    );

    let b1 = client.get_balance(1).await.expect("balance 1").balance;
    let b2 = client.get_balance(2).await.expect("balance 2").balance;
    assert_eq!(b1, 50, "account 1 prefix preserved");
    assert_eq!(b2, 0, "account 2 deposits truncated");

    ctl.stop_node(0).expect("stop");
    // `ctl` Drop removes the cluster's root temp folder.
}

// Suppress unused-import warning when this test is the only thing
// that pulls these in.
#[allow(dead_code)]
fn _ensures_imports_compile() -> Vec<&'static str> {
    let _ = WalEntry::SegmentHeader;
    Vec::new()
}
