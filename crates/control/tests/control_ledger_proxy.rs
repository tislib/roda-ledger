//! Integration tests for the Ledger proxy (which the UI's Ledger and
//! Meta pages drive) plus the streaming `watch_functions` Control RPC.
//! Each test owns its own cluster so children are reaped when the test
//! finishes — see `control_monitoring.rs` for the rationale.

use std::sync::Arc;
use std::time::Duration;

use control::service::ControlService;
use control::{EventStore, LedgerProxy, NODE_SELECTOR_METADATA_KEY};
use proto::control::control_server::Control;
use proto::control::WatchFunctionsRequest;
use proto::ledger as pb;
use proto::ledger::ledger_server::Ledger;
use proto::ledger::submit_operation_request::Operation;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request};

mod common;

fn req_with_node_selector<T>(body: T, node_id: u64) -> Request<T> {
    let mut req = Request::new(body);
    req.metadata_mut().insert(
        NODE_SELECTOR_METADATA_KEY,
        MetadataValue::try_from(node_id.to_string()).expect("valid ascii"),
    );
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_operation_deposit_returns_tx_id() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let resp = proxy
        .submit_operation(Request::new(pb::SubmitOperationRequest {
            operation: Some(Operation::Deposit(pb::Deposit {
                account: 1_001,
                amount: 100,
                user_ref: 7001,
            })),
        }))
        .await
        .expect("submit_operation")
        .into_inner();
    assert!(resp.transaction_id > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_transaction_status_reaches_terminal_state() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let tx = proxy
        .submit_operation(Request::new(pb::SubmitOperationRequest {
            operation: Some(Operation::Deposit(pb::Deposit {
                account: 1_002,
                amount: 200,
                user_ref: 7002,
            })),
        }))
        .await
        .expect("submit_operation")
        .into_inner()
        .transaction_id;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = proxy
            .get_transaction_status(Request::new(pb::GetStatusRequest {
                transaction_id: tx,
                term: 0,
            }))
            .await
            .expect("status")
            .into_inner();
        if status.status == pb::TransactionStatus::OnSnapshot as i32
            || status.status == pb::TransactionStatus::Committed as i32
        {
            assert_eq!(status.fail_reason, 0);
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("tx {} never reached terminal state; last status={}", tx, status.status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_balance_reflects_deposit_after_snapshot() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let acct = 1_003u64;
    let tx = proxy
        .submit_operation(Request::new(pb::SubmitOperationRequest {
            operation: Some(Operation::Deposit(pb::Deposit {
                account: acct,
                amount: 500,
                user_ref: 7003,
            })),
        }))
        .await
        .expect("submit_operation")
        .into_inner()
        .transaction_id;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let bal = proxy
            .get_balance(Request::new(pb::GetBalanceRequest { account_id: acct }))
            .await
            .expect("get_balance")
            .into_inner();
        if bal.last_snapshot_tx_id >= tx {
            assert_eq!(bal.balance, 500);
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "balance never reflected tx {} (last_snapshot_tx_id={}, balance={})",
                tx, bal.last_snapshot_tx_id, bal.balance
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_functions_returns_a_list() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let resp = proxy
        .list_functions(Request::new(pb::ListFunctionsRequest {}))
        .await
        .expect("list_functions")
        .into_inner();
    let _ = resp.functions;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_function_forwards_to_cluster() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let result = proxy
        .register_function(Request::new(pb::RegisterFunctionRequest {
            name: "proxy_smoke_invalid_wasm".into(),
            binary: vec![0x00; 8],
            override_existing: true,
        }))
        .await;
    // Either the cluster accepts the bytes (unlikely for 8 zero bytes)
    // or returns a Status — both prove the proxy forwarded the call.
    match result {
        Ok(_) => {}
        Err(s) => {
            assert!(!s.message().is_empty(), "empty Status: {:?}", s);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_selector_metadata_pins_call() {
    let (proxy, h) = common::build_ledger_proxy(3).await;
    let svc = ControlService::new(h.clone(), Arc::new(EventStore::new()));
    let leader_id = common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    // Pin to each node and read `is_leader`. Only the actual leader's
    // pinned call should report true; if the proxy ignored the pin and
    // round-robined, at least one follower would surface is_leader=true.
    for node_id in 1u64..=3 {
        let resp = proxy
            .get_pipeline_index(req_with_node_selector(
                pb::GetPipelineIndexRequest {},
                node_id,
            ))
            .await
            .expect("get_pipeline_index via node-selector")
            .into_inner();
        let expected = node_id == leader_id;
        assert_eq!(
            resp.is_leader, expected,
            "node-selector={} returned is_leader={}, leader_id={}",
            node_id, resp.is_leader, leader_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_log_pinned_per_node_succeeds() {
    let (proxy, h) = common::build_ledger_proxy(3).await;
    let svc = ControlService::new(h.clone(), Arc::new(EventStore::new()));
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    // Per-node WAL inspection is the use-case the deleted GetNodeWalLog
    // RPC served; the Ledger proxy serves it via Ledger.GetLog with a
    // node-selector header. All three nodes — leader and followers —
    // must serve get_log from their local WAL.
    for node_id in 1u64..=3 {
        proxy
            .get_log(req_with_node_selector(
                pb::GetLogRequest {
                    from_tx_id: 0,
                    to_tx_id: 0,
                    limit: 10,
                },
                node_id,
            ))
            .await
            .expect("get_log via node-selector");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_selector_unknown_id_returns_invalid_argument() {
    let (proxy, _h) = common::build_ledger_proxy(3).await;
    let err = proxy
        .get_balance(req_with_node_selector(
            pb::GetBalanceRequest { account_id: 1 },
            999,
        ))
        .await
        .expect_err("expected invalid_argument for unknown node-selector");
    assert_eq!(err.code(), Code::InvalidArgument, "{:?}", err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_functions_emits_initial_frame() {
    let (svc, _events, _h) = common::build_control(3).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let stream = svc
        .watch_functions(Request::new(WatchFunctionsRequest { interval_ms: 200 }))
        .await
        .expect("watch_functions")
        .into_inner();
    tokio::pin!(stream);

    let frame = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("no frame within 3 s")
        .expect("stream closed without a frame")
        .expect("stream frame Err");
    let _ = frame.functions;
}
