//! Basic smoke tests for the scenario RPCs the UI uses on the Testing
//! page. Per the test plan, scenario coverage is deliberately shallow —
//! the underlying scenario subsystem is tested in `crates/testing` and
//! `process_provisioner_smoke.rs`.

use std::time::Duration;

use proto::control::control_server::Control;
use proto::control::{
    CancelScenarioRequest, GetScenarioStatusRequest, ListAvailableScenariosRequest,
    ListScenarioRunsRequest, RunScenarioRequest, Scenario, ScenarioCategory, ScenarioState,
};
use tonic::{Code, Request};

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_available_scenarios_contains_both_categories() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .list_available_scenarios(Request::new(ListAvailableScenariosRequest {}))
        .await
        .expect("list_available_scenarios")
        .into_inner();
    assert!(!resp.scenarios.is_empty());
    assert!(
        resp.scenarios
            .iter()
            .any(|s| s.category == ScenarioCategory::E2e as i32),
        "expected at least one E2E scenario"
    );
    assert!(
        resp.scenarios
            .iter()
            .any(|s| s.category == ScenarioCategory::Load as i32),
        "expected at least one LOAD scenario"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_scenario_single_deposit_committed_completes() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .run_scenario(Request::new(RunScenarioRequest {
            scenario: Some(Scenario {
                name: "single_deposit_committed".into(),
                description: String::new(),
                steps: Vec::new(),
            }),
        }))
        .await
        .expect("run_scenario")
        .into_inner();
    assert!(!resp.run_id.is_empty());
    let run_id = resp.run_id;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status = svc
            .get_scenario_status(Request::new(GetScenarioStatusRequest {
                run_id: run_id.clone(),
            }))
            .await
            .expect("get_scenario_status")
            .into_inner();
        if status.state == ScenarioState::Completed as i32 {
            break;
        }
        assert_ne!(
            status.state,
            ScenarioState::Failed as i32,
            "scenario failed: {}",
            status.error
        );
        if std::time::Instant::now() >= deadline {
            panic!(
                "scenario {} never reached Completed; last state={}",
                run_id, status.state
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let runs = svc
        .list_scenario_runs(Request::new(ListScenarioRunsRequest { limit: 0 }))
        .await
        .expect("list_scenario_runs")
        .into_inner();
    assert!(
        runs.runs.iter().any(|r| r.run_id == run_id),
        "list_scenario_runs missing run_id {}",
        run_id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_scenario_unknown_name_returns_not_found() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let err = svc
        .run_scenario(Request::new(RunScenarioRequest {
            scenario: Some(Scenario {
                name: "does_not_exist".into(),
                description: String::new(),
                steps: Vec::new(),
            }),
        }))
        .await
        .expect_err("expected not_found");
    assert_eq!(err.code(), Code::NotFound, "{:?}", err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_scenario_status_unknown_run_id_returns_not_found() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let err = svc
        .get_scenario_status(Request::new(GetScenarioStatusRequest {
            run_id: "run-bogus".into(),
        }))
        .await
        .expect_err("expected not_found");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_scenario_unknown_run_id_returns_accepted_false() {
    let (svc, _events, _h) = common::build_control(1).await;
    common::wait_for_leader(&svc, Duration::from_secs(10)).await;

    let resp = svc
        .cancel_scenario(Request::new(CancelScenarioRequest {
            run_id: "run-bogus".into(),
        }))
        .await
        .expect("returns Ok with accepted=false")
        .into_inner();
    assert!(!resp.accepted);
    assert!(!resp.error.is_empty());
}
