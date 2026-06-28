//! Load (throughput / soak) scenarios. The runner records per-stage
//! latency (via the leader probe) and `cluster_commit` movement during
//! execution; assertions are minimal — these scenarios are about
//! producing measurements, not pass/fail.
//!
//! ## Load levels & data budget
//!
//! All scenarios deposit (`amount = 1`) so disk is dominated by the WAL:
//! ~120 bytes per op, per node. Budget is ideally a few GB / node, hard
//! cap <10 GB (≈83M ops). Each scenario provisions a fresh cluster
//! (temp dirs, removed on drop), so totals don't accumulate across runs.
//!
//! | scenario              | rate      | ops   | data/node |
//! |-----------------------|-----------|-------|-----------|
//! | deposit_burst_1k      | uncapped  | 1k    | ~0        |
//! | concurrent_deposit    | uncapped  | 200k  | ~24 MB    |
//! | sustained_2min        | 100k/s    | 12M   | ~1.4 GB   |
//! | sustained_10min       | 500/s     | 300k  | ~36 MB    |
//! | spike                 | 500k/s    | 5M    | ~600 MB   |
//! | peak                  | uncapped  | 10M   | ~1.2 GB   |
//!
//! Drains: every measured scenario ends with `WaitForLevel(OnSnapshot)`
//! on the last `user_ref`, so the report covers commit, not just submit.
//! Deposits use a non-zero base `user_ref` so the runner's per-op
//! `iter * stride` offset yields unique refs `1..=TOTAL_OPS` and the
//! drain can resolve the last one (a zero base collapses every op to
//! `user_ref = 0` and the drain never binds).

use std::time::Duration;

use crate::scenario::{
    Action, AsyncBranch, BatchKind, NodeSelector, PipelineLevel, RetryConfig, Scenario, Step,
    SubmitBatch, SubmitOp, TxRef, Wait, WaitForLevel, WaitLevel,
};

pub fn all() -> Vec<Scenario> {
    vec![
        deposit_burst_1k(),
        concurrent_deposit_load(),
        load_sustained_2min(),
        load_sustained_10min(),
        load_spike(),
        load_peak(),
    ]
}

/// One fire-and-forget deposit stream into `account`. `base_user_ref`
/// must be non-zero so the offset scheme produces unique refs and the
/// caller can drain on `base_user_ref + total - 1`.
fn deposit_stream(
    account: u64,
    base_user_ref: u64,
    total: u32,
    chunk: u32,
    rate: u32,
) -> SubmitBatch {
    SubmitBatch {
        wait: WaitLevel::None,
        retry: None,
        rate,
        kind: BatchKind::Dynamic {
            base: vec![SubmitOp::Deposit {
                account,
                amount: 1,
                user_ref: base_user_ref,
            }],
            repeat: total,
            batch_size: chunk,
        },
    }
}

/// Block until the deposit with `user_ref` lands on the leader's
/// snapshot — drains the pipeline so the report measures commit.
fn drain_to_snapshot(user_ref: u64) -> Step {
    Step::new(Action::WaitForLevel(WaitForLevel {
        node: NodeSelector::Leader,
        tx: TxRef::UserRef(user_ref),
        level: PipelineLevel::OnSnapshot,
    }))
}

/// 1000 deposits from one async branch with retry. Cheap enough for CI,
/// heavy enough to surface obvious latency regressions.
fn deposit_burst_1k() -> Scenario {
    Scenario::new("load_deposit_burst_1k")
        .with_description("Fire 1000 deposits from one async branch (CI smoke).")
        .with_steps(vec![
            Step::new(Action::AsyncBranch(AsyncBranch {
                name: Some("burst".into()),
                steps: vec![
                    Step::new(Action::SubmitBatch(SubmitBatch {
                        wait: WaitLevel::None,
                        retry: Some(RetryConfig {
                            max_retries: 5,
                            backoff_ms: 50,
                        }),
                        rate: 0,
                        kind: BatchKind::Dynamic {
                            base: vec![SubmitOp::Deposit {
                                account: 1,
                                amount: 1,
                                user_ref: 0,
                            }],
                            repeat: 1000,
                            batch_size: 0,
                        },
                    }))
                    .with_label("1k deposits"),
                ],
            })),
            // Implicit join at end-of-scenario; small grace period.
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
        ])
}

/// Two concurrent branches each streaming 100k deposits into a distinct
/// account — exercises fan-out submission across `AsyncBranch`es.
/// (Was the transfer scenario; batched non-deposit ops aren't wired, so
/// it now deposits.)
fn concurrent_deposit_load() -> Scenario {
    const PER_BRANCH: u32 = 100_000;
    const CHUNK: u32 = 1_000;

    let branch = |name: &str, account: u64, base_ref: u64| {
        Step::new(Action::AsyncBranch(AsyncBranch {
            name: Some(name.into()),
            steps: vec![Step::new(Action::SubmitBatch(deposit_stream(
                account, base_ref, PER_BRANCH, CHUNK, 0,
            )))],
        }))
    };

    Scenario::new("load_concurrent_deposit")
        .with_description(
            "Two concurrent branches each streaming 100k deposits (fan-out, 200k total).",
        )
        .with_steps(vec![
            branch("branch_a", 10, 1),
            branch("branch_b", 11, 1_000_000),
            // Implicit join at end-of-scenario; grace period to settle.
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
        ])
}

/// Sustain 100k ops/s for 2 minutes (12M deposits). Rate-limited at the
/// runner; the cluster's achieved throughput shows up in the report's
/// `cluster_commit` movement and the per-stage latency probe.
fn load_sustained_2min() -> Scenario {
    const RATE: u32 = 100_000;
    const DURATION_SECS: u32 = 120;
    const TOTAL_OPS: u32 = RATE * DURATION_SECS;
    const CHUNK: u32 = 1_000;

    Scenario::new("load_sustained_2min")
        .with_description("100k ops/s deposit stream sustained for 2 minutes (12M ops).")
        .with_steps(vec![
            Step::new(Action::SubmitBatch(deposit_stream(
                1, 1, TOTAL_OPS, CHUNK, RATE,
            )))
            .with_label("12M deposits at 100k ops/s"),
            drain_to_snapshot(TOTAL_OPS as u64),
        ])
}

/// 500 ops/s sustained for 10 minutes (300k deposits). Deliberately low:
/// a long, steady-state target for CPU profilers, where per-thread cost
/// stabilises. The only scenario where low load is the point.
fn load_sustained_10min() -> Scenario {
    const RATE: u32 = 500;
    const DURATION_SECS: u32 = 600;
    const TOTAL_OPS: u32 = RATE * DURATION_SECS;

    Scenario::new("load_sustained_10min")
        .with_description(
            "500 ops/s deposit stream sustained for 10 minutes — CPU-profiling target.",
        )
        .with_steps(vec![
            Step::new(Action::SubmitBatch(deposit_stream(
                1, 1, TOTAL_OPS, 0, RATE,
            )))
            .with_label("300k deposits at 500 ops/s over 10 min"),
            drain_to_snapshot(TOTAL_OPS as u64),
        ])
}

/// A short, high-rate spike: 500k ops/s for ~10s (5M deposits), the top
/// of the rate-capped range. Batched at 5k ops/chunk so 100 RPCs/s carry
/// the rate.
fn load_spike() -> Scenario {
    const RATE: u32 = 500_000;
    const DURATION_SECS: u32 = 10;
    const TOTAL_OPS: u32 = RATE * DURATION_SECS;
    const CHUNK: u32 = 5_000;

    Scenario::new("load_spike")
        .with_description("500k ops/s deposit spike for ~10s (5M ops).")
        .with_steps(vec![
            Step::new(Action::SubmitBatch(deposit_stream(
                1, 1, TOTAL_OPS, CHUNK, RATE,
            )))
            .with_label("5M deposits at 500k ops/s"),
            drain_to_snapshot(TOTAL_OPS as u64),
        ])
}

/// Full load, uncapped: 10M deposits as fast as the runner + cluster
/// allow, batched 1k ops/chunk. Measures peak commit throughput; 10M
/// gives a multi-second window the 100ms poller can resolve.
fn load_peak() -> Scenario {
    const TOTAL_OPS: u32 = 10_000_000;
    const CHUNK: u32 = 1_000;

    Scenario::new("load_peak")
        .with_description("Uncapped 10M-deposit peak — full speed, batched 1k/chunk.")
        .with_steps(vec![
            Step::new(Action::SubmitBatch(deposit_stream(
                1, 1, TOTAL_OPS, CHUNK, 0,
            )))
            .with_label("10M deposits at full speed"),
            drain_to_snapshot(TOTAL_OPS as u64),
        ])
}
