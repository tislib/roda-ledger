//! Load (throughput / soak) scenarios. The runner records latency and
//! op counts during execution; assertions are minimal — these
//! scenarios are about producing measurements, not pass/fail.

use std::time::Duration;

use crate::scenario::{
    Action, AsyncBranch, BatchKind, NodeSelector, PipelineLevel, RetryConfig, Scenario, Step,
    Submit, SubmitBatch, SubmitOp, TxRef, Wait, WaitForLevel, WaitLevel,
};

pub fn all() -> Vec<Scenario> {
    vec![
        deposit_burst_1k(),
        sustained_transfer_load(),
        load_sustained_2min(),
        load_sustained_10min(),
        load_spike(),
    ]
}

/// 10-minute sustained deposit stream at the same rate as
/// `load_sustained_2min` (500 ops/s). Used to give CPU profilers a
/// long, steady-state target so per-thread cost stabilises.
fn load_sustained_10min() -> Scenario {
    const RATE_OPS_PER_SEC: u32 = 500;
    const DURATION_SECS: u32 = 600;
    const TOTAL_OPS: u64 = (RATE_OPS_PER_SEC as u64) * (DURATION_SECS as u64);

    Scenario::new("load_sustained_10min")
        .with_description(
            "500 ops/s deposit stream sustained for 10 minutes — long-running target for CPU profiling.",
        )
        .with_steps(vec![
            Step::new(Action::SubmitBatch(SubmitBatch {
                wait: WaitLevel::None,
                retry: None,
                rate: RATE_OPS_PER_SEC,
                kind: BatchKind::Dynamic {
                    base: vec![SubmitOp::Deposit {
                        account: 1,
                        amount: 1,
                        user_ref: 1,
                    }],
                    repeat: TOTAL_OPS as u32,
                    batch_size: 0,
                },
            }))
            .with_label("300k deposits at 500 ops/s over 10 min"),
            Step::new(Action::WaitForLevel(WaitForLevel {
                node: NodeSelector::Leader,
                tx: TxRef::UserRef(TOTAL_OPS),
                level: PipelineLevel::OnSnapshot,
            })),
        ])
}

/// 1000 deposits driven from a single async branch with retry. Cheap
/// enough to run in CI; heavy enough to surface obvious latency
/// regressions. Expressed as one `SubmitBatch::Dynamic` so the
/// scenario stays compact regardless of repeat count.
fn deposit_burst_1k() -> Scenario {
    Scenario::new("load_deposit_burst_1k")
        .with_description(
            "Fire 1000 deposits from one async branch, then assert the final balance.",
        )
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
            // Implicit join at end-of-scenario; small grace period so
            // the cluster has settled before asserting.
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
        ])
}

/// Two account ranges transferring back and forth. Lighter workload
/// than the burst — runs longer wall-clock for soak-style observation.
fn sustained_transfer_load() -> Scenario {
    Scenario::new("load_sustained_transfer")
        .with_description(
            "Two concurrent branches transferring between two accounts; final balances net to zero.",
        )
        .with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 10,
                    amount: 10_000,
                    user_ref: 0,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::AsyncBranch(AsyncBranch {
                name: Some("a_to_b".into()),
                steps: vec![Step::new(Action::SubmitBatch(SubmitBatch {
                    wait: WaitLevel::None,
                    retry: None,
                    rate: 0,
                    kind: BatchKind::Dynamic {
                        base: vec![SubmitOp::Transfer {
                            from: 10,
                            to: 11,
                            amount: 1,
                            user_ref: 1_000,
                        }],
                        repeat: 200,
                        batch_size: 0,
                    },
                }))],
            })),
            Step::new(Action::AsyncBranch(AsyncBranch {
                name: Some("b_to_a".into()),
                steps: vec![Step::new(Action::SubmitBatch(SubmitBatch {
                    wait: WaitLevel::None,
                    retry: None,
                    rate: 0,
                    kind: BatchKind::Dynamic {
                        base: vec![SubmitOp::Transfer {
                            from: 11,
                            to: 10,
                            amount: 1,
                            user_ref: 2_000,
                        }],
                        repeat: 200,
                        batch_size: 0,
                    },
                }))],
            })),
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
        ])
}

/// Sustain ~500 ops/s for 2 minutes (60_000 deposits total). Rate-
/// limited at the runner; the cluster's actual achieved throughput
/// shows up in the report's `cluster_commit` movement. Drains
/// explicitly via `WaitForLevel(OnSnapshot)` on the last user_ref so
/// the report reflects all ops landing, not just being submitted.
fn load_sustained_2min() -> Scenario {
    const RATE_OPS_PER_SEC: u32 = 500;
    const DURATION_SECS: u32 = 120;
    const TOTAL_OPS: u64 = (RATE_OPS_PER_SEC as u64) * (DURATION_SECS as u64);

    Scenario::new("load_sustained_2min")
        .with_description(
            "500 ops/s deposit stream sustained for 2 minutes; report throughput from cluster_commit, latencies if any, per-node lag.",
        )
        .with_steps(vec![
            Step::new(Action::SubmitBatch(SubmitBatch {
                wait: WaitLevel::None,
                retry: None,
                rate: RATE_OPS_PER_SEC,
                kind: BatchKind::Dynamic {
                    base: vec![SubmitOp::Deposit {
                        account: 1,
                        amount: 1,
                        user_ref: 0,
                    }],
                    repeat: TOTAL_OPS as u32,
                    batch_size: 0,
                },
            }))
            .with_label("60k deposits at 500 ops/s"),
            // Drain: block until the last submitted tx lands on
            // snapshot. Ensures the throughput report covers commit
            // (not just submit) over the full window.
            Step::new(Action::WaitForLevel(WaitForLevel {
                node: NodeSelector::Leader,
                tx: TxRef::UserRef(TOTAL_OPS),
                level: PipelineLevel::OnSnapshot,
            })),
        ])
}

/// 1M deposits, dispatched as 1000 batches of 1000 ops each. The
/// runner's `submit_batch` chunking groups `CHUNK_SIZE` ops per RPC,
/// so per-RPC gRPC framing is amortized across 1000 ops and the run
/// exercises the cluster's commit path, not tonic round-trip cost.
fn load_spike() -> Scenario {
    // Total ops = base.len() * TOTAL_OPS = 1 * 1_000_000 = 1_000_000.
    // CHUNK_SIZE is the on-wire chunk: 1M ops / 1000 ops-per-chunk =
    // 1000 `submit_batch` RPCs.
    const CHUNK_SIZE: u32 = 1_000;
    const TOTAL_OPS: u32 = 1_000_000;

    Scenario::new("load_spike")
        .with_description("1M-op spike at full speed via 1000 batches of 1000 deposits.")
        .with_steps(vec![
            Step::new(Action::SubmitBatch(SubmitBatch {
                wait: WaitLevel::None,
                retry: None,
                rate: 0,
                kind: BatchKind::Dynamic {
                    // user_ref: 1 (non-zero) so the runner's per-base
                    // `iter * stride` offset produces unique user_refs
                    // 1..=TOTAL_OPS and the drain step below can
                    // resolve the last one.
                    base: vec![SubmitOp::Deposit {
                        account: 1,
                        amount: 1,
                        user_ref: 1,
                    }],
                    repeat: TOTAL_OPS,
                    batch_size: CHUNK_SIZE,
                },
            }))
            .with_label("1k batches × 1k deposits"),
            // Drain so the report measures the full burst settling,
            // not just submission.
            Step::new(Action::WaitForLevel(WaitForLevel {
                node: NodeSelector::Leader,
                tx: TxRef::UserRef(TOTAL_OPS as u64),
                level: PipelineLevel::OnSnapshot,
            })),
        ])
}
