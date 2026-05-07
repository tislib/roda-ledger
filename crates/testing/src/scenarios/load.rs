//! Load (throughput / soak) scenarios. The runner records latency and
//! op counts during execution; assertions are minimal — these
//! scenarios are about producing measurements, not pass/fail.

use std::time::Duration;

use crate::scenario::{
    Action, AssertBalance, AsyncBranch, BatchKind, NodeSelector, RetryConfig, Scenario, Step,
    Submit, SubmitBatch, SubmitOp, Wait, WaitLevel,
};

pub fn all() -> Vec<Scenario> {
    vec![deposit_burst_1k(), sustained_transfer_load()]
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
                steps: vec![Step::new(Action::SubmitBatch(SubmitBatch {
                    wait: WaitLevel::None,
                    retry: Some(RetryConfig {
                        max_retries: 5,
                        backoff_ms: 50,
                    }),
                    kind: BatchKind::Dynamic {
                        base: vec![SubmitOp::Deposit {
                            account: 1,
                            amount: 1,
                            user_ref: 1,
                        }],
                        repeat: 1000,
                    },
                }))
                .with_label("1k deposits")],
            })),
            // Implicit join at end-of-scenario; small grace period so
            // the cluster has settled before asserting.
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 1000,
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
                    user_ref: 1,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::AsyncBranch(AsyncBranch {
                name: Some("a_to_b".into()),
                steps: vec![Step::new(Action::SubmitBatch(SubmitBatch {
                    wait: WaitLevel::None,
                    retry: None,
                    kind: BatchKind::Dynamic {
                        base: vec![SubmitOp::Transfer {
                            from: 10,
                            to: 11,
                            amount: 1,
                            user_ref: 1_000,
                        }],
                        repeat: 200,
                    },
                }))],
            })),
            Step::new(Action::AsyncBranch(AsyncBranch {
                name: Some("b_to_a".into()),
                steps: vec![Step::new(Action::SubmitBatch(SubmitBatch {
                    wait: WaitLevel::None,
                    retry: None,
                    kind: BatchKind::Dynamic {
                        base: vec![SubmitOp::Transfer {
                            from: 11,
                            to: 10,
                            amount: 1,
                            user_ref: 2_000,
                        }],
                        repeat: 200,
                    },
                }))],
            })),
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(500),
            })),
            // 200 each direction; net is zero, so account 10 stays at 10_000.
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 10,
                expected: 10_000,
            })),
        ])
}
