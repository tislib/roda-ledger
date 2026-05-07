//! E2E (assertion-style) scenarios. Each scenario sets up a small,
//! deterministic situation and asserts the cluster ends up in the
//! expected state. Kept simple on purpose — these are the seed
//! scenarios; the catalogue grows as new shapes are needed.

use std::time::Duration;

use crate::scenario::{
    Action, AssertBalance, AssertBalanceSum, AssertLeader, BatchKind, KillNode, NodeSelector,
    PipelineLevel, RestartNode, RetryConfig, Scenario, StartNode, Step, Submit, SubmitBatch,
    SubmitOp, TxRef, Wait, WaitForLevel, WaitLevel,
};

pub fn all() -> Vec<Scenario> {
    vec![
        single_deposit_committed(),
        transfer_chain(),
        kill_then_restart_recovers(),
        basic_crash_recovery_1m_tx(),
    ]
}

/// Submit a single deposit, wait until it lands on snapshot, and
/// assert the ledger reflects it. Snapshot is the queryable level —
/// `get_balance` returns the new value only after a tx reaches it.
fn single_deposit_committed() -> Scenario {
    Scenario::new("single_deposit_committed")
        .with_description(
            "One deposit submitted to the leader, waited to snapshot, balance asserted.",
        )
        .with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 1,
                },
                wait: WaitLevel::OnSnapshot,
                retry: None,
            }))
            .with_label("deposit 100 into account 1"),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 100,
            }))
            .with_label("balance == 100"),
        ])
}

/// Deposit, then transfer between two accounts, asserting balances.
/// Exercises the typical client flow end-to-end.
fn transfer_chain() -> Scenario {
    Scenario::new("transfer_chain")
        .with_description("Deposit then transfer; assert both account balances on the leader.")
        .with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 1000,
                    user_ref: 1,
                },
                wait: WaitLevel::OnSnapshot,
                retry: None,
            })),
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Transfer {
                    from: 1,
                    to: 2,
                    amount: 250,
                    user_ref: 2,
                },
                wait: WaitLevel::OnSnapshot,
                retry: None,
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 750,
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 2,
                expected: 250,
            })),
        ])
}

/// Submit, then kill a follower, restart it, and assert the cluster
/// still has a leader and the surviving committed work is visible.
fn kill_then_restart_recovers() -> Scenario {
    Scenario::new("kill_then_restart_recovers")
        .with_description(
            "Deposit, kill follower, restart it, assert cluster has a leader and balance holds.",
        )
        .with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 50,
                    user_ref: 1,
                },
                wait: WaitLevel::OnSnapshot,
                retry: None,
            })),
            Step::new(Action::KillNode(KillNode {
                node: NodeSelector::Index(1),
            }))
            .with_label("kill follower"),
            Step::new(Action::Wait(Wait {
                duration: Duration::from_millis(200),
            })),
            Step::new(Action::RestartNode(RestartNode {
                node: NodeSelector::Index(1),
            })),
            Step::new(Action::AssertLeader(AssertLeader { expected: None }))
                .with_label("cluster still has a leader"),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 50,
            })),
        ])
}

/// Crash recovery at scale: 1M deposits across 1000 accounts (1000
/// each), submitted in 1k-op batches with the cluster leader
/// killed+restarted every 10 batches (100 restart cycles total).
///
/// Per-account balance assertions plus the zero-sum invariant
/// verify two things at once:
/// - **No transaction is lost**: every account ends at exactly 1000.
/// - **No transaction is applied twice**: same. The cluster's
///   `user_ref` deduplication window must absorb retries that race
///   leader churn.
///
/// Requires a multi-node cluster (≥ 3) to be meaningful — single-node
/// clusters have no failover target. Submits ride out leader switches
/// via the runner's per-`Submit` retry; kept conservative at 30 ×
/// 200 ms so the cluster has ~6 s to elect a new leader.
fn basic_crash_recovery_1m_tx() -> Scenario {
    const ACCOUNT_COUNT: u64 = 1_000;
    const BATCH_SIZE: u64 = 1_000;
    const BATCHES_PER_RESTART: u64 = 10;
    const TOTAL_BATCHES: u64 = 1_000;
    const TOTAL_OPS: u64 = TOTAL_BATCHES * BATCH_SIZE; // 1_000_000
    const TOTAL_SUPER_BATCHES: u64 = TOTAL_BATCHES / BATCHES_PER_RESTART; // 100

    // 1 SubmitBatch + 1 KillNode + 1 StartNode per super-batch,
    // plus 1 drain WaitForLevel + 1000 AssertBalance + 1 AssertBalanceSum.
    let cap = (TOTAL_SUPER_BATCHES * 3 + ACCOUNT_COUNT + 2) as usize;
    let mut steps: Vec<Step> = Vec::with_capacity(cap);

    for super_idx in 0..TOTAL_SUPER_BATCHES {
        // Per-super-batch base: 1000 deposits across accounts 1..=1000
        // with consecutive user_refs starting at the super-batch's
        // offset. The batch's `Dynamic` repetition strides by
        // base.len() = 1000 so user_refs stay unique across the 10
        // expanded copies.
        let user_ref_base = super_idx * BATCHES_PER_RESTART * BATCH_SIZE;
        let base: Vec<SubmitOp> = (0..BATCH_SIZE)
            .map(|i| SubmitOp::Deposit {
                account: (i % ACCOUNT_COUNT) + 1,
                amount: 1,
                user_ref: user_ref_base + i + 1,
            })
            .collect();

        steps.push(Step::new(Action::SubmitBatch(SubmitBatch {
            wait: WaitLevel::None,
            retry: Some(RetryConfig {
                max_retries: 30,
                backoff_ms: 200,
            }),
            rate: 0,
            kind: BatchKind::Dynamic {
                base,
                repeat: BATCHES_PER_RESTART as u32,
            },
        })));

        // Kill whichever node is leader right now (resolved at
        // step-execution time so churn from prior cycles is OK), then
        // bring it back. `NodeSelector::LastKilled` carries the
        // index from the kill into the start so we don't need to
        // know which physical node was leader at scenario-build
        // time.
        steps.push(Step::new(Action::KillNode(KillNode {
            node: NodeSelector::Leader,
        })));
        steps.push(Step::new(Action::StartNode(StartNode {
            node: NodeSelector::LastKilled,
        })));
    }

    // Drain: block until the last submitted tx (user_ref = 1_000_000)
    // lands on snapshot. Without this the per-account assertions
    // would race the snapshot stage and read pre-snapshot balances.
    steps.push(Step::new(Action::WaitForLevel(WaitForLevel {
        node: NodeSelector::Leader,
        tx: TxRef::UserRef(TOTAL_OPS),
        level: PipelineLevel::OnSnapshot,
    })));

    // 1000 per-account balance checks. Each account got exactly 1000
    // deposits of amount 1, so it should land at 1000. Any drift
    // means a tx was lost (low) or applied twice (high).
    for account in 1..=ACCOUNT_COUNT {
        steps.push(Step::new(Action::AssertBalance(AssertBalance {
            node: NodeSelector::Leader,
            account,
            expected: 1000,
        })));
    }

    // Zero-sum invariant: account 0 absorbed the counter-leg of
    // every deposit; total across [0, ACCOUNT_COUNT] must be 0.
    steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
        node: NodeSelector::Leader,
        max_account: ACCOUNT_COUNT,
    })));

    Scenario::new("basic_crash_recovery_1m_tx")
        .with_description(
            "1M deposits across 1000 accounts (1000 per account), 1k-op batches with leader killed+restarted every 10 batches. Per-account balance + zero-sum invariant prove no tx lost and no tx applied twice.",
        )
        .with_steps(steps)
}
