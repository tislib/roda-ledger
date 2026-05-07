//! E2E (assertion-style) scenarios. Each scenario sets up a small,
//! deterministic situation and asserts the cluster ends up in the
//! expected state. Kept simple on purpose — these are the seed
//! scenarios; the catalogue grows as new shapes are needed.

use std::time::Duration;

use crate::scenario::{
    Action, AssertBalance, AssertLeader, KillNode, NodeSelector, RestartNode, Scenario, Step,
    Submit, SubmitOp, Wait, WaitLevel,
};

pub fn all() -> Vec<Scenario> {
    vec![
        single_deposit_committed(),
        transfer_chain(),
        kill_then_restart_recovers(),
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
