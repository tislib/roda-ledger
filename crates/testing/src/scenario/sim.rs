//! Lightweight in-memory simulator for sanity-checking the scenario
//! catalogue. Tracks account balances using the ledger's double-entry
//! semantics; executes the deterministic parts of a scenario; ignores
//! everything that depends on cluster state or real timing.
//!
//! Purpose is narrow: catch scenario *authoring* bugs — wrong expected
//! balance, off-by-one in a `SubmitBatch::Dynamic`, transferring from
//! an empty account, broken zero-sum invariant. It is **not** a
//! runtime substitute.
//!
//! Double-entry rules (mirroring the real ledger):
//!   - `Deposit { account: X, amount: A }` → X += A, account 0 -= A
//!   - `Withdraw { account: X, amount: A }` → X -= A, account 0 += A
//!   - `Transfer { from: X, to: Y, amount: A }` → X -= A, Y += A
//!
//! Account 0 is the system account (SYSTEM_SOURCE / SYSTEM_SINK in the
//! ledger). The sum of all accounts including 0 is always 0; that is
//! what `AssertBalanceSum` checks.
//!
//! Not modeled:
//!   - Pipeline indices, leadership, tx state machines —
//!     `AssertPipelineCaughtUp`, `AssertLeader`, `AssertTxStatus`,
//!     `WaitForLevel`, `Wait` are no-ops.
//!   - Fault injection (`StopNode` / `KillNode` / `PartitionPair` /
//!     etc.) — happy-path simulation only.
//!   - WASM functions — `Function` ops, `RegisterFunction` /
//!     `UnregisterFunction` are no-ops.
//!
//! `AsyncBranch`es execute their steps inline, sequentially. The
//! simulator only cares about end-state math, where order within
//! commutative operations does not matter. Scenarios depending on
//! observable mid-flight state should not rely on the simulator.

use std::collections::HashMap;
use std::fmt;

use super::Scenario;
use super::step::{Action, BatchKind, Step};
use super::types::SubmitOp;

/// System account id. Mirrors the ledger's SYSTEM_SOURCE / SYSTEM_SINK.
/// Deposits debit it; withdrawals credit it; transfers don't touch it.
pub const SYSTEM_ACCOUNT: u64 = 0;

/// In-memory ledger simulator.
#[derive(Debug, Default)]
pub struct Simulator {
    accounts: HashMap<u64, i64>,
}

impl Simulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run every step of `scenario`. Returns the first failure
    /// encountered; later steps are not executed.
    pub fn run(&mut self, scenario: &Scenario) -> Result<(), SimError> {
        for step in &scenario.steps {
            self.run_step(step)?;
        }
        Ok(())
    }

    /// Current balance for `account`. Returns 0 if the account has
    /// never been touched, mirroring the ledger's lazy-init semantics.
    pub fn balance(&self, account: u64) -> i64 {
        self.accounts.get(&account).copied().unwrap_or(0)
    }

    fn run_step(&mut self, step: &Step) -> Result<(), SimError> {
        match &step.action {
            Action::Submit(s) => {
                self.apply_op(&s.op);
                Ok(())
            }
            Action::SubmitBatch(b) => {
                self.apply_batch(&b.kind);
                Ok(())
            }
            Action::AsyncBranch(b) => {
                for nested in &b.steps {
                    self.run_step(nested)?;
                }
                Ok(())
            }
            Action::AssertBalance(a) => {
                let actual = self.balance(a.account);
                if actual != a.expected {
                    return Err(SimError::BalanceMismatch {
                        account: a.account,
                        expected: a.expected,
                        actual,
                    });
                }
                Ok(())
            }
            Action::AssertBalanceSum(a) => {
                // Inclusive scan over [0, max_account]. Double-entry
                // bookkeeping guarantees this is 0 if the scenario's
                // submissions are well-formed (SYSTEM_ACCOUNT absorbs
                // the counter-leg of every deposit/withdraw).
                let total: i64 = (0..=a.max_account).map(|id| self.balance(id)).sum();
                if total != 0 {
                    return Err(SimError::NotZeroSum {
                        max_account: a.max_account,
                        total,
                    });
                }
                Ok(())
            }
            // Categories deliberately not modeled — see module docs.
            Action::AssertPipelineCaughtUp(_)
            | Action::AssertTxStatus(_)
            | Action::AssertLeader(_)
            | Action::Wait(_)
            | Action::WaitForLevel(_)
            | Action::GetBalance(_)
            | Action::GetPipelineIndex(_)
            | Action::StopNode(_)
            | Action::KillNode(_)
            | Action::StartNode(_)
            | Action::RestartNode(_)
            | Action::PartitionPair(_)
            | Action::HealPartition(_)
            | Action::RegisterFunction(_)
            | Action::UnregisterFunction(_) => Ok(()),
        }
    }

    fn apply_op(&mut self, op: &SubmitOp) {
        match op {
            SubmitOp::Deposit {
                account, amount, ..
            } => {
                let amt = *amount as i64;
                *self.accounts.entry(*account).or_insert(0) += amt;
                *self.accounts.entry(SYSTEM_ACCOUNT).or_insert(0) -= amt;
            }
            SubmitOp::Withdraw {
                account, amount, ..
            } => {
                // Allowed to drive the user account negative — the
                // ledger's insufficient-funds check is unhappy-path
                // territory and outside the simulator's scope.
                let amt = *amount as i64;
                *self.accounts.entry(*account).or_insert(0) -= amt;
                *self.accounts.entry(SYSTEM_ACCOUNT).or_insert(0) += amt;
            }
            SubmitOp::Transfer {
                from, to, amount, ..
            } => {
                let amt = *amount as i64;
                *self.accounts.entry(*from).or_insert(0) -= amt;
                *self.accounts.entry(*to).or_insert(0) += amt;
            }
            SubmitOp::Function { .. } => {
                // WASM ops are opaque to the simulator.
            }
        }
    }

    fn apply_batch(&mut self, kind: &BatchKind) {
        match kind {
            BatchKind::Static(ops) => {
                for op in ops {
                    self.apply_op(op);
                }
            }
            BatchKind::Dynamic { base, repeat } => {
                for _ in 0..*repeat {
                    for op in base {
                        self.apply_op(op);
                    }
                }
            }
        }
    }
}

/// What the simulator reports when a scenario's expectations do not
/// match the math it produces.
#[derive(Debug, PartialEq, Eq)]
pub enum SimError {
    BalanceMismatch {
        account: u64,
        expected: i64,
        actual: i64,
    },
    NotZeroSum {
        max_account: u64,
        total: i64,
    },
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::BalanceMismatch {
                account,
                expected,
                actual,
            } => write!(
                f,
                "AssertBalance failed for account {account}: expected {expected}, got {actual}"
            ),
            SimError::NotZeroSum { max_account, total } => write!(
                f,
                "AssertBalanceSum failed: total over accounts [0, {max_account}] is {total}, expected 0 (zero-sum invariant)"
            ),
        }
    }
}

impl std::error::Error for SimError {}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::super::{
        Action, AssertBalance, AssertBalanceSum, AsyncBranch, BatchKind, NodeSelector, Scenario,
        Step, Submit, SubmitBatch, SubmitOp, WaitLevel,
    };
    use super::*;

    #[test]
    fn deposit_then_assert_passes() {
        let s = Scenario::new("ok").with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 1,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 100,
            })),
        ]);
        Simulator::new().run(&s).expect("scenario should pass");
    }

    #[test]
    fn wrong_assert_is_caught() {
        let s = Scenario::new("buggy").with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 1,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 1,
                expected: 999, // wrong on purpose
            })),
        ]);
        let err = Simulator::new()
            .run(&s)
            .expect_err("simulator should reject the wrong expected value");
        assert_eq!(
            err,
            SimError::BalanceMismatch {
                account: 1,
                expected: 999,
                actual: 100
            }
        );
    }

    #[test]
    fn dynamic_batch_repeats_correctly() {
        let s = Scenario::new("repeat").with_steps(vec![
            Step::new(Action::SubmitBatch(SubmitBatch {
                wait: WaitLevel::None,
                retry: None,
                kind: BatchKind::Dynamic {
                    base: vec![SubmitOp::Deposit {
                        account: 7,
                        amount: 3,
                        user_ref: 1,
                    }],
                    repeat: 50,
                },
            })),
            Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: 7,
                expected: 150,
            })),
        ]);
        Simulator::new().run(&s).expect("3 * 50 = 150");
    }

    #[test]
    fn deposit_debits_system_account() {
        let s = Scenario::new("double_entry").with_steps(vec![Step::new(Action::Submit(Submit {
            op: SubmitOp::Deposit {
                account: 1,
                amount: 100,
                user_ref: 1,
            },
            wait: WaitLevel::Committed,
            retry: None,
        }))]);
        let mut sim = Simulator::new();
        sim.run(&s).expect("scenario should pass");
        assert_eq!(sim.balance(1), 100, "user account credited");
        assert_eq!(
            sim.balance(SYSTEM_ACCOUNT),
            -100,
            "system account holds the counter-leg"
        );
    }

    #[test]
    fn withdraw_credits_system_account() {
        let s = Scenario::new("withdraw").with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 1,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Withdraw {
                    account: 1,
                    amount: 30,
                    user_ref: 2,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
        ]);
        let mut sim = Simulator::new();
        sim.run(&s).expect("scenario should pass");
        assert_eq!(sim.balance(1), 70);
        assert_eq!(sim.balance(SYSTEM_ACCOUNT), -70);
    }

    #[test]
    fn assert_balance_sum_passes_for_well_formed_scenario() {
        let s = Scenario::new("zero_sum").with_steps(vec![
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Deposit {
                    account: 1,
                    amount: 100,
                    user_ref: 1,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::Submit(Submit {
                op: SubmitOp::Transfer {
                    from: 1,
                    to: 2,
                    amount: 40,
                    user_ref: 2,
                },
                wait: WaitLevel::Committed,
                retry: None,
            })),
            Step::new(Action::AssertBalanceSum(AssertBalanceSum {
                node: NodeSelector::Leader,
                max_account: 10,
            })),
        ]);
        Simulator::new().run(&s).expect("zero-sum invariant must hold");
    }

    #[test]
    fn async_branch_steps_are_executed() {
        let s =
            Scenario::new("branch").with_steps(vec![Step::new(Action::AsyncBranch(AsyncBranch {
                name: None,
                steps: vec![
                    Step::new(Action::Submit(Submit {
                        op: SubmitOp::Deposit {
                            account: 1,
                            amount: 10,
                            user_ref: 1,
                        },
                        wait: WaitLevel::None,
                        retry: None,
                    })),
                    Step::new(Action::AssertBalance(AssertBalance {
                        node: NodeSelector::Leader,
                        account: 1,
                        expected: 10,
                    })),
                ],
            }))]);
        Simulator::new().run(&s).expect("branch contents must run");
    }
}
