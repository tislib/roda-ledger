//! E2E (assertion-style) scenarios. Each scenario sets up a small,
//! deterministic situation and asserts the cluster ends up in the
//! expected state. Kept simple on purpose — these are the seed
//! scenarios; the catalogue grows as new shapes are needed.

use std::time::Duration;

use crate::scenario::{
    Action, AssertBalance, AssertBalanceSum, AssertLeader, BatchKind, Concurrent, KillNode,
    NodeSelector, PipelineLevel, RestartNode, RetryConfig, Scenario, StartNode, Step, Submit,
    SubmitBatch, SubmitOp, TxRef, Wait, WaitForLevel, WaitLevel,
};
use crate::scenarios::matrix_grid::MatrixGrid;

pub fn all() -> Vec<Scenario> {
    vec![
        single_deposit_committed(),
        transfer_chain(),
        kill_then_restart_recovers(),
        basic_crash_recovery_1m_tx(),
        crash_recovery_concurrent_multi(),
        matrix_transfer_grid(),
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

/// Build one `SubmitBatch` branch for the `Concurrent` crash-recovery
/// scenarios. Each super-batch's submitter writes `base.len() × repeat`
/// ops; chunk size is `chunk_size` so we dispatch `repeat` RPCs of
/// `chunk_size` ops each, every one waiting for cluster commit. With
/// `ClusterCommit`, a kill landing mid-RPC surfaces as an `Err` that
/// `cluster_client::with_leader_retry` rotates and resubmits with the
/// same `user_ref` — the dedup window collapses any duplicate that
/// happened to land on the new leader.
fn submit_branch(
    base: Vec<SubmitOp>,
    repeat: u32,
    chunk_size: u32,
    label: &'static str,
) -> Vec<Step> {
    vec![
        Step::new(Action::SubmitBatch(SubmitBatch {
            wait: WaitLevel::ClusterCommit,
            retry: Some(RetryConfig {
                max_retries: 30,
                backoff_ms: 200,
            }),
            rate: 0,
            kind: BatchKind::Dynamic {
                base,
                repeat,
                batch_size: chunk_size,
            },
        }))
        .with_label(label),
    ]
}

/// One kill-restart cycle in the chaos branch, prefixed by a head-
/// start `Wait` so the kill lands while the submitter's RPCs are
/// still in flight (rather than before they start). Sequential
/// inside the branch; runs concurrently with the submit branch.
fn chaos_kill_cycle(head_start: Duration) -> Vec<Step> {
    vec![
        Step::new(Action::Wait(Wait {
            duration: head_start,
        })),
        Step::new(Action::KillNode(KillNode {
            node: NodeSelector::Leader,
        })),
        Step::new(Action::StartNode(StartNode {
            node: NodeSelector::LastKilled,
        })),
    ]
}

/// 1 M deposits across 1 000 accounts (1 000 per account). Each super-
/// batch is one `Concurrent` block that races a single `SubmitBatch`
/// (cluster-commit waited) against a single kill-restart cycle. The
/// kill lands mid-flight; in-flight RPCs error,
/// `cluster_client::with_leader_retry` rotates to a surviving node,
/// resubmits with the same `user_ref`, dedup absorbs any duplicate.
/// The block joins when both branches complete — so the kill is
/// always mid-flight, but every emitted op is durably cluster-
/// committed by the time the step returns.
fn basic_crash_recovery_1m_tx() -> Scenario {
    const ACCOUNT_COUNT: u64 = 1_000;
    const BATCH_SIZE: u64 = 1_000;
    const BATCHES_PER_RESTART: u64 = 100;
    const TOTAL_BATCHES: u64 = 1_000;
    const TOTAL_OPS: u64 = TOTAL_BATCHES * BATCH_SIZE; // 1_000_000
    const TOTAL_SUPER_BATCHES: u64 = TOTAL_BATCHES / BATCHES_PER_RESTART; // 10

    let cap = (TOTAL_SUPER_BATCHES + ACCOUNT_COUNT + 2) as usize;
    let mut steps: Vec<Step> = Vec::with_capacity(cap);

    for super_idx in 0..TOTAL_SUPER_BATCHES {
        let user_ref_base = super_idx * BATCHES_PER_RESTART * BATCH_SIZE;
        let base: Vec<SubmitOp> = (0..BATCH_SIZE)
            .map(|i| SubmitOp::Deposit {
                account: (i % ACCOUNT_COUNT) + 1,
                amount: 1,
                user_ref: user_ref_base + i + 1,
            })
            .collect();

        steps.push(Step::new(Action::Concurrent(Concurrent {
            branches: vec![
                submit_branch(
                    base,
                    BATCHES_PER_RESTART as u32,
                    BATCH_SIZE as u32,
                    "submit",
                ),
                chaos_kill_cycle(Duration::from_millis(50)),
            ],
            labels: Some(vec!["submit".into(), "chaos".into()]),
        })));
    }

    // Drain: ensure the read-side snapshot has caught up to the last
    // emitted user_ref before per-account assertions. With cluster-
    // commit waits in every super-batch, the tx is durable on majority
    // by the time we get here — this barrier closes only the local
    // snapshot lag on the queried leader.
    steps.push(Step::new(Action::WaitForLevel(WaitForLevel {
        node: NodeSelector::Leader,
        tx: TxRef::UserRef(TOTAL_OPS),
        level: PipelineLevel::OnSnapshot,
    })));

    for account in 1..=ACCOUNT_COUNT {
        steps.push(Step::new(Action::AssertBalance(AssertBalance {
            node: NodeSelector::Leader,
            account,
            expected: TOTAL_BATCHES as i64,
        })));
    }

    steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
        node: NodeSelector::Leader,
        max_account: ACCOUNT_COUNT,
    })));

    Scenario::new("basic_crash_recovery_1m_tx")
        .with_description(
            "1M deposits across 1000 accounts (1000 per account). Each super-batch is a fork-join \
             `Concurrent` block racing one cluster-commit-waited `SubmitBatch` against a \
             kill-restart cycle; the kill lands mid-flight, `cluster_client::with_leader_retry` \
             rotates and resubmits with the same `user_ref`, dedup absorbs duplicates. \
             Per-account balance + zero-sum invariant prove no tx lost and no tx applied twice \
             across failovers.",
        )
        .with_steps(steps)
}

/// 1 M deposits across 1 000 accounts (1 000 per account) — same end
/// state as `basic_crash_recovery_1m_tx`, but the load shape is four
/// parallel submitters racing a chaos branch that runs three sequential
/// kill-restart pairs. Exercises:
///
/// - multi-writer pressure on the leader's submit queue (four
///   `SubmitBatch`es concurrently going through `with_leader_retry`)
/// - back-to-back failovers (three kills per scenario run against a
///   single big `Concurrent` block)
/// - cross-submitter `user_ref` disjointness (each submitter owns a
///   250 k-wide `user_ref` range so post-failover dedup never produces
///   binding collisions on join).
fn crash_recovery_concurrent_multi() -> Scenario {
    const ACCOUNT_COUNT: u64 = 1_000;
    const PER_SUBMITTER_OPS: u64 = 250_000;
    const TOTAL_OPS: u64 = PER_SUBMITTER_OPS * 4; // 1_000_000
    // Each submitter writes 1 deposit per account per repeat → 250
    // deposits per account from each of 4 submitters → 1000/acct.
    const REPEAT: u32 = (PER_SUBMITTER_OPS / ACCOUNT_COUNT) as u32; // 250
    const CHUNK_SIZE: u32 = ACCOUNT_COUNT as u32; // 1000

    fn build_base(user_ref_start: u64) -> Vec<SubmitOp> {
        (0..ACCOUNT_COUNT)
            .map(|i| SubmitOp::Deposit {
                account: i + 1,
                amount: 1,
                user_ref: user_ref_start + i + 1,
            })
            .collect()
    }

    let big_block = Step::new(Action::Concurrent(Concurrent {
        branches: vec![
            submit_branch(build_base(0), REPEAT, CHUNK_SIZE, "submit_a"),
            submit_branch(
                build_base(PER_SUBMITTER_OPS),
                REPEAT,
                CHUNK_SIZE,
                "submit_b",
            ),
            submit_branch(
                build_base(2 * PER_SUBMITTER_OPS),
                REPEAT,
                CHUNK_SIZE,
                "submit_c",
            ),
            submit_branch(
                build_base(3 * PER_SUBMITTER_OPS),
                REPEAT,
                CHUNK_SIZE,
                "submit_d",
            ),
            // Chaos: 3 sequential kill/restart pairs spaced by short
            // waits. The 50 ms head-start ensures the first kill lands
            // mid-flight; the 200 ms gaps let the new leader settle
            // and accept fresh submits before the next kill.
            vec![
                Step::new(Action::Wait(Wait {
                    duration: Duration::from_millis(50),
                })),
                Step::new(Action::KillNode(KillNode {
                    node: NodeSelector::Leader,
                })),
                Step::new(Action::StartNode(StartNode {
                    node: NodeSelector::LastKilled,
                })),
                Step::new(Action::Wait(Wait {
                    duration: Duration::from_millis(200),
                })),
                Step::new(Action::KillNode(KillNode {
                    node: NodeSelector::Leader,
                })),
                Step::new(Action::StartNode(StartNode {
                    node: NodeSelector::LastKilled,
                })),
                Step::new(Action::Wait(Wait {
                    duration: Duration::from_millis(200),
                })),
                Step::new(Action::KillNode(KillNode {
                    node: NodeSelector::Leader,
                })),
                Step::new(Action::StartNode(StartNode {
                    node: NodeSelector::LastKilled,
                })),
            ],
        ],
        labels: Some(vec![
            "submit_a".into(),
            "submit_b".into(),
            "submit_c".into(),
            "submit_d".into(),
            "chaos".into(),
        ]),
    }));

    let mut steps: Vec<Step> = Vec::with_capacity((2 + ACCOUNT_COUNT + 1) as usize);
    steps.push(big_block);

    // Drain on the last user_ref any submitter emitted (submitter D
    // owns the highest user_ref range).
    steps.push(Step::new(Action::WaitForLevel(WaitForLevel {
        node: NodeSelector::Leader,
        tx: TxRef::UserRef(TOTAL_OPS),
        level: PipelineLevel::OnSnapshot,
    })));

    let expected: i64 = (TOTAL_OPS / ACCOUNT_COUNT) as i64; // 1000
    for account in 1..=ACCOUNT_COUNT {
        steps.push(Step::new(Action::AssertBalance(AssertBalance {
            node: NodeSelector::Leader,
            account,
            expected,
        })));
    }

    steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
        node: NodeSelector::Leader,
        max_account: ACCOUNT_COUNT,
    })));

    Scenario::new("crash_recovery_concurrent_multi")
        .with_description(
            "1M deposits via four parallel cluster-commit-waited submitters racing three \
             back-to-back leader kill-restart cycles inside a single `Concurrent` block. \
             Per-submitter user_ref ranges are disjoint so binding merges never collide. \
             Per-account balance + zero-sum invariant prove no tx lost and no tx applied twice.",
        )
        .with_steps(steps)
}

/// Matrix grid transfer correctness — port of the legacy
/// `crates/testing/src/e2e/matrix-testing-scenario.md`.
///
/// An N×M grid of accounts; each cell transfers `TRANSFER_AMOUNT` to
/// its right and down neighbors every iteration. The topology produces
/// a closed-form expected balance per cell, so deterministic asserts
/// after each phase prove ledger correctness without re-implementing
/// transactor logic.
///
/// Phases:
///
/// 1. **Fund** every cell with `INITIAL_BALANCE`. Verify on snapshot.
/// 2. **Bulk run**: `ITERATIONS_BULK` iterations of the transfer
///    pattern, fire-and-forget; drain at the end and check every
///    cell's balance against the closed-form formula plus the
///    zero-sum invariant.
/// 3. **Checked run**: another `ITERATIONS_CHECKED` iterations, but
///    every iteration is followed by a snapshot drain and a full
///    grid verification. Catches transient consistency violations
///    that the bulk run would miss.
///
/// Expected-balance formula:
/// `balance(r, c, t) = INITIAL + (receives - sends) * TRANSFER * t`
/// where `sends = (c+1<M) + (r+1<N)` and `receives = (c>0) + (r>0)`.
fn matrix_transfer_grid() -> Scenario {
    const N: u64 = 10;
    const M: u64 = 10;
    const INITIAL_BALANCE: u64 = 10_000;
    const TRANSFER_AMOUNT: u64 = 10;
    const ITERATIONS_BULK: u64 = 20;
    const ITERATIONS_CHECKED: u64 = 20;

    let grid = MatrixGrid::new(N, M, INITIAL_BALANCE, TRANSFER_AMOUNT);
    let grid_accounts = grid.grid_accounts();
    let transfers_per_iter = grid.transfers.len();
    let bulk_transfers = ITERATIONS_BULK as usize * transfers_per_iter;
    let checked_transfers = ITERATIONS_CHECKED as usize * transfers_per_iter;

    // Step 1: 1 deposit batch + N*M asserts + 1 sum
    // Step 2: bulk_transfers submits + 1 drain + N*M asserts + 1 sum
    // Step 3: per iter: transfers_per_iter submits + 1 drain + N*M asserts + 1 sum
    let cap = 2
        + grid_accounts as usize
        + bulk_transfers
        + 2
        + grid_accounts as usize
        + checked_transfers
        + ITERATIONS_CHECKED as usize * (2 + grid_accounts as usize);
    let mut steps: Vec<Step> = Vec::with_capacity(cap);

    // ---- Step 1: fund every cell ----
    let deposits: Vec<SubmitOp> = grid
        .deposits
        .iter()
        .map(|&(account, amount)| SubmitOp::Deposit {
            account,
            amount,
            user_ref: account, // user_refs 1..=N*M
        })
        .collect();
    steps.push(
        Step::new(Action::SubmitBatch(SubmitBatch {
            wait: WaitLevel::OnSnapshot,
            retry: None,
            rate: 0,
            kind: BatchKind::Static(deposits),
        }))
        .with_label("fund grid"),
    );
    for r in 0..N {
        for c in 0..M {
            steps.push(Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: grid.account_id(r, c),
                expected: INITIAL_BALANCE as i64,
            })));
        }
    }
    steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
        node: NodeSelector::Leader,
        max_account: grid_accounts,
    })));

    // user_refs 1..=N*M used for deposits; transfers start at N*M+1.
    let mut next_user_ref: u64 = grid_accounts + 1;

    // ---- Step 2: bulk transfer run, no intermediate checks ----
    for _ in 0..ITERATIONS_BULK {
        for &(from, to, amount) in &grid.transfers {
            steps.push(Step::new(Action::Submit(Submit {
                op: SubmitOp::Transfer {
                    from,
                    to,
                    amount,
                    user_ref: next_user_ref,
                },
                wait: WaitLevel::None,
                retry: None,
            })));
            next_user_ref += 1;
        }
    }
    let last_bulk_user_ref = next_user_ref - 1;
    steps.push(
        Step::new(Action::WaitForLevel(WaitForLevel {
            node: NodeSelector::Leader,
            tx: TxRef::UserRef(last_bulk_user_ref),
            level: PipelineLevel::OnSnapshot,
        }))
        .with_label("drain bulk"),
    );
    for r in 0..N {
        for c in 0..M {
            steps.push(Step::new(Action::AssertBalance(AssertBalance {
                node: NodeSelector::Leader,
                account: grid.account_id(r, c),
                expected: grid.expected_balance(r, c, ITERATIONS_BULK),
            })));
        }
    }
    steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
        node: NodeSelector::Leader,
        max_account: grid_accounts,
    })));

    // ---- Step 3: checked transfer run, verify after each iteration ----
    for iter in 1..=ITERATIONS_CHECKED {
        for &(from, to, amount) in &grid.transfers {
            steps.push(Step::new(Action::Submit(Submit {
                op: SubmitOp::Transfer {
                    from,
                    to,
                    amount,
                    user_ref: next_user_ref,
                },
                wait: WaitLevel::None,
                retry: None,
            })));
            next_user_ref += 1;
        }
        let last_iter_user_ref = next_user_ref - 1;
        steps.push(Step::new(Action::WaitForLevel(WaitForLevel {
            node: NodeSelector::Leader,
            tx: TxRef::UserRef(last_iter_user_ref),
            level: PipelineLevel::OnSnapshot,
        })));
        let total_iters = ITERATIONS_BULK + iter;
        for r in 0..N {
            for c in 0..M {
                steps.push(Step::new(Action::AssertBalance(AssertBalance {
                    node: NodeSelector::Leader,
                    account: grid.account_id(r, c),
                    expected: grid.expected_balance(r, c, total_iters),
                })));
            }
        }
        steps.push(Step::new(Action::AssertBalanceSum(AssertBalanceSum {
            node: NodeSelector::Leader,
            max_account: grid_accounts,
        })));
    }

    Scenario::new("matrix_transfer_grid")
        .with_description(
            "10x10 grid of accounts; each cell transfers a fixed amount right+down every \
             iteration. Deterministic per-cell balance formula plus zero-sum invariant prove \
             ledger correctness. Step 2: bulk-fire 20 iterations, drain, verify. Step 3: \
             another 20 iterations, drain and verify after each one — catches transient \
             consistency bugs the bulk run would miss.",
        )
        .with_steps(steps)
}
