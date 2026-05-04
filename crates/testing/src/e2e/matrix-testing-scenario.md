# Matrix Testing Scenario

## Overview

A deterministic integration test that exercises complex balance movements across an N×M grid of accounts. Each cell transfers funds to its right and bottom neighbors over multiple iterations. The topology produces predictable per-cell balances without reimplementing transactor logic — expected values derive purely from grid position.

## Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `N` (rows) | 10 | Grid height |
| `M` (cols) | 10 | Grid width |
| `initial_balance` | 10,000 | Deposit to every cell in Step 1 |
| `iteration_count` | 100 | Number of transfer rounds per step |
| `transfer_amount` | 10 | Amount per transfer per direction per iteration |

Account ID mapping: `account_id(r, c) = r × M + c + 1` (account 0 is the system account).

## Transfer Topology

Each cell `(r, c)` transfers `transfer_amount` to:

- **Right** neighbor `(r, c+1)` — if `c + 1 < M`
- **Down** neighbor `(r+1, c)` — if `r + 1 < N`

No diagonal transfers. No self-transfers.

```
(0,0) → (0,1) → (0,2) → ...
  ↓       ↓       ↓
(1,0) → (1,1) → (1,2) → ...
  ↓       ↓       ↓
 ...     ...     ...
```

## Expected Balance Formula

Per iteration, each cell sends to `S` neighbors and receives from `R` neighbors:

| Direction | Sends to | Receives from |
|-----------|----------|---------------|
| Right     | `c + 1 < M` | `c > 0` (from left) |
| Down      | `r + 1 < N` | `r > 0` (from above) |

```
S(r, c) = (c + 1 < M ? 1 : 0) + (r + 1 < N ? 1 : 0)
R(r, c) = (c > 0 ? 1 : 0) + (r > 0 ? 1 : 0)

balance(r, c, T) = initial_balance + (R - S) × transfer_amount × T
```

Where `T` is the total number of completed iterations.

### Balance by Grid Position

| Position | S | R | Net/iter | After 100 iters | After 200 iters |
|----------|---|---|----------|-----------------|-----------------|
| Top-left corner `(0,0)` | 2 | 0 | −20 | 8,000 | 6,000 |
| Top-right corner `(0,M−1)` | 1 | 1 | 0 | 10,000 | 10,000 |
| Bottom-left corner `(N−1,0)` | 1 | 1 | 0 | 10,000 | 10,000 |
| Bottom-right corner `(N−1,M−1)` | 0 | 2 | +20 | 12,000 | 14,000 |
| Top edge (not corner) | 2 | 1 | −10 | 9,000 | 8,000 |
| Left edge (not corner) | 2 | 1 | −10 | 9,000 | 8,000 |
| Right edge (not corner) | 1 | 2 | +10 | 11,000 | 12,000 |
| Bottom edge (not corner) | 1 | 2 | +10 | 11,000 | 12,000 |
| Interior | 2 | 2 | 0 | 10,000 | 10,000 |

Worst-case cell is `(0,0)`: after 200 total iterations its balance is 6,000 — well above zero. No transfers should fail due to insufficient funds.

## Execution Steps

### Step 1 — Fund All Accounts

Deposit `initial_balance` to every cell in the grid. Wait for all transactions to reach snapshot level. Verify every cell has balance equal to `initial_balance`.

### Step 2 — Bulk Transfer Run (No Intermediate Checks)

Execute `iteration_count` iterations of the transfer pattern. Each iteration submits all transfers without waiting between them.

After all iterations complete, wait for the last transaction to reach snapshot level, then verify:

1. Every cell's balance matches `balance(r, c, iteration_count)`.
2. Global zero-sum: sum of all account balances (including system account 0) equals zero.
3. Zero rejected transactions.

### Step 3 — Checked Transfer Run (Verify After Each Iteration)

Execute another `iteration_count` iterations. After **each** iteration:

1. Wait for the last transaction in that iteration to reach committed level.
2. Verify every cell's balance matches `balance(r, c, T)` where `T = iteration_count + current_iteration`.
3. Verify global zero-sum.
4. Verify zero rejected transactions.

This step catches any transient consistency violations that Step 2 would miss.

## Invariants Verified

- **Per-cell balance correctness**: deterministic expected value at every checkpoint.
- **Global zero-sum**: total across all accounts (including system account 0) is always zero.
- **No rejected transactions**: with the chosen parameters, no cell should ever have insufficient funds.
- **Monotonic transaction IDs**: implicitly verified by the pipeline.
- **Consistency under load**: Step 3's per-iteration verification catches transient pipeline inconsistencies.

## Failure Modes Detected

- Balance leaks or double-counting in the transactor.
- Snapshot race conditions (transactions attributed to wrong segment).
- WAL commit ordering violations.
- Pipeline backpressure causing dropped or reordered transactions.
- Incorrect rollback on failed transactions (should not occur here, but would propagate as balance drift).