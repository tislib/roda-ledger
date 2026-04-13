//! Matrix grid — shared topology and math for matrix transfer tests.
//!
//! See `tests/e2e/matrix-testing-scenario.md` for the full specification.
//!
//! This module provides the grid construction, account ID mapping, and
//! expected balance formula. Test logic (iterations, verification, assertions)
//! lives in the test files, not here.

/// Precomputed grid topology for the matrix transfer scenario.
pub struct MatrixGrid {
    pub n: u64,
    pub m: u64,
    pub initial_balance: u64,
    pub transfer_amount: u64,

    /// All grid account IDs in row-major order.
    pub account_ids: Vec<u64>,
    /// Transfer list for one iteration: `(from, to, amount)`.
    pub transfers: Vec<(u64, u64, u64)>,
    /// Deposit list for funding: `(account_id, amount)`.
    pub deposits: Vec<(u64, u64)>,
}

impl MatrixGrid {
    pub fn new(n: u64, m: u64, initial_balance: u64, transfer_amount: u64) -> Self {
        let account_ids: Vec<u64> = (0..n)
            .flat_map(|r| (0..m).map(move |c| r * m + c + 1))
            .collect();

        let deposits: Vec<(u64, u64)> = account_ids
            .iter()
            .map(|&id| (id, initial_balance))
            .collect();

        let mut transfers = Vec::new();
        for r in 0..n {
            for c in 0..m {
                let from = r * m + c + 1;
                if c + 1 < m {
                    transfers.push((from, r * m + (c + 1) + 1, transfer_amount));
                }
                if r + 1 < n {
                    transfers.push((from, (r + 1) * m + c + 1, transfer_amount));
                }
            }
        }

        Self {
            n,
            m,
            initial_balance,
            transfer_amount,
            account_ids,
            transfers,
            deposits,
        }
    }

    /// Total grid accounts (N × M). Does not include system account 0.
    pub fn grid_accounts(&self) -> u64 {
        self.n * self.m
    }

    /// Account ID for grid cell (r, c). Account 0 is the system account.
    pub fn account_id(&self, r: u64, c: u64) -> u64 {
        r * self.m + c + 1
    }

    /// Expected balance for cell (r, c) after `t` total iterations.
    pub fn expected_balance(&self, r: u64, c: u64, t: u64) -> i64 {
        let sends = (if c + 1 < self.m { 1 } else { 0 }) + (if r + 1 < self.n { 1 } else { 0 });
        let receives = (if c > 0 { 1 } else { 0 }) + (if r > 0 { 1 } else { 0 });

        self.initial_balance as i64 + (receives - sends) * self.transfer_amount as i64 * t as i64
    }

    /// (r, c) for an account ID. Inverse of `account_id`.
    pub fn grid_position(&self, account: u64) -> (u64, u64) {
        let id = account - 1;
        (id / self.m, id % self.m)
    }
}
