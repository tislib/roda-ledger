//! Matrix grid topology helper for transfer scenarios.
//!
//! Models an N×M grid of accounts where each cell can transfer a
//! fixed amount to its right and down neighbors. The interesting
//! property is the closed-form expected balance per cell, which lets
//! scenarios prove ledger correctness without re-implementing
//! transactor logic — every checkpoint compares the live balance to
//! a deterministic function of grid position and iteration count.

/// Precomputed grid topology for matrix transfer scenarios.
///
/// Account `0` is the system account; grid cells occupy IDs
/// `1..=N*M` in row-major order (see [`Self::account_id`]).
pub struct MatrixGrid {
    pub n: u64,
    pub m: u64,
    pub initial_balance: u64,
    pub transfer_amount: u64,

    /// All grid account IDs in row-major order. Useful for emitting
    /// `Deposit` ops or per-cell `AssertBalance` checks.
    pub account_ids: Vec<u64>,
    /// Per-iteration transfer list as `(from, to, amount)`. Each
    /// cell sends to its right and down neighbor when those exist.
    pub transfers: Vec<(u64, u64, u64)>,
    /// Per-cell deposit list as `(account_id, amount)`, sized
    /// `N*M` at `initial_balance` each.
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

    /// Total grid accounts (`N * M`). Does not include the system account.
    pub fn grid_accounts(&self) -> u64 {
        self.n * self.m
    }

    /// Account ID for grid cell `(r, c)`.
    pub fn account_id(&self, r: u64, c: u64) -> u64 {
        r * self.m + c + 1
    }

    /// `(r, c)` for an account ID. Inverse of [`Self::account_id`].
    pub fn grid_position(&self, account: u64) -> (u64, u64) {
        let id = account - 1;
        (id / self.m, id % self.m)
    }

    /// Expected balance for cell `(r, c)` after `t` total transfer
    /// iterations have been applied. Derives directly from the
    /// topology: a cell sends to its right neighbor when `c+1<M` and
    /// to its down neighbor when `r+1<N`; it receives from its left
    /// when `c>0` and from above when `r>0`. The net per-iteration
    /// change is `(receives - sends) * transfer_amount`.
    pub fn expected_balance(&self, r: u64, c: u64, t: u64) -> i64 {
        let sends = (if c + 1 < self.m { 1 } else { 0 }) + (if r + 1 < self.n { 1 } else { 0 });
        let receives = (if c > 0 { 1 } else { 0 }) + (if r > 0 { 1 } else { 0 });

        self.initial_balance as i64 + (receives - sends) * self.transfer_amount as i64 * t as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_id_row_major_round_trips() {
        let g = MatrixGrid::new(3, 4, 1000, 10);
        for r in 0..g.n {
            for c in 0..g.m {
                let id = g.account_id(r, c);
                assert_eq!(g.grid_position(id), (r, c));
            }
        }
        // First cell is 1 (system account is 0), last is N*M.
        assert_eq!(g.account_id(0, 0), 1);
        assert_eq!(g.account_id(2, 3), 12);
    }

    #[test]
    fn transfer_list_matches_topology() {
        let g = MatrixGrid::new(3, 3, 1000, 10);
        // 3x3: 3*2 right + 3*2 down = 12 transfers per iter.
        assert_eq!(g.transfers.len(), 12);
        // Corner (0,0) sends right to (0,1) and down to (1,0).
        assert!(g.transfers.contains(&(1, 2, 10)));
        assert!(g.transfers.contains(&(1, 4, 10)));
        // Corner (2,2) sends nowhere.
        assert!(!g.transfers.iter().any(|(from, _, _)| *from == 9));
    }

    #[test]
    fn expected_balance_matches_doc_examples() {
        // Mirrors the table in matrix-testing-scenario.md for 10x10,
        // initial=10_000, transfer=10, T=100.
        let g = MatrixGrid::new(10, 10, 10_000, 10);
        assert_eq!(g.expected_balance(0, 0, 100), 8_000); // top-left corner: -20/iter
        assert_eq!(g.expected_balance(0, 9, 100), 10_000); // top-right corner: 0/iter
        assert_eq!(g.expected_balance(9, 0, 100), 10_000); // bottom-left corner: 0/iter
        assert_eq!(g.expected_balance(9, 9, 100), 12_000); // bottom-right corner: +20/iter
        assert_eq!(g.expected_balance(0, 5, 100), 9_000); // top edge: -10/iter
        assert_eq!(g.expected_balance(5, 0, 100), 9_000); // left edge: -10/iter
        assert_eq!(g.expected_balance(5, 9, 100), 11_000); // right edge: +10/iter
        assert_eq!(g.expected_balance(9, 5, 100), 11_000); // bottom edge: +10/iter
        assert_eq!(g.expected_balance(5, 5, 100), 10_000); // interior: 0/iter
    }

    #[test]
    fn deposits_cover_every_cell_exactly_once() {
        let g = MatrixGrid::new(4, 5, 1000, 10);
        assert_eq!(g.deposits.len() as u64, g.grid_accounts());
        let mut ids: Vec<u64> = g.deposits.iter().map(|(id, _)| *id).collect();
        ids.sort();
        let expected: Vec<u64> = (1..=g.grid_accounts()).collect();
        assert_eq!(ids, expected);
        assert!(g.deposits.iter().all(|(_, amt)| *amt == 1000));
    }
}
