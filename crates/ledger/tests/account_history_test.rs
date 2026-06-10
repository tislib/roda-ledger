//! `Ledger::get_account_history` (ADR-022) — WAL-backed account history via
//! `WalScanner`: transactions touching an account, newest→oldest, over a
//! `[to_tx_id, from_tx_id]` window, with `scan_last_tx_id` for pagination.

use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::{CommittedTransaction, Operation, WaitLevel};

fn start() -> Ledger {
    let mut l = Ledger::new(LedgerConfig::temp());
    l.start().expect("start");
    l
}

fn deposit(l: &Ledger, account: u64, amount: u64) {
    l.submit_and_wait(
        Operation::Deposit {
            account,
            amount,
            user_ref: 0,
        },
        WaitLevel::OnSnapshot,
    );
}

fn ids(txs: &[CommittedTransaction]) -> Vec<u64> {
    txs.iter().map(|t| t.meta.tx_id).collect()
}

#[test]
fn account_history_newest_first_filtered_windowed_paginated() {
    let l = start();
    l.open_accounts(10); // tx 1: AccountOpened(1..=10) — covers accounts 5 and 7
    deposit(&l, 5, 100); // tx 2
    deposit(&l, 7, 200); // tx 3 — a different account
    deposit(&l, 5, 300); // tx 4
    deposit(&l, 5, 400); // tx 5

    // Full history (to_tx_id = 0 → scan to the WAL start): account-5 deposits +
    // the open that created it, newest→oldest.
    let all = l.get_account_history(5, 0, 0);
    assert_eq!(ids(&all.transactions), vec![5, 4, 2, 1]);
    // The whole transaction is returned: tx5 carries its debit(5)+credit(SYSTEM).
    assert_eq!(all.transactions[0].entries.len(), 2);

    // Window [3, latest]: the scan stops below tx 3, so only tx5, tx4 (tx3 is
    // account 7), and reports the floor it reached.
    let win = l.get_account_history(5, 0, 3);
    assert_eq!(ids(&win.transactions), vec![5, 4]);
    assert_eq!(
        win.scan_last_tx_id, 2,
        "stopped at the first tx below the floor"
    );

    // from_tx_id starts the scan there (tx5, tx4 skipped); to_tx_id 0 = to start.
    assert_eq!(
        ids(&l.get_account_history(5, 3, 0).transactions),
        vec![2, 1]
    );

    // Account 7 only shows its own deposit (tx3) + the covering open (tx1).
    assert_eq!(
        ids(&l.get_account_history(7, 0, 0).transactions),
        vec![3, 1]
    );

    // Opened-but-untouched account: just the covering open.
    assert_eq!(ids(&l.get_account_history(9, 0, 0).transactions), vec![1]);

    // Never-opened account: empty history.
    assert!(l.get_account_history(9999, 0, 0).transactions.is_empty());
}
