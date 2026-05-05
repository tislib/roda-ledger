use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::{Operation, WaitLevel};

/// `Operation::NewTerm` is an internal cluster op that emits a
/// `TxMetadata` with a single `TxTerm` sub-item. Both records carry the
/// same `tx_id` semantically, but `TxTerm` is a structural record whose
/// `WalEntry::tx_id()` returns 0. The WAL runner must therefore track
/// the current tx_id from the most recent `Metadata` rather than from
/// the closing sub-item — otherwise `last_received_tx_id` regresses to
/// 0 and `commit_index` never advances past a NewTerm tx.
#[test]
fn new_term_advances_commit_index() {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().unwrap();

    let result = ledger.submit_and_wait(
        Operation::NewTerm {
            term: 1,
            node_id: 1,
            node_count: 3,
            node_voted: 2,
        },
        WaitLevel::Committed,
    );

    assert!(result.tx_id > 0, "tx_id should be assigned by the sequencer");
    assert!(
        result.fail_reason.is_success(),
        "NewTerm should not be rejected (got {:?})",
        result.fail_reason
    );
    assert!(
        ledger.last_commit_id() >= result.tx_id,
        "commit_index ({}) did not reach NewTerm tx_id ({})",
        ledger.last_commit_id(),
        result.tx_id
    );
}

/// Interleaving a NewTerm between two normal transfers must keep all
/// three committing in order — exercises the case where a
/// structural-tail tx sits between two regular txs in the WAL stream.
#[test]
fn new_term_interleaved_with_transfers_all_commit() {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().unwrap();

    let r1 = ledger.submit_and_wait(
        Operation::Deposit {
            account: 1,
            amount: 100,
            user_ref: 1,
        },
        WaitLevel::Committed,
    );
    assert!(r1.fail_reason.is_success());

    let r2 = ledger.submit_and_wait(
        Operation::NewTerm {
            term: 2,
            node_id: 1,
            node_count: 3,
            node_voted: 2,
        },
        WaitLevel::Committed,
    );
    assert!(r2.fail_reason.is_success());

    let r3 = ledger.submit_and_wait(
        Operation::Deposit {
            account: 2,
            amount: 50,
            user_ref: 2,
        },
        WaitLevel::Committed,
    );
    assert!(r3.fail_reason.is_success());

    assert!(r1.tx_id < r2.tx_id);
    assert!(r2.tx_id < r3.tx_id);
    assert!(
        ledger.last_commit_id() >= r3.tx_id,
        "commit_index ({}) did not reach final tx_id ({})",
        ledger.last_commit_id(),
        r3.tx_id
    );
}
