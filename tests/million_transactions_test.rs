use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;

#[test]
fn million_deposits_final_balance() {
    let total_txs: u64 = 1_000_000;
    let mut ledger = Ledger::new(LedgerConfig {
        queue_size: 1024,
        location: None,
        in_memory: true,
        ..Default::default()
    });
    ledger.start();

    // Generate 1,000,000 deposit transactions of amount 1 into account 1
    let mut last_id = 0;
    for _ in 0..total_txs {
        last_id = ledger.submit(Operation::Deposit {
            account: 1,
            amount: 1,
            user_ref: 0,
        });
    }

    ledger.wait_for_transaction(last_id);

    let balance = ledger.get_balance(1);
    assert_eq!(balance, total_txs as i64);

    let system_balance = ledger.get_balance(0);
    assert_eq!(system_balance, -(total_txs as i64));
}
