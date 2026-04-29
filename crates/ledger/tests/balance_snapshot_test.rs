use ledger::ledger::{Ledger, LedgerConfig};
use ledger::transaction::{Operation, WaitLevel};

#[test]
fn test_balance_always_guaranteed_when_wait_level_is_snapshotted() {
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().unwrap();

    let account_id = 1;
    let mut expected_balance = 0;
    let iterations = 100;

    for i in 0..iterations {
        let amount = 1;
        let result = ledger.submit_and_wait(
            Operation::Deposit {
                account: account_id,
                amount,
                user_ref: i as u64,
            },
            WaitLevel::OnSnapshot,
        );

        assert!(
            result.fail_reason.is_success(),
            "Deposit failed at iteration {}",
            i
        );
        expected_balance += amount as i64;

        let balance = ledger.get_balance(account_id);
        assert_eq!(
            balance, expected_balance,
            "Balance mismatch at iteration {}",
            i
        );
    }
}
