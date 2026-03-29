use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{ComplexOperation, ComplexOperationFlags, Operation, Step};
use smallvec::smallvec;

fn main() {
    let config = LedgerConfig {
        in_memory: true,
        ..Default::default()
    };

    println!("Starting Complex Operation example...");
    let mut ledger = Ledger::new(config);
    ledger.start();

    let item_id = 42;

    // Add 100 items using Deposit
    println!("Adding 100 items to ID {}...", item_id);
    ledger.submit(Operation::Deposit {
        account: item_id,
        amount: 100,
        user_ref: 0,
    });

    // Try to remove 150 items using Withdrawal (should fail due to insufficient funds)
    println!("Attempting to remove 150 items (should fail)...");
    let fail_tx_id = ledger.submit(Operation::Withdrawal {
        account: item_id,
        amount: 150,
        user_ref: 0,
    });

    // Remove 30 items using Withdrawal (should succeed)
    println!("Removing 30 items...");
    let _success_tx_id = ledger.submit(Operation::Withdrawal {
        account: item_id,
        amount: 30,
        user_ref: 0,
    });

    // Complex operation: Transfer with a fee
    let user_a = 101;
    let user_b = 102;
    let fee_account = 0;

    ledger.submit(Operation::Deposit {
        account: user_a,
        amount: 1000,
        user_ref: 0,
    });

    println!("Executing complex operation: Transfer 100 with 5 fee...");
    let complex_tx_id = ledger.submit(Operation::Complex(Box::new(ComplexOperation {
        steps: smallvec![
            Step::Credit {
                account_id: user_a,
                amount: 105
            }, // Pay 100 + 5 fee
            Step::Debit {
                account_id: user_b,
                amount: 100
            }, // Receiver gets 100
            Step::Debit {
                account_id: fee_account,
                amount: 5
            }, // System gets 5 fee
        ],
        flags: ComplexOperationFlags::empty(),
        user_ref: 12345,
    })));

    // Wait for everything
    ledger.wait_for_transaction(complex_tx_id);

    // Check final balances
    println!("Final balances:");
    println!("Item {}: {}", item_id, ledger.get_balance(item_id));
    println!("User A: {}", ledger.get_balance(user_a));
    println!("User B: {}", ledger.get_balance(user_b));
    println!("Fee Account: {}", ledger.get_balance(fee_account));

    // Check failure reason for the 150 withdrawal
    let status = ledger.get_transaction_status(fail_tx_id);
    if status.is_err() {
        println!(
            "Expected error for transaction {}: {:?}",
            fail_tx_id,
            status.error_reason()
        );
    }
}
