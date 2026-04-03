use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;

fn main() {
    // 1. Initialize the Ledger with a custom configuration
    // We'll use in-memory mode for this example to avoid creating files.
    let config = LedgerConfig {
        queue_size: 1024,
        ..Default::default()
    };

    println!("Starting Roda-Ledger example...");
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    // 2. Perform some operations
    let account_a = 101;
    let account_b = 102;

    println!("Depositing $1000 into account {}...", account_a);
    ledger.submit(Operation::Deposit {
        account: account_a,
        amount: 1000,
        user_ref: 0,
    });

    println!("Transferring $400 from {} to {}...", account_a, account_b);
    let tx_id = ledger.submit(Operation::Transfer {
        from: account_a,
        to: account_b,
        amount: 400,
        user_ref: 0,
    });

    // 3. Wait for transactions to be fully processed
    // Roda-Ledger is pipelined, so operations are asynchronous.
    println!("Waiting for transactions to complete...");
    ledger.wait_for_transaction(tx_id);

    // 4. Check balances
    let balance_a = ledger.get_balance(account_a);
    let balance_b = ledger.get_balance(account_b);

    println!("Account {} balance: ${}", account_a, balance_a);
    println!("Account {} balance: ${}", account_b, balance_b);

    // 5. Inspect transaction status
    let status = ledger.get_transaction_status(tx_id);
    println!("Transfer transaction status: {:?}", status);

    if status.is_committed() {
        println!("Verified: Transfer successfully committed to WAL and Snapshot.");
    }

    // 6. Cleanup
    // Ledger is dropped automatically when it goes out of scope, stopping its threads.
    println!("Example completed successfully.");
}
