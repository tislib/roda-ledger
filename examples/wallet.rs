use roda_ledger::wallet::{Wallet, WalletConfig};

fn main() {
    // 1. Initialize the Wallet with a custom configuration
    // We'll use in-memory mode for this example to avoid creating files.
    let config = WalletConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    println!("Starting Roda-Ledger Wallet example...");
    let mut wallet = Wallet::new_with_config(config);
    wallet.start();

    // 2. Perform some operations
    let account_a = 101;
    let account_b = 102;

    println!("Depositing $1000 into account {}...", account_a);
    wallet.deposit(account_a, 1000);

    println!("Transferring $400 from {} to {}...", account_a, account_b);
    let tx_id = wallet.transfer(account_a, account_b, 400);

    // 3. Wait for transactions to be fully processed
    // Roda-Ledger is pipelined, so operations are asynchronous.
    // wait_pending_operations() ensures all submitted transactions reach the Snapshot phase.
    println!("Waiting for transactions to complete...");
    wallet.wait_pending_operations();

    // 4. Check balances
    let balance_a = wallet.get_balance(account_a);
    let balance_b = wallet.get_balance(account_b);

    println!("Account {} balance: ${}", account_a, balance_a);
    println!("Account {} balance: ${}", account_b, balance_b);

    // 5. Inspect transaction status
    let status = wallet.get_transaction_status(tx_id);
    println!("Transfer transaction status: {:?}", status);

    if status.is_committed() {
        println!("Verified: Transfer successfully committed to WAL and Snapshot.");
    }

    // 6. Cleanup
    wallet.destroy();
    println!("Example completed successfully.");
}
