# Library Mode (Rust)

Integrate Roda-Ledger directly into your Rust application for maximum performance.

## 1. Add Dependency

Add this to your `Cargo.toml`:

```toml
[dependencies]
roda-ledger = { git = "https://github.com/tislib/roda-ledger" }
```

## 2. Basic Usage

```rust
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::Operation;

fn main() {
    // Initialize with default config
    let mut ledger = Ledger::new(LedgerConfig::default());
    ledger.start();

    // Submit a deposit
    let tx_id = ledger.submit(Operation::Deposit {
        account: 1,
        amount: 1000,
        user_ref: 0
    });

    // Wait for the transaction to be persisted (WAL)
    ledger.wait_for_transaction(tx_id);

    // Retrieve balance
    let balance = ledger.get_balance(1);
    println!("Account 1 balance: {}", balance);
}
```

For more examples, see the [`examples/`](https://github.com/tislib/roda-ledger/tree/master/examples) directory in the repository.
