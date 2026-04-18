//! Custom-transaction example.
//!
//! Registers a WASM `Function` that performs a transfer-with-fee
//! atomically (sender pays `amount + fee`, receiver gets `amount`,
//! fee account gets `fee`) and invokes it through the normal
//! `Operation::Function` path.
//!
//! This is the pattern replacing the old (removed) `Composite`
//! operation: arbitrary multi-account, multi-step atomic logic is now
//! expressed as a registered WASM function.

use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::storage::StorageConfig;
use roda_ledger::transaction::{Operation, WaitLevel};

/// Transfer-with-fee function, expressed as WAT.
///
/// Parameters:
///   param0: sender account id
///   param1: receiver account id
///   param2: fee account id
///   param3: amount
///   param4: fee
///
/// Host-side semantics: `credit` subtracts, `debit` adds. Zero-sum
/// holds because `credit(sender, amount+fee) == debit(receiver, amount)
/// + debit(fee, fee)`.
const TRANSFER_WITH_FEE_WAT: &str = r#"
    (module
      (import "ledger" "credit" (func $credit (param i64 i64)))
      (import "ledger" "debit"  (func $debit  (param i64 i64)))
      (func (export "execute")
        (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
        ;; credit(sender, amount + fee)
        local.get 0
        local.get 3 local.get 4 i64.add
        call $credit
        ;; debit(receiver, amount)
        local.get 1 local.get 3 call $debit
        ;; debit(fee_account, fee)
        local.get 2 local.get 4 call $debit
        i32.const 0))
"#;

fn main() {
    let config = LedgerConfig {
        storage: StorageConfig {
            temporary: true,
            ..Default::default()
        },
        ..Default::default()
    };

    println!("Starting custom-transaction (WASM Function) example...");
    let mut ledger = Ledger::new(config);
    ledger.start().unwrap();

    let item_id = 42;

    // Add 100 items using Deposit.
    println!("Adding 100 items to ID {}...", item_id);
    ledger.submit(Operation::Deposit {
        account: item_id,
        amount: 100,
        user_ref: 0,
    });

    // Try to remove 150 items using Withdrawal (should fail: insufficient funds).
    println!("Attempting to remove 150 items (should fail)...");
    let fail_tx_id = ledger.submit(Operation::Withdrawal {
        account: item_id,
        amount: 150,
        user_ref: 0,
    });

    // Remove 30 items successfully.
    println!("Removing 30 items...");
    let _success_tx_id = ledger.submit(Operation::Withdrawal {
        account: item_id,
        amount: 30,
        user_ref: 0,
    });

    // Register the transfer-with-fee WASM function.
    let user_a = 101;
    let user_b = 102;
    let fee_account = 0;

    ledger.submit(Operation::Deposit {
        account: user_a,
        amount: 1000,
        user_ref: 0,
    });

    println!("Registering WASM transfer_with_fee function...");
    let binary = wat::parse_str(TRANSFER_WITH_FEE_WAT).expect("wat parse");
    let (version, crc) = ledger
        .register_function("transfer_with_fee", &binary, false)
        .expect("register");
    println!("  → registered v{} (crc={:08x})", version, crc);

    println!("Invoking transfer_with_fee(user_a, user_b, fee_account, 100, 5)...");
    let result = ledger.submit_and_wait(
        Operation::Function {
            name: "transfer_with_fee".into(),
            params: [
                user_a as i64,
                user_b as i64,
                fee_account as i64,
                100,
                5,
                0,
                0,
                0,
            ],
            user_ref: 12345,
        },
        WaitLevel::OnSnapshot,
    );
    assert!(
        result.fail_reason.is_success(),
        "transfer_with_fee failed: {:?}",
        result.fail_reason
    );

    // Check final balances.
    println!("Final balances:");
    println!("Item {}: {}", item_id, ledger.get_balance(item_id));
    println!("User A: {}", ledger.get_balance(user_a));
    println!("User B: {}", ledger.get_balance(user_b));
    println!("Fee Account: {}", ledger.get_balance(fee_account));

    // Check failure reason for the 150 withdrawal.
    let status = ledger.get_transaction_status(fail_tx_id);
    if status.is_err() {
        println!(
            "Expected error for transaction {}: {:?}",
            fail_tx_id,
            status.error_reason()
        );
    }
}
