use bytemuck::{Pod, Zeroable};
use roda_ledger::balance::Balance;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use roda_ledger::entities::{FailReason, TxEntry};

// 1. Balance is now non-generic u64 everywhere.
// You can still wrap it in your own logic inside transaction processing.

// 2. Define your custom transaction type
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, Default)]
pub struct InventoryAdjust {
    pub item_id: u64,
    pub delta: i64,
}

impl TransactionDataType for InventoryAdjust {
    fn process(
        &self,
        ctx: &mut TransactionExecutionContext<'_>,
    ) {
        if self.delta < 0 {
            ctx.credit(self.item_id, self.delta.unsigned_abs());
        } else {
            ctx.debit(self.item_id, self.delta as u64);
        }
    }
}

fn main() {
    let config = LedgerConfig {
        in_memory: true,
        ..Default::default()
    };

    println!("Starting Custom Transaction example...");
    let mut ledger = Ledger::<InventoryAdjust>::new(config);
    ledger.start();

    let item_id = 42;

    // Add 100 items
    println!("Adding 100 items to ID {}...", item_id);
    ledger.submit(Transaction::new(InventoryAdjust {
        item_id,
        delta: 100,
    }));

    // Try to remove 150 items (should fail)
    println!("Attempting to remove 150 items (should fail)...");
    let fail_tx_id = ledger.submit(Transaction::new(InventoryAdjust {
        item_id,
        delta: -150,
    }));

    // Remove 30 items (should succeed)
    println!("Removing 30 items...");
    let success_tx_id = ledger.submit(Transaction::new(InventoryAdjust {
        item_id,
        delta: -30,
    }));

    // Wait for everything
    ledger.wait_for_transaction(success_tx_id);

    // Check final balance
    let final_balance = ledger.get_balance(item_id);
    println!(
        "Final inventory for item {}: {}",
        item_id, final_balance
    );

    // Check failure reason
    let status = ledger.get_transaction_status(fail_tx_id);
    if status.is_err() {
        println!(
            "Expected error for transaction {}: {:?}",
            fail_tx_id,
            status.error_reason()
        );
    }

    // drop(ledger) will stop background threads
}
