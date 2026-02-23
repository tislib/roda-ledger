use bytemuck::{Pod, Zeroable};
use roda_ledger::balance::BalanceDataType;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};

// 1. Define your custom balance type
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, Default)]
pub struct InventoryBalance {
    pub item_count: u64,
}

impl BalanceDataType for InventoryBalance {}

// 2. Define your custom transaction type
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, Default)]
pub struct InventoryAdjust {
    pub item_id: u64,
    pub delta: i64,
}

impl TransactionDataType for InventoryAdjust {
    type BalanceData = InventoryBalance;

    fn process(
        &self,
        ctx: &mut impl TransactionExecutionContext<Self::BalanceData>,
    ) -> Result<(), String> {
        let mut balance = ctx.get_balance(self.item_id);

        if self.delta < 0 && balance.item_count < self.delta.unsigned_abs() {
            return Err("Insufficient inventory".to_string());
        }

        if self.delta < 0 {
            balance.item_count -= self.delta.unsigned_abs();
        } else {
            balance.item_count += self.delta as u64;
        }

        ctx.update_balance(self.item_id, balance);
        Ok(())
    }
}

fn main() {
    let config = LedgerConfig {
        in_memory: true,
        ..Default::default()
    };

    println!("Starting Custom Transaction example...");
    let mut ledger = Ledger::<InventoryAdjust, InventoryBalance>::new(config);
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
        item_id, final_balance.item_count
    );

    // Check failure reason
    let status = ledger.get_transaction_status(fail_tx_id);
    if status.is_err() {
        println!(
            "Expected error for transaction {}: {}",
            fail_tx_id,
            status.error_reason()
        );
    }

    // drop(ledger) will stop background threads
}
