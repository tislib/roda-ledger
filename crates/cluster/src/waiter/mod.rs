//! Shared pipeline-wait primitive. A single [`Waiter`] per node, held
//! behind `Arc`, blocks until a transaction reaches a wait level. Both
//! the ledger handler (for `*_and_wait` RPCs) and the latency-probe
//! handler share the one instance.

pub mod wait_for_transaction_level;

use crate::consensus::state::Consensus;
use crate::ledger_slot::LedgerSlot;
use std::sync::Arc;

pub struct Waiter {
    ledger: Arc<LedgerSlot>,
    consensus: Arc<Consensus>,
}

impl Waiter {
    pub fn new(ledger: Arc<LedgerSlot>, consensus: Arc<Consensus>) -> Self {
        Self { ledger, consensus }
    }
}
