use crate::pipeline::SequencerContext;
use crate::transaction::{Operation, Transaction, TransactionBatch, TransactionInput};
use std::hint::spin_loop;
use std::thread::yield_now;

/// Synchronous sequencer stage. Not a thread runner — invoked from
/// `Ledger::submit`. Queue and next-id index live in `Pipeline`; this struct
/// is a thin wrapper around `SequencerContext` exposing the submit/recovery
/// API.
pub struct Sequencer {
    ctx: SequencerContext,
}

impl Sequencer {
    pub fn new(ctx: SequencerContext) -> Self {
        Self { ctx }
    }

    /// Stamp the transaction with a freshly sequenced id and push it onto
    /// the sequencer→transactor queue, blocking (spin/yield) until there is
    /// space. Returns the assigned id.
    #[inline(always)]
    pub fn submit(&self, operation: Operation) -> u64 {
        let mut transaction = Transaction::new(operation);
        let id = self.ctx.fetch_next_id(1);
        transaction.id = id;

        let outbound = self.ctx.output();
        let mut retry_count = 0u64;
        while let Err(t) = outbound.push(TransactionInput::Single(transaction)) {
            transaction = t.single();
            spin_loop();
            retry_count += 1;
            if retry_count.is_multiple_of(10_000) {
                yield_now();
            }
        }

        id
    }

    #[inline(always)]
    pub fn submit_batch(&self, operations: Vec<Operation>) -> u64 {
        let start_id = self.ctx.fetch_next_id(operations.len() as u64);
        let mut transaction_batch = TransactionBatch {
            start_tx_id: start_id,
            operations,
        };

        let outbound = self.ctx.output();
        let mut retry_count = 0u64;
        while let Err(t) = outbound.push(TransactionInput::Batch(transaction_batch)) {
            transaction_batch = t.batch();
            spin_loop();
            retry_count += 1;
            if retry_count.is_multiple_of(10_000) {
                yield_now();
            }
        }

        start_id
    }

    pub(crate) fn last_id(&self) -> u64 {
        self.ctx.last_id()
    }
}
