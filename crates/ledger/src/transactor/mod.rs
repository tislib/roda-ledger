mod computer;
pub mod runner;
pub mod transaction;
#[allow(clippy::module_inception)]
pub mod wasm_runtime;

use crate::config::LedgerConfig;
use crate::dedup::DedupCache;
use crate::pipeline::TransactorContext;
use crate::recover::ActiveSnapshot;
pub use crate::transactor::computer::{
    Computer, STATUS_OPEN, STATUS_PROGRAMMED, STATUS_SYSTEM, TransactorAccount, grow_capacity,
    new_account_vec, set_flag,
};
use crate::transactor::runner::Runner;
use crate::transactor::wasm_runtime::{WasmRuntime, WasmRuntimeEngine};
use crate::tx_ring::writer::TxRingWriter;
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;

/// One-shot launcher for the transactor runner thread. Holds exactly the inputs
/// `start` needs; `start` consumes `self`, seeds the runner from
/// `active_snapshot`, spawns the thread, and drops `self` while the runner runs.
pub struct Transactor<'a> {
    pub ctx: TransactorContext,
    pub active_snapshot: &'a ActiveSnapshot,
    pub config: &'a LedgerConfig,
    pub wasm_runtime: Arc<WasmRuntime>,
    pub ring_writer: TxRingWriter,
}

impl Transactor<'_> {
    pub fn start(self) -> std::io::Result<JoinHandle<()>> {
        let Transactor {
            ctx,
            active_snapshot,
            config,
            wasm_runtime,
            ring_writer,
        } = self;

        let cap = ctx.input_capacity();

        // Seed balances from the recovered accounts.
        let mut accounts: Vec<TransactorAccount> =
            Vec::with_capacity(active_snapshot.next_account_id as usize);
        accounts.resize(
            active_snapshot.next_account_id as usize,
            TransactorAccount::default(),
        );
        for (account_id, account) in active_snapshot.accounts.iter() {
            accounts[*account_id as usize] = TransactorAccount {
                balance: account.balance,
                flags: account.flags,
            };
        }
        // system account
        set_flag(&mut accounts[0].flags, 0, STATUS_SYSTEM);
        let next_account_id = active_snapshot.next_account_id;

        // Seed parent→bucket links.
        let mut links = FxHashMap::default();
        for (parent_id, type_id, child_id) in active_snapshot.links.iter() {
            links.insert((*parent_id, *type_id), *child_id);
        }

        // wasm_runtime
        let wasm_runtime_ref = wasm_runtime.as_ref();
        for function_registered in active_snapshot.functions.iter() {
            wasm_runtime_ref.recover_function(function_registered)?
        }

        let resize_factor = config.resize_factor;

        // Seed the dedup window from the last-closed + active segment maps.
        let mut dedup = DedupCache::new(config.storage.transaction_count_per_segment);
        dedup.recover(
            active_snapshot.last_segment_user_ref_tx_id_map.clone(),
            active_snapshot.active_segment_user_ref_tx_id_map.clone(),
        );

        // Publish the recovered compute high-water so the runner expects the
        // next tx after recovery (else it stalls waiting for tx 1).
        ctx.set_processed_index(active_snapshot.last_tx_id);
        let expected_next_id = ctx.get_processed_index() + 1;

        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || {
                let state = Rc::new(RefCell::new(Computer::from_balances(
                    accounts,
                    next_account_id,
                    links,
                    resize_factor,
                    ring_writer,
                    ctx.wait_strategy(),
                )));
                let wasm_engine = WasmRuntimeEngine::new(wasm_runtime, Rc::clone(&state));
                let mut runner = Runner {
                    transaction_buffer: Vec::with_capacity(cap),
                    expected_next_id,
                    pending: BTreeMap::new(),
                    input_retry_count: 0,
                    dedup,
                    state,
                    wasm_engine,
                };
                runner.run(ctx);
            })
    }
}
