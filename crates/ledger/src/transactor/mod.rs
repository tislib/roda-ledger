mod computer;
pub mod runner;
pub mod transaction;
#[allow(clippy::module_inception)]
pub mod wasm_runtime;

use crate::balance::Balance;
use crate::config::LedgerConfig;
use crate::dedup::DedupCache;
use crate::pipeline::TransactorContext;
pub use crate::transactor::computer::{
    Computer, STATUS_OPEN, STATUS_PROGRAMMED, STATUS_SYSTEM, TransactorAccount, grow_capacity,
    new_account_vec, set_flag,
};
use crate::transactor::runner::Runner;
use crate::transactor::wasm_runtime::{WasmRuntime, WasmRuntimeEngine};
use crate::tx_ring::writer::TxRingWriter;
use crossbeam_skiplist::SkipMap;
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;
use storage::entities::{FailReason, FunctionRegistered, SYSTEM_ACCOUNT_ID};

pub struct Transactor {
    rejected_transactions: Arc<SkipMap<u64, FailReason>>,
    balances: Vec<TransactorAccount>,
    next_account_id: u64,
    resize_factor: f64,
    /// Recovered parent→bucket links (ADR-022 §3), seeded into the state at
    /// `start` so a recovered leader resolves buckets instead of re-creating.
    links: FxHashMap<(u64, u16), u64>,
    dedup: DedupCache,
    wasm_runtime: Arc<WasmRuntime>,
    ring_writer: Option<TxRingWriter>,
}

impl Transactor {
    pub fn new(
        config: &LedgerConfig,
        wasm_runtime: Arc<WasmRuntime>,
        ring_writer: TxRingWriter,
    ) -> Self {
        Self {
            rejected_transactions: Arc::new(Default::default()),
            balances: new_account_vec(config.initial_account_size),
            next_account_id: 1,
            resize_factor: config.resize_factor,
            links: FxHashMap::default(),
            dedup: DedupCache::new(config.storage.transaction_count_per_segment),
            wasm_runtime,
            ring_writer: Some(ring_writer),
        }
    }

    pub(crate) fn recover_balances(&mut self, balances: &HashMap<u64, Balance>) {
        for (account_id, balance) in balances {
            if let Some(slot) = self.balances.get_mut(*account_id as usize) {
                slot.balance = *balance;
            }
        }
    }

    /// Recovery hook: reconstruct account existence from the high-water.
    /// This phase only OPEN status exists (sequential, never closed), so
    /// `[1, next)` are OPEN and `0` is SYSTEM. Grows the Vec geometrically to
    /// cover `next_account_id`. Call BEFORE `recover_balances` so balances
    /// overlay onto already-OPEN cells.
    pub(crate) fn recover_account_layout(&mut self, next_account_id: u64) {
        let needed = next_account_id as usize;
        if needed > self.balances.len() {
            let new_cap = grow_capacity(self.balances.len(), self.resize_factor, needed);
            self.balances.resize(new_cap, TransactorAccount::default());
        }
        if let Some(a) = self.balances.get_mut(0) {
            set_flag(&mut a.flags, 0, STATUS_SYSTEM);
        }
        let mut open_flags = 0u64;
        set_flag(&mut open_flags, 0, STATUS_OPEN);
        let upper = needed.min(self.balances.len());
        for id in 1..upper {
            self.balances[id].flags = open_flags;
        }
        self.next_account_id = next_account_id.max(1);
    }

    /// Recovery hook: replay an `AccountLinked` record into the link map so a
    /// recovered leader resolves the bucket instead of re-creating it (ADR-022).
    pub(crate) fn recover_account_link(&mut self, parent_id: u64, type_id: u16, child_id: u64) {
        self.links.insert((parent_id, type_id), child_id);
    }

    /// Recovery hook: overlay exact per-account flags (snapshot + replayed
    /// `AccountOpened`) on the blanket-OPEN layout so PROGRAMMED buckets recover
    /// with their real status. SYSTEM(0) is skipped — the layout owns it.
    pub(crate) fn recover_account_flags(&mut self, flags: &HashMap<u64, u64>) {
        for (&account_id, &f) in flags {
            if account_id == SYSTEM_ACCOUNT_ID {
                continue;
            }
            if let Some(slot) = self.balances.get_mut(account_id as usize) {
                slot.flags = f;
            }
        }
    }

    pub fn transaction_rejection_reason(&self, transaction_id: u64) -> Option<FailReason> {
        self.rejected_transactions
            .get(&transaction_id)
            .map(|entry| *entry.value())
    }

    pub fn get_rejected_count(&self) -> u64 {
        self.rejected_transactions.len() as u64
    }

    pub(crate) fn dedup_cache_mut(&mut self) -> &mut DedupCache {
        &mut self.dedup
    }

    /// Recovery hook — replay a `FunctionRegistered` WAL record by
    /// either reloading the binary from disk and inserting it into the
    /// runtime's registry, or unloading the handler. Mirrors the
    /// pattern of [`DedupCache::recover_entry`] but for the WASM
    /// runtime. Called from [`crate::recover::Recover::recover_until`]
    /// on every `WalEntry::FunctionRegistered` encountered during
    /// replay.
    pub(crate) fn recover_function_registered(
        &self,
        record: &FunctionRegistered,
    ) -> std::io::Result<()> {
        let name = record.name_str();
        if record.is_unregister() {
            self.wasm_runtime.recover_unregister(name)
        } else {
            self.wasm_runtime
                .recover_register(name, record.version, record.crc32c)
        }
    }

    pub fn start(&mut self, ctx: TransactorContext) -> std::io::Result<JoinHandle<()>> {
        let cap = ctx.input_capacity();
        let balances = std::mem::take(&mut self.balances);
        let next_account_id = self.next_account_id;
        let links = std::mem::take(&mut self.links);
        let resize_factor = self.resize_factor;
        let rejected_transactions = self.rejected_transactions.clone();
        let dedup = std::mem::replace(&mut self.dedup, DedupCache::new(0));
        let wasm_runtime = self.wasm_runtime.clone();
        let expected_next_id = ctx.get_processed_index() + 1;
        let ring_writer = self.ring_writer.take().unwrap();
        // `Rc<RefCell<>>` and `WasmRuntimeEngine` are `!Send`; build them
        // inside the spawned thread so nothing non-`Send` ever crosses the
        // thread boundary.
        std::thread::Builder::new()
            .name("transactor".to_string())
            .spawn(move || {
                let state = Rc::new(RefCell::new(Computer::from_balances(
                    balances,
                    next_account_id,
                    links,
                    resize_factor,
                    ring_writer,
                    ctx.wait_strategy(),
                )));
                let wasm_engine = WasmRuntimeEngine::new(wasm_runtime, Rc::clone(&state));
                let mut runner = Runner {
                    rejected_transactions,
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
