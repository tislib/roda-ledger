//! WASM Function Registry runtime (ADR-014).
//!
//! Two layers:
//!
//! - [`WasmRuntime`] — shared (`Arc`) registry. Owns the wasmtime [`Engine`],
//!   the map of compiled [`Module`]s keyed by name, and **the** `Linker`
//!   with the three host imports already wired. All three are built exactly
//!   once per `Ledger`. Registration work (compile + signature check +
//!   insert) happens only on `load_function` — never on the hot path.
//!
//! - [`WasmRuntimeEngine`] — per-Transactor execution layer. Owns:
//!   - a long-lived `Store<Rc<RefCell<TransactorState>>>` — **not**
//!     re-created per call and not per registration change;
//!   - a lazy per-name [`FunctionCaller`] cache tagged with the global
//!     `update_seq` at which it was last verified. On every
//!     [`WasmRuntimeEngine::caller`] call the entry is re-checked *for
//!     that name only* against the shared registry's current crc — no
//!     global wipe, unrelated cache entries are untouched.
//!   - host functions read the transactor state via
//!     `caller.data().borrow_mut()` — no tx_id is carried through the
//!     wasmtime boundary; the transactor calls [`TransactorState::init`]
//!     once per transaction and state methods pick the current id up
//!     from the field.
//!
//! No `unsafe`, no raw pointers, no generics.

use crate::entities::{FunctionRegistered, WalEntry};
use crate::pipeline::LedgerContext;
use crate::storage::Storage;
use crate::transactor::TransactorState;
use crate::wait_strategy::WaitStrategy;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use wasmtime::{Caller, Engine, Linker, Module, Store, TypedFunc};

/// The required export name on every registered function.
pub const EXECUTE_FN: &str = "execute";
/// The required host module name for host imports.
pub const HOST_MODULE: &str = "ledger";
/// The fixed arity of the `execute` export (i64 × N).
pub const EXECUTE_ARITY: usize = 8;

/// Status returned when the wasmtime layer itself fails (link,
/// instantiation, trap). Numerically equal to
/// `FailReason::INVALID_OPERATION` so the Transactor can map it through the
/// same standard-status pipeline.
pub const INVALID_OPERATION_STATUS: u8 = 5;

/// Typed handle to the WASM `execute` export. Used by both the shared
/// signature check and the per-engine caller cache.
type ExecTypedFunc = TypedFunc<(i64, i64, i64, i64, i64, i64, i64, i64), i32>;

/// Store data threaded through wasmtime host calls: the shared mutable
/// transactor state. The transactor writes its per-transaction id into
/// the state via [`TransactorState::init`] before calling
/// [`FunctionCaller::execute`], so no `tx_id` ever crosses the wasmtime
/// boundary.
pub type WasmStoreData = Rc<RefCell<TransactorState>>;

// ─────────────────────────────────────────────────────────────────────────────
// WasmRuntime — shared compiled-module registry
// ─────────────────────────────────────────────────────────────────────────────

/// One registered function in the shared registry. Holds the compiled
/// `Module` (cheap — `Arc` inside), its CRC32C, and the version number
/// from the `FunctionRegistered` WAL record that installed it.
#[derive(Clone)]
struct Registered {
    version: u16,
    crc32c: u32,
    module: Module,
}

type Registry = HashMap<String, Registered>;

/// Shared wasmtime state for the ledger. One per `Ledger`, wrapped in
/// `Arc` and shared with every `WasmRuntimeEngine`. Owns the [`Engine`],
/// the shared [`Linker`] with host imports pre-wired, and the registry
/// of compiled [`Module`]s.
pub struct WasmRuntime {
    engine: Engine,
    linker: Linker<WasmStoreData>,
    handlers: RwLock<Registry>,
    update_seq: AtomicU32,
}

impl WasmRuntime {
    /// Construct a new, empty runtime with a default `Engine`.
    pub fn new() -> Self {
        Self::with_engine(Engine::default())
    }

    /// Construct a new runtime with the provided engine.
    pub fn with_engine(engine: Engine) -> Self {
        let linker = build_host_linker(&engine);
        Self {
            engine,
            linker,
            handlers: RwLock::new(Registry::new()),
            update_seq: AtomicU32::new(0),
        }
    }

    /// Validate a WASM binary against the registry ABI. Does NOT install
    /// the function. Checks: parses, exports `execute`, signature is
    /// exactly eight `i64` params + one `i32` result.
    pub fn validate(&self, binary: &[u8]) -> io::Result<()> {
        let module = Module::from_binary(&self.engine, binary)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        check_execute_signature(&module)?;
        Ok(())
    }

    /// Compile a binary, verify its CRC32C matches, and insert it into
    /// the registry under the given `version`. Increments `update_seq`
    /// so every engine's next lookup picks it up.
    pub fn load_function(
        &self,
        name: &str,
        binary: &[u8],
        version: u16,
        crc32c: u32,
    ) -> io::Result<()> {
        validate_name(name)?;

        let observed = crc32c::crc32c(binary);
        if observed != crc32c {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "crc32c mismatch for function {name}: expected {crc32c:08x}, got {observed:08x}"
                ),
            ));
        }

        let module = Module::from_binary(&self.engine, binary)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        check_execute_signature(&module)?;

        let mut guard = self
            .handlers
            .write()
            .map_err(|_| io::Error::other("handlers lock poisoned"))?;
        guard.insert(
            name.to_string(),
            Registered {
                version,
                crc32c,
                module,
            },
        );
        drop(guard);

        self.update_seq.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Remove a function from the registry. Not an error if the name is
    /// unknown — recovery may legitimately replay such a record.
    pub fn unload_function(&self, name: &str) -> io::Result<()> {
        let mut guard = self
            .handlers
            .write()
            .map_err(|_| io::Error::other("handlers lock poisoned"))?;
        guard.remove(name);
        drop(guard);
        self.update_seq.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Current update sequence — used by [`WasmRuntimeEngine::caller`] as
    /// a fast-path skip token: when it matches the local cache marker,
    /// the cached entry is trusted without a registry re-check.
    #[inline]
    pub fn last_update_seq(&self) -> u32 {
        self.update_seq.load(Ordering::Acquire)
    }

    /// True iff a function with the given name is currently registered.
    pub fn contains(&self, name: &str) -> bool {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .contains_key(name)
    }

    /// Number of currently-registered functions.
    pub fn len(&self) -> usize {
        self.handlers.read().expect("handlers lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access the underlying engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Current version for `name`, or `None` if no handler is loaded.
    /// Used by the `Ledger` facade to compute the next monotonic
    /// version without scanning disk.
    pub fn version_of(&self, name: &str) -> Option<u16> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .get(name)
            .map(|r| r.version)
    }

    /// CRC32C of the currently registered binary for `name`, or `None`
    /// if no handler is loaded.
    pub fn crc32c_of(&self, name: &str) -> Option<u32> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .get(name)
            .map(|r| r.crc32c)
    }

    /// Snapshot of `(name, version, crc32c)` for every currently-loaded
    /// handler. Not on the hot path.
    pub fn handlers_snapshot(&self) -> Vec<(String, u16, u32)> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .iter()
            .map(|(n, r)| (n.clone(), r.version, r.crc32c))
            .collect()
    }

    /// Internal: fetch a named module + its crc by cloning from the
    /// registry (cheap — `Module` is `Arc`-backed).
    fn get_module(&self, name: &str) -> Option<(u32, Module)> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .get(name)
            .map(|r| (r.crc32c, r.module.clone()))
    }
}

impl Default for WasmRuntime {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the shared [`Linker`] with the three host imports
/// (`ledger.credit` / `ledger.debit` / `ledger.get_balance`) routed
/// directly to the transactor state. Called exactly once per
/// `WasmRuntime`.
fn build_host_linker(engine: &Engine) -> Linker<WasmStoreData> {
    let mut linker: Linker<WasmStoreData> = Linker::new(engine);

    linker
        .func_wrap(
            HOST_MODULE,
            "credit",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, amount: u64| {
                caller.data().borrow_mut().credit(account_id, amount);
            },
        )
        .expect("register ledger.credit");
    linker
        .func_wrap(
            HOST_MODULE,
            "debit",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, amount: u64| {
                caller.data().borrow_mut().debit(account_id, amount);
            },
        )
        .expect("register ledger.debit");
    linker
        .func_wrap(
            HOST_MODULE,
            "get_balance",
            |caller: Caller<'_, WasmStoreData>, account_id: u64| -> i64 {
                caller.data().borrow().get_balance(account_id)
            },
        )
        .expect("register ledger.get_balance");

    linker
}

// ─────────────────────────────────────────────────────────────────────────────
// FunctionCaller — the cache value; crc + the resolved TypedFunc
// ─────────────────────────────────────────────────────────────────────────────

/// A compiled, ready-to-run function. Produced lazily by
/// [`WasmRuntimeEngine::caller`] on first lookup and cached until the
/// entry's crc stops matching the shared registry.
///
/// Self-contained — holds everything it needs to execute:
/// - `crc32c` of the registered binary (for `TxMetadata.tag` building);
/// - the wasmtime [`TypedFunc`] handle (cheap `Arc`-clone internally);
/// - an `Rc`-shared handle to the engine's long-lived [`Store`], so
///   `execute(params)` is a terminal call that does not require the
///   caller to thread the store through;
/// - `verified_at_seq` — the shared-registry `update_seq` this entry
///   was last reconciled against; used by [`WasmRuntimeEngine::caller`]
///   to short-circuit the per-name re-check when no registration has
///   happened globally since the last lookup.
#[derive(Clone)]
pub struct FunctionCaller {
    crc32c: u32,
    exec_fn: ExecTypedFunc,
    store: Rc<RefCell<Store<WasmStoreData>>>,
    verified_at_seq: u32,
}

impl FunctionCaller {
    /// CRC32C of the registered binary. Cheap getter.
    #[inline]
    pub fn crc32c(&self) -> u32 {
        self.crc32c
    }

    /// Invoke the function. Returns the `u8` status: `0` = success,
    /// otherwise standard / user-defined ranges. A wasmtime trap
    /// surfaces as [`INVALID_OPERATION_STATUS`].
    ///
    /// The engine's `Store` is borrowed through this caller's shared
    /// `Rc<RefCell<>>`. Host imports read the transactor state (and its
    /// `tx_id`) from the same store via `caller.data()`.
    #[inline]
    pub fn execute(&self, params: [i64; 8]) -> u8 {
        let mut store = self.store.borrow_mut();
        match self.exec_fn.call(
            &mut *store,
            (
                params[0], params[1], params[2], params[3], params[4], params[5], params[6],
                params[7],
            ),
        ) {
            Ok(r) => (r as u32 & 0xFF) as u8,
            Err(_) => INVALID_OPERATION_STATUS,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WasmRuntimeEngine — per-Transactor execution layer
// ─────────────────────────────────────────────────────────────────────────────

/// Per-Transactor WASM execution layer. Holds the long-lived `Store`
/// and a lazy `name → FunctionCaller` cache.
///
/// Cache coherence is per-entry and surgical: [`Self::caller`] checks
/// the shared registry's current crc *for the requested name only*
/// against the cached entry, and rebuilds just that entry if they
/// diverge. Unrelated cached entries are never touched by unrelated
/// registrations.
pub struct WasmRuntimeEngine {
    runtime: Arc<WasmRuntime>,
    state: Rc<RefCell<TransactorState>>,
    /// Wrapped in `Rc<RefCell<>>` so every produced [`FunctionCaller`]
    /// can carry an `Rc::clone` and `execute(params)` without the
    /// caller threading the store through the call site.
    store: Rc<RefCell<Store<WasmStoreData>>>,
    cache: HashMap<String, FunctionCaller>,
}

impl WasmRuntimeEngine {
    /// Build a new per-Transactor engine. Creates the long-lived `Store`
    /// holding an `Rc::clone(&state)` so every host call sees the same
    /// mutable transactor state.
    pub fn new(runtime: Arc<WasmRuntime>, state: Rc<RefCell<TransactorState>>) -> Self {
        let store = Rc::new(RefCell::new(Store::new(
            runtime.engine(),
            Rc::clone(&state),
        )));
        Self {
            runtime,
            state,
            store,
            cache: HashMap::new(),
        }
    }

    /// Locate the handler for `name`, reconciling it against the shared
    /// registry if necessary. Returns `None` if the function is not
    /// currently registered.
    ///
    /// Hot path — cached + fresh: one `HashMap::get` + one atomic load.
    /// Slow path — stale or missing: one `HashMap::get` + one read-lock
    /// on the shared registry + (on crc mismatch) one
    /// `Linker::instantiate` + `get_typed_func` + cache insert.
    ///
    /// Cache invalidation is surgical: a registration / unregistration
    /// of `foo` does **not** invalidate the cached entry for `bar`.
    pub fn caller(&mut self, name: &str) -> Option<&FunctionCaller> {
        let live_seq = self.runtime.last_update_seq();

        // Fast path — cache hit on an entry verified at the current
        // global seq. No registry touch.
        if let Some(c) = self.cache.get(name)
            && c.verified_at_seq == live_seq
        {
            return self.cache.get(name);
        }

        // Slow path — cross-check against the shared registry.
        match self.runtime.get_module(name) {
            None => {
                // The function is no longer registered. Drop any stale
                // cached entry and report missing.
                self.cache.remove(name);
                None
            }
            Some((shared_crc, module)) => {
                let is_fresh = self
                    .cache
                    .get(name)
                    .map(|c| c.crc32c == shared_crc)
                    .unwrap_or(false);
                if is_fresh {
                    // Cached entry still matches; just bump its
                    // verification watermark.
                    self.cache.get_mut(name).unwrap().verified_at_seq = live_seq;
                } else {
                    // New or replaced version — compile and install.
                    // Instantiation needs the store temporarily; borrow
                    // mutably for the duration, then drop so the
                    // resulting `FunctionCaller` can share it.
                    let exec_fn: ExecTypedFunc = {
                        let mut store = self.store.borrow_mut();
                        let Ok(instance) = self.runtime.linker.instantiate(&mut *store, &module)
                        else {
                            return None;
                        };
                        let Ok(f) = instance.get_typed_func(&mut *store, EXECUTE_FN) else {
                            return None;
                        };
                        f
                    };
                    self.cache.insert(
                        name.to_string(),
                        FunctionCaller {
                            crc32c: shared_crc,
                            exec_fn,
                            store: Rc::clone(&self.store),
                            verified_at_seq: live_seq,
                        },
                    );
                }
                self.cache.get(name)
            }
        }
    }

    /// True iff `name` resolves (lazy reconcile on call).
    pub fn contains(&mut self, name: &str) -> bool {
        self.caller(name).is_some()
    }

    /// Shared reference to the `TransactorState` handle held by the
    /// engine's Store. Useful for tests.
    #[inline]
    pub fn state(&self) -> &Rc<RefCell<TransactorState>> {
        &self.state
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ABI / name validation
// ─────────────────────────────────────────────────────────────────────────────

/// Enforce the fixed ABI: an exported function named `execute` with
/// exactly eight `i64` parameters and a single `i32` result.
fn check_execute_signature(module: &Module) -> io::Result<()> {
    use wasmtime::{ExternType, ValType};

    let export = module.get_export(EXECUTE_FN).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("module does not export `{EXECUTE_FN}`"),
        )
    })?;

    let func_ty = match export {
        ExternType::Func(f) => f,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("export `{EXECUTE_FN}` is not a function"),
            ));
        }
    };

    let params: Vec<_> = func_ty.params().collect();
    if params.len() != EXECUTE_ARITY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "`{EXECUTE_FN}` must take exactly {EXECUTE_ARITY} parameters, got {}",
                params.len()
            ),
        ));
    }
    for (i, p) in params.iter().enumerate() {
        if !matches!(p, ValType::I64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("`{EXECUTE_FN}` param {i} must be i64"),
            ));
        }
    }

    let results: Vec<_> = func_ty.results().collect();
    if results.len() != 1 || !matches!(results[0], ValType::I32) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("`{EXECUTE_FN}` must return exactly one i32"),
        ));
    }

    Ok(())
}

/// Name constraints: 1..=32 bytes, ASCII letters / digits / underscore,
/// must start with an ASCII letter.
pub fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "function name must not be empty",
        ));
    }
    if name.len() > 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("function name `{name}` is longer than 32 bytes"),
        ));
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphabetic() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("function name `{name}` must start with an ASCII letter"),
        ));
    }
    for b in bytes {
        if !(b.is_ascii_alphanumeric() || *b == b'_') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "function name `{name}` must contain only ASCII alphanumerics and underscore"
                ),
            ));
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// WasmRegistry — Ledger-facing façade (ADR-014 register/unregister/list)
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata returned by [`WasmRegistry::list`] for every currently
/// registered WASM function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionInfo {
    pub name: String,
    pub version: u16,
    pub crc32c: u32,
}

/// Facade over the full function-registry lifecycle: validate → write
/// to disk → push `FunctionRegistered` onto the WAL → block until the
/// Snapshot stage commits it and the live runtime reflects the change.
///
/// Owned by [`crate::ledger::Ledger`] as a thin composition layer so
/// the Ledger's public API (`register_function` / `unregister_function`
/// / `list_functions`) reduces to one-line delegations.
///
/// Holds:
/// - `Arc<WasmRuntime>` — the shared registry whose `update_seq` gates
///   every engine's caller cache;
/// - `Arc<Storage>` — the `{data_dir}/functions` binary store;
/// - [`LedgerContext`] — the pipeline handle used to push
///   non-transactional WAL entries;
/// - `WaitStrategy` — backpressure + polling cadence for the blocking
///   waits after WAL push.
#[derive(Clone)]
pub struct WasmRegistry {
    runtime: Arc<WasmRuntime>,
    storage: Arc<Storage>,
    ledger_ctx: LedgerContext,
    wait_strategy: WaitStrategy,
}

impl WasmRegistry {
    pub fn new(
        runtime: Arc<WasmRuntime>,
        storage: Arc<Storage>,
        ledger_ctx: LedgerContext,
        wait_strategy: WaitStrategy,
    ) -> Self {
        Self {
            runtime,
            storage,
            ledger_ctx,
            wait_strategy,
        }
    }

    /// Register a WASM function under `name` at the next monotonic
    /// version. Returns `(version, crc32c)` once the Snapshot stage has
    /// committed the record and the live runtime reflects the handler.
    pub fn register(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> io::Result<(u16, u32)> {
        // 1. Validate (ABI + name).
        validate_name(name)?;
        self.runtime.validate(binary)?;

        // 2. Uniqueness check against the live registry.
        if !override_existing && self.runtime.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("function `{}` is already registered", name),
            ));
        }

        // 3. Pick next version. The WAL + WasmRuntime carry the
        //    authoritative version counter; the on-disk `functions/`
        //    directory is reference data only.
        let next_version = self.next_version(name)?;
        self.storage.write_function(name, next_version, binary)?;
        let crc = crc32c::crc32c(binary);

        // 4. Push WAL record (non-transactional; bypasses Transactor).
        let record = FunctionRegistered::new(name, next_version, crc);
        self.push_wal_entry_blocking(WalEntry::FunctionRegistered(record));

        // 5. Wait for the Snapshot stage to commit it.
        self.wait_until_loaded(name, crc);

        Ok((next_version, crc))
    }

    /// Unregister the currently-loaded function under `name`. Writes a
    /// 0-byte `{name}_v{N+1}.wasm` on disk and a WAL record with
    /// `crc32c = 0`. Blocks until the Snapshot stage commits it and the
    /// handler is gone from the live runtime. Returns the version
    /// number stamped on the unregister record. Errors with
    /// [`io::ErrorKind::NotFound`] if `name` is not currently loaded.
    pub fn unregister(&self, name: &str) -> io::Result<u16> {
        validate_name(name)?;

        if !self.runtime.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("function `{}` is not registered", name),
            ));
        }

        let next_version = self.next_version(name)?;
        // Empty file under the next version — audit-trail preserved.
        self.storage.write_function(name, next_version, &[])?;
        let record = FunctionRegistered::new(name, next_version, 0);
        self.push_wal_entry_blocking(WalEntry::FunctionRegistered(record));

        self.wait_until_unloaded(name);
        Ok(next_version)
    }

    /// Snapshot of every currently-loaded function with its version + CRC32C.
    pub fn list(&self) -> Vec<FunctionInfo> {
        self.runtime
            .handlers_snapshot()
            .into_iter()
            .map(|(name, version, crc32c)| FunctionInfo {
                name,
                version,
                crc32c,
            })
            .collect()
    }

    // ── internals ──────────────────────────────────────────────────────

    /// Next monotonic version number for `name`. The authoritative
    /// counter is the in-memory registry (installed by the Snapshot
    /// stage from committed `FunctionRegistered` WAL records).
    fn next_version(&self, name: &str) -> io::Result<u16> {
        let prev = self.runtime.version_of(name).unwrap_or(0);
        prev.checked_add(1)
            .ok_or_else(|| io::Error::other("function version overflow (u16 exhausted)"))
    }

    /// Push a WAL entry with backpressure.
    fn push_wal_entry_blocking(&self, mut entry: WalEntry) {
        let mut retry_count = 0u64;
        while self.ledger_ctx.is_running() {
            match self.ledger_ctx.push_wal_entry(entry) {
                Ok(()) => return,
                Err(returned) => {
                    entry = returned;
                    self.wait_strategy.retry(retry_count);
                    retry_count += 1;
                }
            }
        }
    }

    fn wait_until_loaded(&self, name: &str, expected_crc: u32) {
        let mut retry_count = 0u64;
        while self.ledger_ctx.is_running() {
            if self.runtime.crc32c_of(name) == Some(expected_crc) {
                return;
            }
            self.wait_strategy.retry(retry_count);
            retry_count += 1;
        }
    }

    fn wait_until_unloaded(&self, name: &str) {
        let mut retry_count = 0u64;
        while self.ledger_ctx.is_running() {
            if !self.runtime.contains(name) {
                return;
            }
            self.wait_strategy.retry(retry_count);
            retry_count += 1;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_wat() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (func (export "execute")
                (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
                i32.const 0))
            "#,
        )
        .expect("noop wat")
    }

    fn transfer_wat() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (import "ledger" "credit" (func $credit (param i64 i64)))
              (import "ledger" "debit"  (func $debit  (param i64 i64)))
              (func (export "execute")
                (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
                local.get 0
                local.get 2
                call $credit
                local.get 1
                local.get 2
                call $debit
                i32.const 0))
            "#,
        )
        .expect("transfer wat")
    }

    fn bad_arity_wat() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (func (export "execute")
                (param i64 i64) (result i32)
                i32.const 0))
            "#,
        )
        .expect("bad arity wat")
    }

    fn missing_export_wat() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
              (func (export "not_execute")
                (param i64 i64 i64 i64 i64 i64 i64 i64) (result i32)
                i32.const 0))
            "#,
        )
        .expect("missing export wat")
    }

    #[test]
    fn validate_accepts_valid_module() {
        let rt = WasmRuntime::new();
        rt.validate(&noop_wat()).unwrap();
        rt.validate(&transfer_wat()).unwrap();
    }

    #[test]
    fn validate_rejects_bad_arity() {
        let rt = WasmRuntime::new();
        let err = rt.validate(&bad_arity_wat()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_rejects_missing_export() {
        let rt = WasmRuntime::new();
        let err = rt.validate(&missing_export_wat()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_rejects_garbage() {
        let rt = WasmRuntime::new();
        let err = rt.validate(b"not a wasm binary").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn load_increments_update_seq() {
        let rt = WasmRuntime::new();
        assert_eq!(rt.last_update_seq(), 0);

        let bin = noop_wat();
        rt.load_function("noop", &bin, 1, crc32c::crc32c(&bin))
            .unwrap();
        assert_eq!(rt.last_update_seq(), 1);

        rt.unload_function("noop").unwrap();
        assert_eq!(rt.last_update_seq(), 2);
    }

    #[test]
    fn load_rejects_crc_mismatch() {
        let rt = WasmRuntime::new();
        let bin = noop_wat();
        let err = rt
            .load_function("noop", &bin, 1, crc32c::crc32c(&bin) ^ 1)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_name_accepts_ok_names() {
        validate_name("a").unwrap();
        validate_name("fee_calculation").unwrap();
        validate_name("A1_b2").unwrap();
        validate_name(&"x".repeat(32)).unwrap();
    }

    #[test]
    fn validate_name_rejects_bad_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("1abc").is_err());
        assert!(validate_name("_abc").is_err());
        assert!(validate_name("bad-name").is_err());
        assert!(validate_name("bad name").is_err());
        assert!(validate_name(&"x".repeat(33)).is_err());
    }

    #[test]
    fn unload_unknown_is_noop() {
        let rt = WasmRuntime::new();
        rt.unload_function("does_not_exist").unwrap();
        assert_eq!(rt.last_update_seq(), 1);
    }

    #[test]
    fn registry_contains_and_len_track_load_unload() {
        let rt = WasmRuntime::new();
        assert!(rt.is_empty());
        let bin = noop_wat();
        rt.load_function("noop", &bin, 1, crc32c::crc32c(&bin))
            .unwrap();
        assert!(rt.contains("noop"));
        assert_eq!(rt.len(), 1);
        rt.unload_function("noop").unwrap();
        assert!(!rt.contains("noop"));
        assert!(rt.is_empty());
    }
}
