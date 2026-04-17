//! WasmRuntime — ADR-014 WASM Function Registry runtime.
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
//!   - a long-lived `Store<HostStoreData>` — **not** re-created per call;
//!   - a lazy per-name [`FunctionCaller`] cache holding a pre-resolved
//!     [`TypedFunc`] so each `execute` is just a store-data write + a
//!     typed call. Zero instantiation, zero linker lookup on the hot path.
//!   - the cache is invalidated as a whole whenever `WasmRuntime::update_seq`
//!     advances, and the Store is recycled at the same time to keep
//!     wasmtime's per-Store instance count bounded.
//!
//! No `unsafe`, no raw pointers, no generics.

use crate::transactor::TransactorState;
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

// ─────────────────────────────────────────────────────────────────────────────
// WasmRuntime — shared compiled-module registry
// ─────────────────────────────────────────────────────────────────────────────

/// One registered function in the shared registry. Holds the compiled
/// `Module` (cheap — `Arc` inside) and its CRC32C.
#[derive(Clone)]
struct Registered {
    crc32c: u32,
    module: Module,
}

type Registry = HashMap<String, Registered>;

/// Shared wasmtime state for the ledger. One per `Ledger`, wrapped in
/// `Arc` and shared with every `WasmRuntimeEngine`. Owns:
/// - the wasmtime [`Engine`];
/// - a single [`Linker<HostStoreData>`] with the three host imports
///   registered at construction — reused by every engine's
///   `Linker::instantiate` call;
/// - the registry of compiled [`Module`]s keyed by name.
pub struct WasmRuntime {
    engine: Engine,
    linker: Linker<HostStoreData>,
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
    /// the registry. Increments `update_seq` so every engine's next
    /// refresh picks it up.
    pub fn load_function(&self, name: &str, binary: &[u8], crc32c: u32) -> io::Result<()> {
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
        guard.insert(name.to_string(), Registered { crc32c, module });
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

    /// Current update sequence — used by [`WasmRuntimeEngine::refresh`] to
    /// decide whether to invalidate its caller cache.
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

    /// Internal: fetch a named module + its crc by cloning from the
    /// registry (cheap — `Module` is `Arc`-backed). Used by
    /// [`WasmRuntimeEngine`] on cache miss.
    fn get(&self, name: &str) -> Option<(u32, Module)> {
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

/// Build the shared [`Linker`] with the three ADR-014 host imports
/// (`ledger.credit` / `ledger.debit` / `ledger.get_balance`) routed to
/// `HostStoreData.state` via `Rc<RefCell<TransactorState>>`. Called exactly
/// once per `WasmRuntime`.
fn build_host_linker(engine: &Engine) -> Linker<HostStoreData> {
    let mut linker: Linker<HostStoreData> = Linker::new(engine);

    linker
        .func_wrap(
            HOST_MODULE,
            "credit",
            |caller: Caller<'_, HostStoreData>, account_id: u64, amount: u64| {
                let d = caller.data();
                d.state.borrow_mut().credit(d.tx_id, account_id, amount);
            },
        )
        .expect("register ledger.credit");
    linker
        .func_wrap(
            HOST_MODULE,
            "debit",
            |caller: Caller<'_, HostStoreData>, account_id: u64, amount: u64| {
                let d = caller.data();
                d.state.borrow_mut().debit(d.tx_id, account_id, amount);
            },
        )
        .expect("register ledger.debit");
    linker
        .func_wrap(
            HOST_MODULE,
            "get_balance",
            |caller: Caller<'_, HostStoreData>, account_id: u64| -> i64 {
                caller.data().state.borrow().get_balance(account_id)
            },
        )
        .expect("register ledger.get_balance");

    linker
}

// ─────────────────────────────────────────────────────────────────────────────
// FunctionCaller — minimal pre-resolved call record
// ─────────────────────────────────────────────────────────────────────────────

/// Minimal pre-resolved call record. Stores **only** the typed export
/// handle; the `Module` and `Linker` that produced it are not kept here
/// — they live in [`WasmRuntime`].
///
/// Built once at cache-miss time via [`FunctionCaller::new`] (which does
/// the expensive `instantiate` + `get_typed_func` work), then reused for
/// every subsequent call on the same engine's `Store` until the cache is
/// invalidated.
#[derive(Clone)]
pub struct FunctionCaller {
    exec_fn: ExecTypedFunc,
}

impl FunctionCaller {
    /// Instantiate `module` against the caller-supplied `store` using the
    /// shared `linker`'s pre-registered host imports, then resolve the
    /// typed `execute` handle. All the expensive work happens here — the
    /// hot path never touches the linker or instance again.
    fn new(
        linker: &Linker<HostStoreData>,
        module: &Module,
        store: &mut Store<HostStoreData>,
    ) -> Result<Self, wasmtime::Error> {
        let instance = linker.instantiate(&mut *store, module)?;
        let exec_fn: ExecTypedFunc = instance.get_typed_func(&mut *store, EXECUTE_FN)?;
        Ok(Self { exec_fn })
    }

    /// Invoke the cached typed function. Returns the `u8` status: `0` =
    /// success, otherwise ADR-014 standard / user-defined ranges; a
    /// wasmtime trap surfaces as [`INVALID_OPERATION_STATUS`].
    ///
    /// **Does not** touch the linker, instance, or module — all three
    /// were resolved in [`Self::new`]. The per-call sequence is just:
    /// `TypedFunc::call` plus the caller's own Store-data update.
    #[inline]
    pub fn execute(&self, store: &mut Store<HostStoreData>, params: [i64; 8]) -> u8 {
        match self.exec_fn.call(
            store,
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

/// Data carried inside the engine's `Store`. `state` is set once at Store
/// construction and never changes; `tx_id` is rewritten before every
/// [`FunctionCaller::execute`] call via `Store::data_mut`.
pub struct HostStoreData {
    state: Rc<RefCell<TransactorState>>,
    tx_id: u64,
}

/// One cache slot: the function's CRC32C plus its pre-resolved
/// [`FunctionCaller`].
#[derive(Clone)]
struct CacheEntry {
    crc32c: u32,
    caller: FunctionCaller,
}

/// Per-Transactor WASM execution layer. Holds a long-lived `Store` and a
/// lazy `name → FunctionCaller` cache keyed on the shared registry's
/// `update_seq` watermark.
///
/// Per-call cost on a cache hit: one `HashMap::get`, one `Store::data_mut`
/// write, one `TypedFunc::call`. No instantiation, no linker touch.
pub struct WasmRuntimeEngine {
    runtime: Arc<WasmRuntime>,
    state: Rc<RefCell<TransactorState>>,
    store: Store<HostStoreData>,
    cache: HashMap<String, CacheEntry>,
    cache_seq: u32,
}

impl WasmRuntimeEngine {
    /// Build a new per-Transactor engine. Creates the long-lived `Store`
    /// carrying `Rc::clone(&state)` and an initial `tx_id = 0`.
    pub fn new(runtime: Arc<WasmRuntime>, state: Rc<RefCell<TransactorState>>) -> Self {
        let store = Store::new(
            runtime.engine(),
            HostStoreData {
                state: Rc::clone(&state),
                tx_id: 0,
            },
        );
        Self {
            runtime,
            state,
            store,
            cache: HashMap::new(),
            cache_seq: u32::MAX, // forces first refresh to be a no-op (no runtime changes yet)
        }
    }

    /// Invalidate the caller cache and rebuild the `Store` iff the
    /// shared registry's `update_seq` has advanced. Steady state is a
    /// single atomic load.
    ///
    /// Rebuilding the `Store` on each invalidation keeps wasmtime's
    /// per-Store instance count bounded no matter how many names have
    /// been registered and replaced over time.
    #[inline]
    pub fn refresh(&mut self) {
        let seq = self.runtime.last_update_seq();
        if seq != self.cache_seq {
            self.cache.clear();
            self.store = Store::new(
                self.runtime.engine(),
                HostStoreData {
                    state: Rc::clone(&self.state),
                    tx_id: 0,
                },
            );
            self.cache_seq = seq;
        }
    }

    /// Resolve `name` into the cache and return its CRC32C, or `None` if
    /// the function is not registered.
    ///
    /// On cache miss this performs `Linker::instantiate` + `get_typed_func`
    /// once — the only wasmtime instantiation work in the whole path.
    /// Every subsequent [`Self::execute`] for the same name is a pure
    /// cache hit.
    pub fn crc32c_of(&mut self, name: &str) -> Option<u32> {
        if let Some(entry) = self.cache.get(name) {
            return Some(entry.crc32c);
        }
        let (crc32c, module) = self.runtime.get(name)?;
        let caller = FunctionCaller::new(&self.runtime.linker, &module, &mut self.store).ok()?;
        self.cache
            .insert(name.to_string(), CacheEntry { crc32c, caller });
        Some(crc32c)
    }

    /// True iff `name` is currently loaded (after a passive cache check
    /// and, if needed, registry lookup).
    pub fn contains(&mut self, name: &str) -> bool {
        self.crc32c_of(name).is_some()
    }

    /// Execute `name` with the given `params` and `tx_id`. Returns:
    /// - `None` — function is not loaded in the cache or the registry.
    /// - `Some(u8)` — the function's status (0 = success; otherwise
    ///   per-ADR-014 standard / user-defined ranges; wasmtime traps
    ///   surface as [`INVALID_OPERATION_STATUS`]).
    ///
    /// Hot path: `HashMap::get` → `Store::data_mut` → `TypedFunc::call`.
    pub fn execute(&mut self, name: &str, params: [i64; 8], tx_id: u64) -> Option<u8> {
        // TypedFunc is Clone (cheap: internal handle), not Copy. Clone out of
        // the cache so we don't hold an immutable borrow on `self.cache`
        // while we mutably borrow `self.store`.
        let caller = self.cache.get(name).map(|e| e.caller.clone())?;
        self.store.data_mut().tx_id = tx_id;
        Some(caller.execute(&mut self.store, params))
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

/// Name constraints per ADR-014:
/// - 1..=32 bytes
/// - ASCII letters, digits, underscore
/// - must start with an ASCII letter
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
        rt.load_function("noop", &bin, crc32c::crc32c(&bin))
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
            .load_function("noop", &bin, crc32c::crc32c(&bin) ^ 1)
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
        rt.load_function("noop", &bin, crc32c::crc32c(&bin))
            .unwrap();
        assert!(rt.contains("noop"));
        assert_eq!(rt.len(), 1);
        rt.unload_function("noop").unwrap();
        assert!(!rt.contains("noop"));
        assert!(rt.is_empty());
    }
}
