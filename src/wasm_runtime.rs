//! WasmRuntime — ADR-014 WASM Function Registry runtime.
//!
//! Two layers, both lock-free on the hot path:
//!
//! - [`WasmRuntime`] — shared (`Arc`) registry of compiled modules. Validates
//!   binaries, compiles them with wasmtime, and tracks an `update_seq` so
//!   downstream consumers can passively refresh their local caches.
//!
//! - [`WasmRuntimeEngine`] — per-Transactor execution layer. Owns:
//!   - one wasmtime [`Engine`] (shared via `Arc<WasmRuntime>` — no clones);
//!   - **one** [`Linker`] built once with the three host imports
//!     (`ledger.credit` / `ledger.debit` / `ledger.get_balance`) wired to
//!     call straight into the Transactor's [`TransactorState`] via
//!     `Rc<RefCell<>>`;
//!   - the local handler-map snapshot and its watermark — moved here from
//!     the Transactor so the host has zero WASM bookkeeping.
//!
//! No `unsafe`, no raw pointers, no generic traits. The engine is bound
//! directly to `Rc<RefCell<TransactorState>>` because that's its only
//! caller; the simpler, monomorphic API outweighs the abstraction tax.

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

// ─────────────────────────────────────────────────────────────────────────────
// FunctionCaller — opaque, ready-to-run registration
// ─────────────────────────────────────────────────────────────────────────────

/// One compiled, ready-to-run registration. Stored in the [`HandlerMap`].
///
/// Opaque abstraction: callers see only [`FunctionCaller::crc32c`] and
/// [`FunctionCaller::execute`]. The compiled `Module` and the `execute`
/// export name never leave this module.
#[derive(Clone)]
pub struct FunctionCaller {
    /// CRC32C of the raw WASM binary. Embedded in `TxMetadata.tag` on
    /// invocation so auditors can cross-reference the exact binary executed.
    crc32c: u32,
    /// Compiled module. `wasmtime::Module` is cheap to clone (`Arc` inside).
    module: Module,
}

impl FunctionCaller {
    /// CRC32C of the raw WASM binary. Cheap getter.
    #[inline]
    pub fn crc32c(&self) -> u32 {
        self.crc32c
    }

    /// Run the registered function's `execute` export against a
    /// caller-supplied wasmtime `Store` / `Linker` pair. Returns the `u8`
    /// status: `0` = success, `1..=127` standard reason, `128..=255`
    /// user-defined per ADR-014. Any wasmtime-level failure (link,
    /// instantiate, or trap) is reported as [`INVALID_OPERATION_STATUS`].
    pub fn execute<T>(
        &self,
        store: &mut Store<T>,
        linker: &Linker<T>,
        params: [i64; 8],
    ) -> u8 {
        let instance = match linker.instantiate(&mut *store, &self.module) {
            Ok(i) => i,
            Err(_) => return INVALID_OPERATION_STATUS,
        };
        let exec_fn: TypedFunc<(i64, i64, i64, i64, i64, i64, i64, i64), i32> =
            match instance.get_typed_func(&mut *store, EXECUTE_FN) {
                Ok(f) => f,
                Err(_) => return INVALID_OPERATION_STATUS,
            };
        match exec_fn.call(
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

/// Status returned by [`FunctionCaller::execute`] when the wasmtime layer
/// itself fails (link error, instantiation failure, trap). Numerically
/// equal to `FailReason::INVALID_OPERATION` so the Transactor can map it
/// through the same standard-status pipeline.
pub const INVALID_OPERATION_STATUS: u8 = 5;

pub type HandlerMap = HashMap<String, FunctionCaller>;

// ─────────────────────────────────────────────────────────────────────────────
// WasmRuntime — shared compiled-module registry
// ─────────────────────────────────────────────────────────────────────────────

/// Shared wasmtime state for the ledger. One `WasmRuntime` per `Ledger`,
/// wrapped in `Arc` and shared between the Ledger facade, the Snapshot
/// stage, and every `WasmRuntimeEngine`.
pub struct WasmRuntime {
    engine: Engine,
    handlers: RwLock<HandlerMap>,
    update_seq: AtomicU32,
}

impl WasmRuntime {
    /// Construct a new, empty runtime with a default `Engine`.
    pub fn new() -> Self {
        Self::with_engine(Engine::default())
    }

    /// Construct a new runtime with the provided engine.
    pub fn with_engine(engine: Engine) -> Self {
        Self {
            engine,
            handlers: RwLock::new(HandlerMap::new()),
            update_seq: AtomicU32::new(0),
        }
    }

    /// Validate a WASM binary against the registry ABI. Does NOT install
    /// the function. Used by `Ledger::register_function` before writing
    /// to disk. Checks: parses, exports `execute`, signature is exactly
    /// eight `i64` params + one `i32` result.
    pub fn validate(&self, binary: &[u8]) -> io::Result<()> {
        let module = Module::from_binary(&self.engine, binary)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        check_execute_signature(&module)?;
        Ok(())
    }

    /// Compile a binary, verify its CRC32C matches, and insert the
    /// resulting `FunctionCaller` into the handler map. Increments
    /// `update_seq` so every consumer's next refresh picks it up.
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
        guard.insert(name.to_string(), FunctionCaller { crc32c, module });
        drop(guard);

        self.update_seq.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Remove a function from the handler map. Not an error if the name
    /// is unknown — recovery may legitimately replay such a record.
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
    /// decide whether to clone a fresh handler-map snapshot.
    #[inline]
    pub fn last_update_seq(&self) -> u32 {
        self.update_seq.load(Ordering::Acquire)
    }

    /// Return an owned clone of the handler map.
    pub fn get_handler_map(&self) -> HandlerMap {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .clone()
    }

    /// True iff a function with the given name is currently loaded.
    pub fn contains(&self, name: &str) -> bool {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .contains_key(name)
    }

    /// Number of currently-loaded functions.
    pub fn len(&self) -> usize {
        self.handlers.read().expect("handlers lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access the underlying engine. The same `Engine` is used by every
    /// `WasmRuntimeEngine`; no clones, no per-call allocations.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}

impl Default for WasmRuntime {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WasmRuntimeEngine — per-Transactor execution layer
// ─────────────────────────────────────────────────────────────────────────────

/// Internal `Store` data: a clone of the shared `Rc<RefCell<TransactorState>>`
/// (the `Rc` clone is just a refcount bump) plus the current transaction id.
struct HostStoreData {
    state: Rc<RefCell<TransactorState>>,
    tx_id: u64,
}

/// Per-Transactor WASM execution layer. Owns its `Linker` and looks up
/// compiled modules from the shared [`WasmRuntime`] registry. Single
/// consumer (the Transactor), so no generic / trait indirection — the
/// engine is hard-wired to [`TransactorState`] and routes the three host
/// imports straight into its `credit` / `debit` / `get_balance` methods.
///
/// Construction-time work happens **once**:
/// - the wasmtime [`Linker`] is built and the three host imports are
///   registered exactly once;
/// - the shared `Rc<RefCell<TransactorState>>` is captured.
///
/// Per-call work is just `Store::new(engine, HostStoreData)` — cheap; the
/// `Engine` is shared via `Arc` and `Rc::clone` is a refcount bump.
pub struct WasmRuntimeEngine {
    runtime: Arc<WasmRuntime>,
    state: Rc<RefCell<TransactorState>>,
    local_handlers: HandlerMap,
    local_handler_seq: u32,
    linker: Linker<HostStoreData>,
}

impl WasmRuntimeEngine {
    /// Build a new engine bound to `runtime`. Wires the three host imports
    /// (`ledger.credit`, `ledger.debit`, `ledger.get_balance`) into a
    /// single owned [`Linker`]. Panics if any registration fails — that
    /// indicates a bug in this module (e.g. duplicate name), not a runtime
    /// condition.
    pub fn new(runtime: Arc<WasmRuntime>, state: Rc<RefCell<TransactorState>>) -> Self {
        let mut linker: Linker<HostStoreData> = Linker::new(runtime.engine());

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

        Self {
            runtime,
            state,
            local_handlers: HandlerMap::new(),
            local_handler_seq: u32::MAX,
            linker,
        }
    }

    /// CRC32C of a registered function, or `None` if not loaded. Performs
    /// the same passive refresh of the local handler cache as
    /// [`Self::execute`].
    pub fn crc32c_of(&mut self, name: &str) -> Option<u32> {
        self.local_handlers.get(name).map(|c| c.crc32c())
    }

    /// True iff `name` is currently loaded after a passive refresh.
    pub fn contains(&mut self, name: &str) -> bool {
        self.local_handlers.contains_key(name)
    }

    /// Execute `name` with the given `params` and `tx_id`. Returns:
    /// - `None` — function is not loaded.
    /// - `Some(u8)` — the function's `u8` status (0 = success; otherwise
    ///   per ADR-014 standard / user-defined ranges; wasmtime-level
    ///   failures surface as [`INVALID_OPERATION_STATUS`]).
    ///
    /// Builds a per-call `Store<HostStoreData>` carrying an `Rc::clone` of
    /// the shared state plus the current `tx_id`. The host imports
    /// registered in [`Self::new`] read those out of the store on each
    /// call and route into `state.borrow_mut().credit/debit/get_balance`.
    pub fn execute(&mut self, name: &str, params: [i64; 8], tx_id: u64) -> Option<u8> {
        let caller = self.local_handlers.get(name).cloned()?;

        let mut store = Store::new(
            self.runtime.engine(),
            HostStoreData {
                state: Rc::clone(&self.state),
                tx_id,
            },
        );

        Some(caller.execute(&mut store, &self.linker, params))
    }

    /// Passively refresh the cached handler map iff `WasmRuntime` advanced
    /// its `update_seq`. Cheap atomic load on the steady path.
    #[inline]
    pub fn refresh(&mut self) {
        let seq = self.runtime.last_update_seq();
        if seq != self.local_handler_seq {
            self.local_handlers = self.runtime.get_handler_map();
            self.local_handler_seq = seq;
        }
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
    fn handler_map_clones_are_independent() {
        let rt = WasmRuntime::new();
        let bin = noop_wat();
        rt.load_function("noop", &bin, crc32c::crc32c(&bin))
            .unwrap();
        let map = rt.get_handler_map();
        assert_eq!(map.len(), 1);
        rt.unload_function("noop").unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(rt.len(), 0);
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
    fn overwriting_a_name_still_works() {
        let rt = WasmRuntime::new();
        let bin = noop_wat();
        rt.load_function("noop", &bin, crc32c::crc32c(&bin))
            .unwrap();
        let bin2 = transfer_wat();
        rt.load_function("noop", &bin2, crc32c::crc32c(&bin2))
            .unwrap();
        let caller = rt.get_handler_map().get("noop").cloned().unwrap();
        assert_eq!(caller.crc32c, crc32c::crc32c(&bin2));
        assert_eq!(rt.last_update_seq(), 2);
    }
}
