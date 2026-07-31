//! WASM Function Registry runtime (ADR-014).
//!
//! Two layers:
//!
//! - [`WasmRuntime`] — shared (`Arc`) registry. Owns the wasmtime [`Engine`],
//!   the map of compiled [`Module`]s keyed by name, **the** `Linker` with
//!   the three host imports already wired, and a handle to [`Storage`] for
//!   persisting/reading function binaries on disk. All construction work
//!   happens exactly once per `Ledger`. Registration (validate + write
//!   binary + compile + insert) happens via [`WasmRuntime::register`],
//!   called from the Transactor's `Operation::FunctionRegistration`
//!   dispatcher arm — never on the hot transaction path.
//!
//! - [`WasmRuntimeEngine`] — per-Transactor execution layer. Owns:
//!   - a long-lived `Store<Rc<RefCell<TransactorComputer>>>` — **not**
//!     re-created per call and not per registration change;
//!   - a lazy per-name [`FunctionCaller`] cache tagged with the global
//!     `update_seq` at which it was last verified. On every
//!     [`WasmRuntimeEngine::caller`] call the entry is re-checked *for
//!     that name only* against the shared registry's current crc — no
//!     global wipe, unrelated cache entries are untouched.
//!   - host functions read the transactor state via
//!     `caller.data().borrow_mut()` — no tx_id is carried through the
//!     wasmtime boundary; the transactor calls [`Computer::init`]
//!     once per transaction and state methods pick the current id up
//!     from the field.
//!
//! No `unsafe`, no raw pointers, no generics.

use crate::transactor::Computer;
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use storage::Storage;
use storage::entities::{FailReason, FunctionRegistered, KV_CONSTANT_NAME_MAX};
use wasmtime::{Caller, Engine, Linker, Module, Store, TypedFunc};

/// The required export name on every registered function.
pub const EXECUTE_FN: &str = "execute";
/// Optional export (ADR-023 §6): defines the module's constants. Called once per
/// instantiation; signature `() -> ()`.
pub const REGISTER_FN: &str = "register";
/// The required host module name for host imports.
pub const HOST_MODULE: &str = "ledger";
/// The fixed arity of the `execute` export (i64 × N).
pub const EXECUTE_ARITY: usize = 8;

/// Status returned when the wasmtime layer itself fails (link,
/// instantiation, trap). Numerically equal to
/// `FailReason::INVALID_OPERATION` so the Transactor can map it through the
/// same standard-status pipeline.
pub const INVALID_OPERATION_STATUS: u8 = 5;

/// Status a host shim sets before trapping when the module calls a verb
/// prohibited in the current phase (ADR-023 §6). Numerically equal to
/// `FailReason::PROHIBITED_HOST_CALL`.
pub const PROHIBITED_HOST_CALL_STATUS: u8 = 8;

/// Typed handle to the WASM `execute` export. Used by both the shared
/// signature check and the per-engine caller cache.
type ExecTypedFunc = TypedFunc<(i64, i64, i64, i64, i64, i64, i64, i64), i32>;

/// Typed handle to the optional `register` export (`() -> ()`).
type RegisterTypedFunc = TypedFunc<(), ()>;

/// Host-call phase (ADR-023 §6): `register()` runs in `Register` (only
/// `kv_register_constant` callable); `execute` and other exports run in `Execute`.
#[derive(Copy, Clone, PartialEq, Eq)]
enum Phase {
    Execute,
    Register,
}

/// Store data threaded through wasmtime host calls: the shared mutable
/// transactor state plus the current host-call phase. Keeping the phase and its
/// enforcement here (rather than scattered through `build_host_linker`) is what
/// gates `kv_register_constant` to the register phase and every other verb to execute.
pub struct WasmStoreData {
    computer: Rc<RefCell<Computer>>,
    phase: Cell<Phase>,
}

impl WasmStoreData {
    pub fn new(computer: Rc<RefCell<Computer>>) -> Self {
        Self {
            computer,
            phase: Cell::new(Phase::Execute),
        }
    }

    pub fn borrow(&self) -> Ref<'_, Computer> {
        self.computer.borrow()
    }
    pub fn borrow_mut(&self) -> RefMut<'_, Computer> {
        self.computer.borrow_mut()
    }

    pub fn enter_register_phase(&self) {
        self.phase.set(Phase::Register);
    }
    pub fn exit_register_phase(&self) {
        self.phase.set(Phase::Execute);
    }
    pub fn in_register_phase(&self) -> bool {
        self.phase.get() == Phase::Register
    }

    /// Guard for a data/balance verb: in the register phase it is prohibited —
    /// record `PROHIBITED_HOST_CALL` (failing the tx) and report rejection.
    fn reject_in_register(&self) -> bool {
        if self.in_register_phase() {
            self.computer
                .borrow_mut()
                .fail(FailReason::PROHIBITED_HOST_CALL);
            true
        } else {
            false
        }
    }
}

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
/// the shared [`Linker`] with host imports pre-wired, the registry of
/// compiled [`Module`]s, and a handle to [`Storage`] for persisting and
/// reading function binaries on disk.
pub struct WasmRuntime {
    engine: Engine,
    linker: Linker<WasmStoreData>,
    handlers: RwLock<Registry>,
    update_seq: AtomicU32,
    storage: Arc<Storage>,
}

impl WasmRuntime {
    /// Construct a new, empty runtime with a default `Engine`.
    pub fn new(storage: Arc<Storage>) -> Self {
        Self::with_engine(Engine::default(), storage)
    }

    /// Construct a new runtime with the provided engine.
    pub fn with_engine(engine: Engine, storage: Arc<Storage>) -> Self {
        let linker = build_host_linker(&engine);
        Self {
            engine,
            linker,
            handlers: RwLock::new(Registry::new()),
            update_seq: AtomicU32::new(0),
            storage,
        }
    }

    /// Validate a WASM binary against the registry ABI. Does NOT install
    /// the function. Checks: parses, exports `execute`, signature is
    /// exactly eight `i64` params + one `i32` result.
    pub fn validate(&self, binary: &[u8]) -> io::Result<()> {
        let module = Module::from_binary(&self.engine, binary)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        check_execute_signature(&module)?;
        check_register_signature(&module)?;
        Ok(())
    }

    /// Compile a binary, verify its CRC32C matches, and insert it into
    /// the registry under the given `version`. Increments `update_seq`
    /// so every engine's next lookup picks it up.
    ///
    /// Internal: callers should prefer [`Self::register`] (transactor
    /// dispatcher) or [`Self::recover_register`] (recovery hook), which
    /// also persist the binary on disk and bookkeep version monotonicity.
    pub(crate) fn load_function(
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
        check_register_signature(&module)?;

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
    ///
    /// Internal: callers should prefer [`Self::unregister`] or
    /// [`Self::recover_unregister`].
    pub(crate) fn unload_function(&self, name: &str) -> io::Result<()> {
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

    // ── lifecycle methods (called from Transactor + recovery) ──────────────

    /// Validate a binary's ABI + name, compute the next monotonic
    /// version, persist the binary on disk, compile, and insert into the
    /// registry. Returns `(version, crc32c)` of the installed handler.
    ///
    /// Called from the Transactor's `Operation::FunctionRegistration`
    /// dispatcher arm. The caller pre-validates inputs in
    /// [`crate::ledger::Ledger::register_function`] for synchronous
    /// `io::ErrorKind` reporting; the validation here is the
    /// authoritative one.
    pub fn register(
        &self,
        name: &str,
        binary: &[u8],
        override_existing: bool,
    ) -> io::Result<(u16, u32)> {
        validate_name(name)?;
        self.validate(binary)?;

        if !override_existing && self.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("function `{}` is already registered", name),
            ));
        }

        let next_version = self.next_version(name)?;
        self.storage.write_function(name, next_version, binary)?;
        let crc = crc32c::crc32c(binary);
        self.load_function(name, binary, next_version, crc)?;
        Ok((next_version, crc))
    }

    /// Unregister the currently-loaded function under `name`. Writes a
    /// 0-byte `{name}_v{N+1}.wasm` on disk (audit trail) and removes the
    /// handler from the registry. Returns the version stamped onto the
    /// unregister record.
    pub fn unregister(&self, name: &str) -> io::Result<u16> {
        validate_name(name)?;

        if !self.contains(name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("function `{}` is not registered", name),
            ));
        }

        let next_version = self.next_version(name)?;
        self.storage.write_function(name, next_version, &[])?;
        self.unload_function(name)?;
        Ok(next_version)
    }

    /// Recovery hook — replay a register record from the WAL by reading
    /// the binary from disk at `(name, version)` and inserting it into
    /// the registry. Does NOT increment a fresh version (the version
    /// recorded in the WAL is authoritative).
    pub fn recover_register(&self, name: &str, version: u16, crc32c: u32) -> io::Result<()> {
        let binary = self.storage.read_function(name, version)?;
        self.load_function(name, &binary, version, crc32c)
    }

    /// Recovery hook — replay an unregister record from the WAL.
    pub fn recover_unregister(&self, name: &str) -> io::Result<()> {
        self.unload_function(name)
    }

    /// Identity of the currently-loaded handler for `name` — its
    /// `(version, crc32c)`, or `None` if unloaded. Captured before a
    /// registration so [`Self::revert_registration`] can restore it.
    pub fn handler_identity(&self, name: &str) -> Option<(u16, u32)> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .get(name)
            .map(|r| (r.version, r.crc32c))
    }

    /// Undo a just-applied registration when its transaction rolls back
    /// (e.g. the module's `register()` export failed, ADR-023 §6). Restores
    /// the `prior` handler by reloading its binary from disk, or unloads the
    /// function entirely if there was none — so the in-memory registry matches
    /// the (uncommitted) WAL. Bumps `update_seq`, so every engine's next
    /// `caller` lookup reconciles.
    pub fn revert_registration(&self, name: &str, prior: Option<(u16, u32)>) -> io::Result<()> {
        match prior {
            Some((version, crc32c)) => {
                let binary = self.storage.read_function(name, version)?;
                self.load_function(name, &binary, version, crc32c)
            }
            None => self.unload_function(name),
        }
    }

    pub fn recover_function(&self, function_registered: &FunctionRegistered) -> io::Result<()> {
        let fn_name = std::str::from_utf8(&function_registered.name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_end_matches('\0');

        if function_registered.crc32c == 0 {
            self.recover_unregister(fn_name)
        } else {
            self.recover_register(
                fn_name,
                function_registered.version,
                function_registered.crc32c,
            )
        }
    }

    /// Snapshot of every currently-loaded function with its version + CRC32C.
    pub fn list(&self) -> Vec<FunctionInfo> {
        self.handlers_snapshot()
            .into_iter()
            .map(|(name, version, crc32c)| FunctionInfo {
                name,
                version,
                crc32c,
            })
            .collect()
    }

    /// Next monotonic version for `name`. The in-memory registry holds
    /// the authoritative counter; the on-disk `functions/` directory is
    /// reference data only.
    fn next_version(&self, name: &str) -> io::Result<u16> {
        let prev = self.version_of(name).unwrap_or(0);
        prev.checked_add(1)
            .ok_or_else(|| io::Error::other("function version overflow (u16 exhausted)"))
    }
}

/// Build the shared [`Linker`] with the `ledger` host verbs — balances,
/// account flags, and KV/constants (ADR-022/023) — routed to the
/// transactor state. Called once per `WasmRuntime`.
fn build_host_linker(engine: &Engine) -> Linker<WasmStoreData> {
    let mut linker: Linker<WasmStoreData> = Linker::new(engine);

    linker
        .func_wrap(
            HOST_MODULE,
            "credit",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, amount: u64| {
                if caller.data().reject_in_register() {
                    return;
                }
                caller.data().borrow_mut().credit(account_id, amount);
            },
        )
        .expect("register ledger.credit");
    linker
        .func_wrap(
            HOST_MODULE,
            "debit",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, amount: u64| {
                if caller.data().reject_in_register() {
                    return;
                }
                caller.data().borrow_mut().debit(account_id, amount);
            },
        )
        .expect("register ledger.debit");
    linker
        .func_wrap(
            HOST_MODULE,
            "get_balance",
            |caller: Caller<'_, WasmStoreData>, account_id: u64| -> i64 {
                if caller.data().reject_in_register() {
                    return 0;
                }
                caller.data().borrow().get_balance(account_id)
            },
        )
        .expect("register ledger.get_balance");
    linker
        .func_wrap(
            HOST_MODULE,
            "linked_account",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, type_id: u32| -> u64 {
                if caller.data().reject_in_register() {
                    return 0;
                }
                // Get-or-create the bucket linked to `account_id` under
                // `type_id` (ADR-022 §6); returns the child account id.
                caller
                    .data()
                    .borrow_mut()
                    .linked_account(account_id, type_id as u16)
            },
        )
        .expect("register ledger.linked_account");
    linker
        .func_wrap(
            HOST_MODULE,
            "get_flag",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, lane: u32| -> u32 {
                if caller.data().reject_in_register() {
                    return 0;
                }
                caller
                    .data()
                    .borrow()
                    .get_account_flag(account_id, lane as u8) as u32
            },
        )
        .expect("register ledger.get_flag");
    linker
        .func_wrap(
            HOST_MODULE,
            "has_flag",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, lane: u32, value: u32| -> u32 {
                if caller.data().reject_in_register() {
                    return 0;
                }
                caller
                    .data()
                    .borrow()
                    .has_account_flag(account_id, lane as u8, value as u8) as u32
            },
        )
        .expect("register ledger.has_flag");
    linker
        .func_wrap(
            HOST_MODULE,
            "set_flag",
            |caller: Caller<'_, WasmStoreData>, account_id: u64, lane: u32, value: u32| {
                if caller.data().reject_in_register() {
                    return;
                }
                // ADR-022 §6: WASM may set any lane, including the status lane (0).
                caller
                    .data()
                    .borrow_mut()
                    .set_account_flag(account_id, lane as u8, value as u8);
            },
        )
        .expect("register ledger.set_flag");

    // ── Programmable KV state (ADR-023 §4) — single map, two verbs ───────────
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_get",
            |caller: Caller<'_, WasmStoreData>, k0: u32, k1: u32, k2: u32, k3: u32| -> i64 {
                if caller.data().reject_in_register() {
                    return 0;
                }
                caller.data().borrow().kv_get([k0, k1, k2, k3])
            },
        )
        .expect("register ledger.kv_get");
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_set",
            |caller: Caller<'_, WasmStoreData>, k0: u32, k1: u32, k2: u32, k3: u32, value: i64| {
                if caller.data().reject_in_register() {
                    return;
                }
                caller.data().borrow_mut().kv_set([k0, k1, k2, k3], value);
            },
        )
        .expect("register ledger.kv_set");

    // Constants (ADR-023 §6). `kv_register_constant` declares a constant and is
    // the ONLY verb allowed in the register phase; `kv_get_constant` resolves a
    // name to its id at execute time (modules stay stateless — no globals). Both
    // take a `name_ptr` to a null-terminated UTF-8 name in guest memory.
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_register_constant",
            |mut caller: Caller<'_, WasmStoreData>, name_ptr: u32| -> Result<(), wasmtime::Error> {
                if !caller.data().in_register_phase() {
                    caller
                        .data()
                        .borrow_mut()
                        .fail(FailReason::PROHIBITED_HOST_CALL);
                    return Err(wasmtime::Error::msg(
                        "kv_register_constant is callable only during register()",
                    ));
                }
                let name = read_guest_name(&mut caller, name_ptr)?;
                if name.len() > KV_CONSTANT_NAME_MAX {
                    caller
                        .data()
                        .borrow_mut()
                        .fail(FailReason::CONSTANT_NAME_TOO_LONG);
                    return Err(wasmtime::Error::msg(
                        "constant name exceeds KV_CONSTANT_NAME_MAX bytes",
                    ));
                }
                caller.data().borrow_mut().kv_register_constant(&name);
                Ok(())
            },
        )
        .expect("register ledger.kv_register_constant");
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_get_constant",
            |mut caller: Caller<'_, WasmStoreData>,
             name_ptr: u32|
             -> Result<u32, wasmtime::Error> {
                if caller.data().reject_in_register() {
                    return Err(wasmtime::Error::msg(
                        "kv_get_constant is not callable during register()",
                    ));
                }
                let name = read_guest_name(&mut caller, name_ptr)?;
                // Resolve and drop the borrow before the not-found arm re-borrows
                // mutably to set the fail reason.
                let resolved = caller.data().borrow().kv_get_constant(&name);
                match resolved {
                    // Tag the id so a key component built from it packs as a
                    // constant (`kv_key` strips the tag), not a plain integer.
                    Some(id) => Ok(id | crate::transactor::kv::KV_CONST_TAG),
                    None => {
                        // Stop execution rather than let the module run with a bogus id.
                        caller
                            .data()
                            .borrow_mut()
                            .fail(FailReason::CONSTANT_NOT_FOUND);
                        Err(wasmtime::Error::msg("kv_get_constant: unknown constant"))
                    }
                }
            },
        )
        .expect("register ledger.kv_get_constant");

    linker
}

/// Read a null-terminated UTF-8 name from guest memory at `ptr` (the full name —
/// length is bounded by the guest memory edge, not truncated). The caller
/// enforces `KV_CONSTANT_NAME_MAX`; silently truncating here would alias two
/// distinct names that share a prefix. Used by the constant verbs.
fn read_guest_name(
    caller: &mut Caller<'_, WasmStoreData>,
    ptr: u32,
) -> Result<Vec<u8>, wasmtime::Error> {
    let mem = match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(m)) => m,
        _ => return Err(wasmtime::Error::msg("module exports no `memory`")),
    };
    let bytes = mem.data(&*caller);
    let start = ptr as usize;
    if start > bytes.len() {
        return Err(wasmtime::Error::msg("constant name_ptr out of bounds"));
    }
    let rest = &bytes[start..];
    let len = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
    Ok(rest[..len].to_vec())
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
    /// Optional `register` export (ADR-023 §6), resolved at instantiation.
    register_fn: Option<RegisterTypedFunc>,
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
            // A host shim that aborts the module (e.g. a prohibited-call phase
            // violation) sets the fail reason before trapping; surface it, else
            // map the bare trap to INVALID_OPERATION.
            Err(_) => match store.data().borrow().status() {
                0 => INVALID_OPERATION_STATUS,
                reason => reason,
            },
        }
    }

    /// Invoke the optional `register` export (ADR-023 §6) to define the module's
    /// constants. Returns the `u8` status: `0` = success or no `register` export;
    /// otherwise a host-set fail reason (e.g. a prohibited-call phase violation),
    /// falling back to `INVALID_OPERATION_STATUS` on a bare trap.
    #[inline]
    pub fn call_register(&self) -> u8 {
        let Some(register_fn) = &self.register_fn else {
            return 0;
        };
        let mut store = self.store.borrow_mut();
        // Enter the register phase: only `kv_constant` is callable inside.
        store.data().enter_register_phase();
        let result = register_fn.call(&mut *store, ());
        store.data().exit_register_phase();
        // A prohibited verb sets the fail reason without trapping; surface it
        // whether `register` returned Ok or trapped.
        let status = store.data().borrow().status();
        match result {
            Ok(()) => status,
            Err(_) if status != 0 => status,
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
    state: Rc<RefCell<Computer>>,
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
    pub fn new(runtime: Arc<WasmRuntime>, state: Rc<RefCell<Computer>>) -> Self {
        let store = Rc::new(RefCell::new(Store::new(
            runtime.engine(),
            WasmStoreData::new(Rc::clone(&state)),
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
                    let (exec_fn, register_fn): (ExecTypedFunc, Option<RegisterTypedFunc>) = {
                        let mut store = self.store.borrow_mut();
                        let Ok(instance) = self.runtime.linker.instantiate(&mut *store, &module)
                        else {
                            return None;
                        };
                        let Ok(f) = instance.get_typed_func(&mut *store, EXECUTE_FN) else {
                            return None;
                        };
                        // `register` is optional; absent modules define no constants.
                        let r = instance
                            .get_typed_func::<(), ()>(&mut *store, REGISTER_FN)
                            .ok();
                        (f, r)
                    };
                    self.cache.insert(
                        name.to_string(),
                        FunctionCaller {
                            crc32c: shared_crc,
                            exec_fn,
                            register_fn,
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

    /// Shared reference to the `TransactorComputer` handle held by the
    /// engine's Store. Useful for tests.
    #[inline]
    pub fn state(&self) -> &Rc<RefCell<Computer>> {
        &self.state
    }

    /// The shared `WasmRuntime` this engine resolves callers against.
    /// Used by the Transactor's `Operation::FunctionRegistration`
    /// dispatcher arm to install / remove handlers.
    #[inline]
    pub fn runtime(&self) -> &WasmRuntime {
        &self.runtime
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

/// Enforce the optional `register` ABI (ADR-023 §6): if a module exports
/// `register`, it must be a function taking no params and returning nothing.
/// Absent is fine — the export is optional.
fn check_register_signature(module: &Module) -> io::Result<()> {
    use wasmtime::ExternType;

    let Some(export) = module.get_export(REGISTER_FN) else {
        return Ok(());
    };
    let ExternType::Func(func_ty) = export else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("export `{REGISTER_FN}` is not a function"),
        ));
    };
    if func_ty.params().len() != 0 || func_ty.results().len() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("`{REGISTER_FN}` must take no parameters and return nothing"),
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
// FunctionInfo — public surface returned from list_functions
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata returned by [`WasmRuntime::list`] for every currently
/// registered WASM function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionInfo {
    pub name: String,
    pub version: u16,
    pub crc32c: u32,
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use storage::StorageConfig;
    use tempfile::TempDir;

    fn temp_runtime() -> (WasmRuntime, TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = StorageConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..StorageConfig::default()
        };
        let storage = Arc::new(Storage::new(cfg).expect("storage"));
        (WasmRuntime::new(storage), dir)
    }

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
        let (rt, _td) = temp_runtime();
        rt.validate(&noop_wat()).unwrap();
        rt.validate(&transfer_wat()).unwrap();
    }

    #[test]
    fn validate_rejects_bad_arity() {
        let (rt, _td) = temp_runtime();
        let err = rt.validate(&bad_arity_wat()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_rejects_missing_export() {
        let (rt, _td) = temp_runtime();
        let err = rt.validate(&missing_export_wat()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn validate_rejects_garbage() {
        let (rt, _td) = temp_runtime();
        let err = rt.validate(b"not a wasm binary").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn load_increments_update_seq() {
        let (rt, _td) = temp_runtime();
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
        let (rt, _td) = temp_runtime();
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
        let (rt, _td) = temp_runtime();
        rt.unload_function("does_not_exist").unwrap();
        assert_eq!(rt.last_update_seq(), 1);
    }

    #[test]
    fn registry_contains_and_len_track_load_unload() {
        let (rt, _td) = temp_runtime();
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
