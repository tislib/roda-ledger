# ADR-023: Programmable State — Typed KV Stores and the WASM State ABI

**Status:** Proposed
**Date:** 2026-06-15
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-014 — WASM Function Registry: adds the KV/effect host verbs to module `ledger`; generalizes `execute(i64×8)->i32` to named, typed event exports.
- ADR-020 — Trailer Metadata: `KvEntry` is a new trailer follower, under the commit CRC and `sub_item_count`.
- ADR-022 — Account Layouts: adds KV state to the transactor alongside account state; reuses `account_id` keying and rollback. Consistent with Principle **P1**.
- ADR-006 — WAL/Snapshot/Seal Durability: KV is checkpointed in a per-segment `kv_snapshot_{N}` file (mirroring the function snapshot); recovery = snapshot + tail replay.

---

## Context

The ledger runs user logic as WASM (ADR-014) over balances, flags, and links (ADR-022), but a module cannot read or write **general typed state** — counters, maps, ordered indexes — atomically with its balance effects. Today such state has to be faked as accounts.

This ADR adds a programmable state layer: modules read/write typed KV state atomically with balance operations, replicated through the existing WAL. The ledger stays **domain-agnostic** — primitives, not policy; domain rules live in operator-deployed modules.

---

## Decision

### 1. WAL record — `KvEntry` (key → value)

One fixed 40-byte record per state mutation:

```rust
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct KvEntry {
    pub entry_type: u8,     // 0      WalEntryKind::Kv (10)
    pub _pad0:      u8,      // 1      reserved (aligns kv_scope)
    pub kv_scope:   u16,    // 2..4   scope: 0 = global, 1 = account
    pub _pad:       [u8; 4], // 4..8   zero-written, verified on read
    pub key:        [u32; 4],// 8..24
    pub account_id: u64,    // 24..32 0 = global
    pub value:      i64,    // 32..40
}
const _: () = assert!(size_of::<KvEntry>()  == 40);
const _: () = assert!(align_of::<KvEntry>() == 8);
```

It is a trailer follower (ADR-020), carries no amount (zero-sum-trivial), and may mix with `TxEntry`/account records in one transaction.

**P1 holds.** A `KvEntry` is a single concrete key→value record, fully defined by its fields — no op-code, no sub-tag. The transactor logs the **resolved value** (like `TxEntry.computed_balance`), so a counter add records its result, not a delta, and replay is a plain per-key assignment.

**Conventions (frozen with the format):** a missing key reads `0`; `value = 0` deletes; presence ⟺ nonzero (no meaningful stored `0`, ever); `kv_scope = 0` is global (`account_id` unused), `kv_scope = 1` scopes to `account_id`; `_pad`/`_pad0` are zeroed and verified.

### 2. In-memory state (transactor)

The transactor holds the live KV state and offers three access shapes, chosen by verb family — a global **register** array, a point-access **map**, and an ordered **tree** for range scans (deferred until a real range use case). **Scope** is a 16-bit `kv_scope` recorded on each `KvEntry` (`0` = global, `1` = account), set by the operation; `kv_get_scoped` reads the account scope, then falls back to global. The three stores occupy disjoint key namespaces, so every key resolves to exactly one store.

These stores and the methods the host imports call live in a new module **`crates/ledger/src/transactor/kv.rs`** — an extension of `Computer`: the KV data structures (register array, map, tree) plus an `impl Computer` block with one method per host verb (§4).

### 3. WASM execution — stateless, typed exports

Modules are stateless pure functions; inputs arrive as export arguments, e.g. `on_transfer(debit: u64, credit: u64, amount: i64, time: u64) -> i32`. `time` is the committed entry timestamp (not wall clock) so replicas and replays agree. This generalizes ADR-014's `execute(i64×8)`: the host resolves the export by name and type-checks it at registration. A nonzero return triggers the standard rollback.

### 4. Host ABI — added to module `ledger`

New verbs on the existing `HOST_MODULE = "ledger"` (no new namespace). Each is a thin shim that forwards to an identically-named method on `Computer` (`caller.data().borrow_mut().kv_set(..)`), exactly as `credit`/`debit` already do (ADR-014). Keys pass flattened as four `u32` components — no pointers, no guest-memory bounds checks; only `tree_range` reads guest memory.

```rust
// module "ledger" — guest imports; each mirrors a Computer method of the same name

// Map (point) — global namespace (kv_scope = 0)
fn kv_get(k0: u32, k1: u32, k2: u32, k3: u32) -> i64;                          // 0 if absent
fn kv_set(k0: u32, k1: u32, k2: u32, k3: u32, value: i64);                     // value 0 deletes

// Map (point) — account namespace (kv_scope = 1)
fn kv_get_scoped(account_id: u64, k0: u32, k1: u32, k2: u32, k3: u32) -> i64;  // account, else global
fn kv_set_scoped(account_id: u64, k0: u32, k1: u32, k2: u32, k3: u32, value: i64);

// Map (merge) — account_id 0 = global; returns the resolved value
fn kv_add(account_id: u64, k0: u32, k1: u32, k2: u32, k3: u32, delta: i64) -> i64;

// Tree (ordered) — account_id 0 = global
fn tree_get(account_id: u64, k0: u32, k1: u32, k2: u32, k3: u32) -> i64;
fn tree_set(account_id: u64, k0: u32, k1: u32, k2: u32, k3: u32, value: i64);
// scan k3 in [lo, hi] under prefix (k0,k1,k2); write <= cap (k3, value) pairs at out_ptr; return count
fn tree_range(account_id: u64, k0: u32, k1: u32, k2: u32, lo: u32, hi: u32, out_ptr: u32, cap: u32) -> u32;

// Registers — global i64 slots, id in 0..65536
fn register_read(id: u32) -> i64;
fn register_write(id: u32, value: i64);
```

Guest-side ergonomics (safe wrappers, a `path!` macro) are Future Work.

### 5. Recovery

KV is **checkpointed in the snapshot**, like balances and functions. The seal stage folds `KvEntry` records into a baseline as segments seal and writes a per-segment **kv-snapshot** file (mirroring the function snapshot). Recovery loads that snapshot into `ActiveSnapshot.kv`, then replays only the **post-snapshot tail** (un-snapshotted sealed segments + the active segment), applying each `KvEntry` last-writer-wins (`value = 0` removes the key):

```rust
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct KvKey { pub kv_scope: u16, pub account_id: u64, pub key: [u32; 4] }
// ActiveSnapshot gains:  pub kv: HashMap<KvKey, i64>
```

On startup the transactor takes `ActiveSnapshot.kv` and rebuilds its stores in `kv.rs`, routing each `KvKey` to its store by namespace — recovering itself without re-running any module. Recovery cost is bounded by snapshot frequency, exactly like balances.

---

## Consequences

**Positive**
- Programmable state, atomic with balances, on the one existing WAL path; the ledger stays domain-agnostic.
- `KvEntry` is a self-defining key→value record — P1 holds, and a new namespace or shape needs no new WAL type.
- Recovery is snapshot + tail replay, bounded by snapshot frequency like balances; the host ABI is additive under `ledger`.

**Negative**
- Presence ⟺ nonzero forbids a meaningful stored `0` forever (frozen in the format).
- No permission model — a bad module can corrupt KV state (money still bounded by zero-sum).
- KV adds its own per-segment snapshot file, a small extra write at each seal (mirrors the function snapshot).
- 4×u32 path width frozen; registers pre-allocate ~512 KB.

**Neutral**
- Tree deferred until a real ordered-iteration use case.
- "register" is overloaded: the function *registry* (ADR-014) vs. the *register* store.

---

## Open Questions (answer in code)

- Host-call overhead × KV-ops/tx at 1M TPS — benchmark with state enabled before trusting the number.
- Final path width — validate 4×u32 against real paths before freezing; widen a leaf to `u64` if needed.

---

## Future Work

**`roda-abi` crate.** Once the host ABI settles, move the guest binding into a `no_std`, wasm32-clean, independently-versioned crate that owns the `unsafe` FFI (so modules run under `forbid(unsafe_code)`), shares the signatures, and ships a CI conformance test. v1 modules use raw `extern "C"` directly.

---

## References
- ADR-014, ADR-020, ADR-022 (Principle P1)
- `crates/storage/src/entities.rs` — `WalEntryKind`, records, `FailReason`
- `crates/ledger/src/transactor/wasm_runtime.rs` — host linker (`build_host_linker`, `HOST_MODULE`)
- `crates/ledger/src/transactor/computer.rs`, `recover.rs`, `seal.rs` — apply / rollback / replay / KV checkpoint
- `crates/storage/src/snapshot.rs` — balance / function / KV snapshot files
- wasmtime — https://github.com/bytecodealliance/wasmtime
