# ADR-023: Programmable State — Typed KV Store and the WASM State ABI

**Status:** Proposed
**Date:** 2026-06-16 (revised; supersedes the initial 2026-06-15 draft)
**Author:** Taleh Ibrahimli

**Revision note:** The first draft (scopes, per-account keying, a dense register array, an ordered
tree) was premature — none had a concrete use case yet and each froze format/ABI surface. This
revision narrows v1 to a single typed key→value map with a TLV-encoded key and value. Scope,
account keying, registers, and the tree are removed; they return later as additive types or a
follow-up ADR when a real need appears.

**Amends:**
- ADR-014 — WASM Function Registry: adds the KV host verbs to module `ledger`.
- ADR-020 — Trailer Metadata: `KvEntry` is a trailer follower, under the commit CRC and `sub_item_count`.
- ADR-006 — WAL/Snapshot/Seal Durability: KV is checkpointed in a per-segment `kv_snapshot_{N}` file
  (mirroring the function snapshot); recovery = snapshot + tail replay.

---

## Context

The ledger runs user logic as WASM (ADR-014) over balances and account state (ADR-022), but a module
cannot read or write **general typed state** — counters, maps, labels — atomically with its balance
effects. Today such state has to be faked as accounts.

This ADR adds a minimal programmable state layer: a single key→value map that modules read and write
atomically with balance operations, replicated through the existing WAL. The ledger stays
**domain-agnostic** — primitives, not policy; domain rules live in operator-deployed modules.

The first draft over-reached (scopes, per-account keying, a register array, an ordered tree). v1 is
deliberately minimal: **one map, one typed key, one typed value.** Structure the draft baked into the
record (scope, account) is now just convention inside the key; the shapes deferred anyway (ordered
range scans, dense registers) are removed until a concrete need returns them as additive types.

---

## Decision

### 1. Two representations — packed (WAL) and unpacked (memory)

The key and value have a compact **packed** form for the 40-byte WAL record and a pure-Rust
**unpacked** form for the in-memory store. The transactor **encodes** on the WAL write path and
**decodes** on the read / recovery path. Equality and ordering are defined on the *unpacked* form, so
the byte encoding never participates in lookups.

### 2. WAL record — `KvEntry` (40 bytes)

```rust
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable, PartialEq, Eq)]
pub struct KvEntry {
    pub entry_type: u8,   // 0       WalEntryKind::Kv (10)
    pub key:   [u8; 30],  // 1..31   packed KeyPath (TLV), zero-terminated
    pub value: [u8; 9],   // 31..40  packed Value (TLV); empty = delete
}
const _: () = assert!(size_of::<KvEntry>() == 40);
```

A trailer follower (ADR-020), carries no amount (zero-sum-trivial), and may mix with account /
`TxEntry` records in one transaction. **P1 holds:** the record is a single concrete key→value pair,
fully defined by its bytes — no op-code, no sub-tag. The transactor logs the **resolved value**, so
replay is a plain per-key assignment; an **empty** `value` removes the key.

### 3. TLV encoding (packed form)

A **Value** is `{typeId}{content}`:

- `typeId` is one byte: high 3 bits = `kind` (1..7), low 5 bits = `len - 1` (content length 1..32).
- `typeId == 0x00` is the **terminator / empty** — no content. (`kind = 0` is reserved for it.)
- `content` is `len` bytes, **big-endian**.

A **KeyPath** is a sequence of Values terminated by a `0x00` typeId. Zero-padding in the fixed buffer
self-terminates, so the `Zeroable` tail is a valid (empty) terminator for free. An empty key (first
byte `0x00`) is invalid.

| kind | type | content |
|---|---|---|
| 0 | terminator / empty | — |
| 1 | Integer | big-endian signed, minimal length |
| 2 | Constant | opaque bytes / short string |
| 3..7 | reserved (String, Blob, Bool, …) | additive |

The encoder uses **minimal length** (no leading zero bytes) to stay inside the 30-byte key / 8-byte
value budgets; an encoding that overflows the buffer **fails the operation (rollback)**. Minimal
(canonical) encoding is recommended for compactness but is **not required for correctness** — the
in-memory map keys on the *decoded* value, so two encodings of the same logical key resolve to the
same entry.

**Length tracks the value, not the source type.** Packed width depends only on the magnitude being
stored, not on the Rust type it came from. An integer `5` — whether it arrived as a `u8`, `u32`, or
`u64` — packs to its minimal `1` content byte, so the whole value is `2` bytes: `{typeId}` +
`{0x05}`. `300` needs `2` content bytes (`3` total); only a value using the full range packs all `8`.
On unpack the bytes sign-extend back to the in-memory `i64`, so the round trip is exact regardless of
how many bytes the wire used.

The `value` field holds one Value (1 typeId + up to 8 content bytes — a full `i64` fits). A `value`
with typeId `0x00` (empty) deletes the key, so a stored `Integer 0` (`{kind=1}{0x00}`) is **distinct
from absent** — the old "`value == 0` deletes, no meaningful stored zero" wart is gone.

The codec lives in **`crates/storage/src/kv.rs`** (KV-specific, not a general serializer): `Value` and
`KeyPath` are the unpacked truth, with `pack`/`unpack` to and from the fixed `KvEntry` fields —
`KeyPath::pack/unpack` for the 30-byte key, `Value::pack_slot/unpack_slot` for the 9-byte value
(`None` = empty/delete). `Int` uses minimal-length two's-complement big-endian; `pack` rejects an
empty key and any overflow past the budget.

**Constant strings — `KvConstant` (40 bytes).** A standalone WAL record (`WalEntryKind::KvConstant`,
kind in the enum, not a trailer follower — like `FunctionRegistered`) defining an interned constant:
a `u32 key` bound to a null-terminated UTF-8 `[u8; 32]` value. It is the **bounded** constant registry
behind the memory model — strings live here once and a KV key/value references them by `u32`, so the
shared pool grows only by module-defined constants (§6), never by runtime input. The referencing
`Value` kind (a `u32` constant-ref vs. today's inline `Const` bytes) is the next step. Lives in
`crates/storage/src/entities.rs`.

### 4. In-memory state (transactor)

A single point-access map over the unpacked types from `storage::kv` (§3):

```rust
enum Value   { Int(i64), Const(SmallVec<[u8; 8]>) }  // one TLV unit; derives Hash, Eq, Ord
struct KeyPath(SmallVec<[Value; 4]>);                // a run of Values; derives Hash, Eq, Ord

// in Computer (ledger): delete = remove the entry, so None is never stored
kv: FxHashMap<KeyPath, Value>
```

`Value` is the single TLV unit — both a `KeyPath` element and the `KvEntry` value slot (the slot is an
`Option<Value>`, `None` = empty/delete). No scope, no account array, no register array, no tree. Keys
are decoded at the WAL / guest boundary; the map **never keys on raw packed bytes** (recovery, seal,
and dedup all key on `KeyPath`). `KeyPath` derives `Ord` — unused in v1 (no range verb), costs
nothing, and lets an ordered store return additively later.

The transactor side lives in **`crates/ledger/src/transactor/kv.rs`** — the KV map plus an
`impl Computer` block with one method per host verb (§5); it reuses `storage::kv::{KeyPath, Value}` as
both the map key/value and the WAL codec.

### 5. Host ABI — added to module `ledger`

Keys and values pass as **packed byte buffers** in guest memory; the host decodes them (bounds-checked):

```rust
// writes the packed value to out; returns its length (0 = absent / empty)
fn kv_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> u32;
// empty val_len deletes; both buffers are packed TLV
fn kv_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32);
// intern a constant by name (null-terminated UTF-8); create-or-get, returns the stable u32 id
fn kv_constant(name_ptr: u32) -> u32;
```

Each is a thin shim forwarding to an identically-named `Computer` method, as `credit`/`debit` do
(ADR-014). A nonzero module return triggers the standard rollback; a per-tx KV undo log restores prior
values. Guest-side ergonomics (a `path!` builder, typed get/set) are Future Work / `roda-abi`.

### 6. Constant definitions — the `register()` export

Constants are **defined in module code, not config**. A module may export `register()`, which the host
calls **once per instantiation** (at registration, and after any re-instantiation — recovery, a new
leader). It interns each constant via `kv_constant` and caches the returned id in a module global:

```c
static i32 pending;                    // module global

void register() {
    pending = kv_constant("PENDING");  // null-terminated string
}

i32 execute(/* ... */) {
    // `pending` is just an integer — the stable id of the PENDING constant
    kv_set(/* ...key... */, pending);
}
```

`kv_constant(name)` is **create-or-get and idempotent**: the first call for a name allocates the next
id from the Transactor's monotonic counter and emits a `KvConstant` WAL record (`id → name`); every
later call — same node or another, this run or a replay — returns the **same id**. Constants are thus
immutable, created exactly once, and globally stable.

Because the id comes from the single-threaded Transactor and is logged, replicas and recoveries agree
**without re-running the module**: a follower replays `KvConstant` records to rebuild the name→id map
(and the counter), and when it later instantiates the module to execute, `register()` re-resolves the
same ids (create-or-get finds them — no new records). New `KvConstant` records are therefore written
only by whichever node first registers the module. The module never mints strings from runtime input —
only its own authored constant names enter the registry — so the pool stays bounded (§3).

**Callable surface is split by phase** (host-enforced):

| Export | May call | May **not** call |
|---|---|---|
| `register()` | `kv_constant` only (for now) | everything else (`kv_get`/`kv_set`, `credit`/`debit`, …) |
| `execute()` (and other event exports) | every host verb | `kv_constant` |

`kv_constant` is callable **only during the register phase**; calling it from `execute` — or calling
any data/balance verb from `register` — is rejected with `FailReason::PROHIBITED_HOST_CALL` (the host
shim sets the reason and traps; the Transactor surfaces it and rolls back, like any other fail reason).
This keeps constant definition a one-time, registration-time act, separate from per-transaction work:
`execute` resolves names it already cached as ids in `register`, never defines new ones. The host
enforces the split by admitting only the phase-appropriate verbs while each export runs.

### 7. Recovery

KV is **checkpointed in the snapshot**, like balances. The seal stage folds `KvEntry` records into a
baseline as segments seal and writes a per-segment kv-snapshot file. Recovery loads that baseline into
the in-memory map (decoding each record's `KeyPath` / `Value`), then replays the post-snapshot tail
last-writer-wins (empty value removes the key). Recovery **decodes at the boundary and keys on
`KeyPath`**, never on raw bytes. Cost is bounded by snapshot frequency, exactly like balances.

### 8. Scope — self-sufficient state, not a query engine

This establishes Principle **P3** (architecture: *Design principles*). The WAL is self-sufficient and self-explanatory by design: a `KvEntry`, like every record, is fully legible from its fields (P1 holds), and the WAL together with the Snapshot is enough to understand and surface *any* detail about the data — no module re-execution, no external context. The ledger reads KV state back as raw, typed values, but builds **no custom or analytical queries** over it — no in-engine query language, no programmable read path, no server-side aggregation — and there is no plan or need for one under this design. Any derived or analytical view — rollups, joins, time-series, cross-key aggregates — is the consuming system's responsibility: it reads the self-describing WAL/Snapshot and computes downstream. This keeps the programmable layer to primitives, not policy — and computation out of the single-writer Transactor.

---

## Consequences

**Positive**
- Minimal, typed key→value state, atomic with balances, on the one existing WAL path; the ledger
  stays domain-agnostic.
- TLV makes both key and value self-describing — P1 holds, and a new type or a future ordered store is
  additive (new `kind`, no new WAL type).
- Packed/unpacked split: structural equality on the decoded key removes the canonical-encoding hazard,
  and Rust `Ord` (not byte order) defines any future range scan.
- Explicit empty-value delete kills the old `value == 0` wart — a real `0` is storable.
- Big-endian content keeps the wire form deterministic and debuggable.

**Negative**
- Encode/decode on every op (hot-path cost) — benchmark with `kv_bench` before trusting throughput.
- `kv_get` / `kv_set` now read guest memory (bounds-checked), unlike the discarded flattened-`u32` ABI.
- Unpacked keys cost more RAM per *live* entry than the packed 30 bytes; `SmallVec` keeps small keys
  off the heap. WAL and snapshots stay packed, so only the live set pays.
- No scoping / permission model — a bad module can corrupt KV state (money still bounded by zero-sum).
  Scope / account, if ever needed, are encoded as leading path items by convention.

**Neutral**
- Ordered (tree) and dense (register) stores are removed from v1; they return as additive types or a
  follow-up ADR when a concrete use case exists.

---

## Open Questions (answer in code)

- Host-call + encode/decode overhead × KV-ops/tx at 1M TPS — benchmark before freezing the ABI.
- Final `kind` table and content limits — validate against real key/value shapes before freezing.

---

## Future Work

- Re-introduce an ordered store (BTreeMap + range verb) and/or a constant/namespace registry as
  additive `kind`s when a use case appears.
- **`roda-abi` crate.** Once the host ABI settles, move guest bindings into a `no_std`, wasm32-clean,
  independently-versioned crate that owns the `unsafe` FFI, ships a `path!` builder, and runs a CI
  conformance test. v1 modules use raw `extern "C"` directly.

---

## References
- ADR-014, ADR-020, ADR-006 (Principle P1)
- `docs/03-architecture.md` → *Design principles* — establishes Principle **P3** (§8)
- `crates/storage/src/entities.rs` — `WalEntryKind`, records
- `crates/storage/src/kv.rs` — `Value` / `KeyPath` types and `pack` / `unpack` codec
- `crates/ledger/src/transactor/kv.rs` — KV map, apply / rollback
- `crates/ledger/src/transactor/wasm_runtime.rs` — host linker (`build_host_linker`, `HOST_MODULE`)
- `crates/ledger/src/{recover.rs, seal.rs}` — KV checkpoint, replay
- `crates/storage/src/snapshot.rs` — KV snapshot file
- wasmtime — https://github.com/bytecodealliance/wasmtime
