# ADR-022: Account Layouts and Program-Defined Accounts

**Status:** Proposed
**Date:** 2026-06-08
**Author:** Taleh

**Amends:**
- ADR-001 — Entries-Based Execution Model: adds three account-layout WAL record kinds; transactor rollback extends to account-layout mutations.
- ADR-002 — Vec-Based Balance Storage: the per-account cell grows from `i64` to `{ i64 balance, u64 flags }`. Account ids stay `u64`.
- ADR-006 — WAL, Snapshot, and Seal Durability: snapshot/seal persist `(id, balance, flags)` plus link triples.
- ADR-014 — WASM Function Registry: adds host functions (`linked_account`, existence-aware `credit`/`debit`, `get_flag`/`has_flag`); lazy program-account creation.
- ADR-020 — Trailer Metadata: the three account records are new trailer followers, covered by the commit CRC and `sub_item_count`.

---

## Context

Today an "account" is not a first-class entity. It is a bare index into a flat `Vec<i64>` of balances (ADR-002): no account struct, no metadata, no flags, no notion of existence. Any in-range id is implicitly live at balance 0, and `credit`/`debit` perform only a bounds check — the reserved `FailReason::ACCOUNT_NOT_FOUND` is never constructed. There is no way to:

- mark an account as existing vs. not (a mistyped id silently succeeds);
- distinguish a normal user account from an internal, program-controlled one (a user could move funds straight into what is meant to be a reserved bucket);
- relate one account to another ("reserved balance of account X" is convention-only and cannot be reported);
- attach any per-account attribute (type, currency, frozen, overdraft policy).

This is tolerable for plain transfers but blocks **program-defined accounts**: sub-accounts (reserved, bonus, escrow, hold, …) linked to a main account, manipulated only by WASM programs, used to model holds and earmarked balances. Those require accounts that provably exist, that can be classified as internal, and that can be linked to and enumerated under a parent — none of which the current model supports.

This ADR introduces account existence, a per-account flags word, parent→bucket links, three WAL record kinds, the snapshot-side representation, and the WASM host surface that ties it together. Motivating use case:

```
reserve(main, amount):   h = linked_account(main, HOLD); debit(h, amount);  credit(main, amount)
release(main, amount):   h = linked_account(main, HOLD); credit(h, amount); debit(main, amount)
GetBalance(main, include_linked=true) -> { balance, [(HOLD, 50), (BONUS, 20), …] }
```

RodaLedger is pre-production with no users, so breaking on-disk and wire changes are acceptable and no migration path is required.

## Decision

### 1. Account identity and existence

- Account ids are **`u64`** (see *Implementation status* — an earlier draft narrowed them to `u32`, but the memory saving wasn't worth the cross-stack churn). The gRPC wire uses `uint64 account_id`; no `< 2^32` validation.
- Accounts are created **only** by the new `OpenAccount` operation, allocated **strictly sequentially** (preserving ADR-002's dense indexing). `next_account_id` is a monotonic counter in transactor state.
- `OpenAccount { count }` opens a contiguous range: `count == 0` is treated as 1; `count > 0` opens that many. The committed transaction reports `begin_account_id` / `end_account_id`.
- **Existence is enforced.** A balance operation on a non-existent account fails with `ACCOUNT_NOT_FOUND` (the previously-vestigial code, now wired).

### 1.1 Dynamic capacity (no fixed maximum)

There is no `max_account_count`. Config carries `initial_account_size` (default `1<<10`) and `resize_factor` (default `0.75`); the account arrays grow on demand, so an unknown or growing population neither wastes RAM up front nor forces a restart to expand.

- **Trigger:** only account-creating paths (`OpenAccount`, and `linked_account`'s create branch) advance the high-water. When an allocation would exceed capacity, the array grows to `max(needed, ⌈capacity × (1 + resize_factor)⌉)` (e.g. 1024 → 1792).
- **Transactor (single thread):** a plain `Vec::resize` (new cells zero = `NOT_EXISTENT`). Capacity is node-local — not replicated, not WAL'd (replaying `AccountOpened` re-derives it); a resize is not undone on rollback (spare capacity is harmless).
- **Snapshot (1 writer / N readers):** the array is an `ArcSwap<Vec<SnapshotAccount>>`, **swapped only on resize**. Normal balance/flag updates mutate the atomics in place on the shared backing, so the writer keeps a local `Arc` and the hot write path never touches `ArcSwap` (preserving §5's write-path priority). To resize, the writer builds a larger Vec, copies the atomic values across, `store`s it, and refreshes its local handle; readers `load` per query (lock-free), and a reader still holding the old Vec keeps reading it (stale is acceptable under P2) until it drops the guard.
- **Bound semantics:** an id ≥ current capacity is by construction not-opened → `ACCOUNT_NOT_FOUND` (opening would have grown capacity). **`ACCOUNT_LIMIT_EXCEEDED` is removed** (see *Implementation status*): id-space exhaustion (`u32::MAX` accounts ≈ >50 GB RAM) is unreachable in practice, so the allocator **panics** on overflow rather than surfacing a handled failure.
- **Cost:** copy-on-resize is O(capacity) — a brief, rare writer pause (geometric growth, account-creation only). At very large scale a segmented array (append a chunk, swap a small index Vec) trades one extra index per access for O(1) resize.

### 2. The account cell — balance + flags

The transactor's `Vec<i64>` becomes `Vec<TransactorAccount>`:

```rust
struct TransactorAccount {
    balance: i64,   // signed — SYSTEM/liability accounts go negative
    flags:   u64,   // 8 byte-lanes
}                   // 16 B — same size as a padded { i64, status:u8 } cell, so flags is effectively free
```

`flags` is **8 byte-lanes** (lane id `0..8`, each a `u8`):

```
get_flag(flags, id)    = (flags >> (id*8)) as u8
set_flag(flags, id, v) = clear and set that byte lane
has_flag(flags, id, v) = get_flag(flags, id) == v
```

- **Lane 0 = status:** `0 = NOT_EXISTENT`, then `OPEN`, `PROGRAMMED`, `SYSTEM` (+ `CLOSED`, future). A zero-initialised cell is therefore non-existent with no init step; the hot-path existence check is `(flags & 0xFF) != 0`, in-cache with the balance.
- Remaining lanes (provisional): `1` type/class, `2` frozen/lock, `3` overdraft policy, `4` currency id (if ≤256), `5–7` reserved.
- Flags hold only small *categorical* attributes. Wide references (external id, owner, free-form KV) are intentionally out of scope and would return later as a separate read-side structure if needed.

### 3. Links — parent → bucket

A link maps `(parent_id, type_id)` to a child (bucket) account, where `type_id: u16` is an interned, program-defined bucket type (`HOLD`, `BONUS`, …) — a global per-parent namespace shared across programs.

- **Transactor (hot, single-thread, point-resolve only):** `HashMap<(u64 parent, u16 type_id), u64 child>` with a fast hasher (FxHash). O(1) `get`; the transactor never enumerates.
- **Snapshot (1 writer / N readers, point + enumerate):** `SkipMap<(u64, u16), u64>`. Because it is ordered, a parent's links are contiguous: `get((P,t))` resolves and `range((P,0)..=(P,u16::MAX))` enumerates all of P's buckets in one scan — no separate parent→children index and no type-probing.

Both stages key identically; the structure differs by each stage's job. The balance hot path (`credit`/`debit`) never touches links — resolution happens once per bucket-op, off the mutation path (composite-key lookup ~30 ns single-thread; the link map is cold relative to balances).

### 4. WAL records — three concrete kinds

Per **principle P1**, each WAL entry type does exactly one concrete thing and its named fields define it — no op/sub-kind discriminators. Three new fixed-width (40-byte family) `WalEntryKind`s:

```rust
AccountOpened       { begin_account_id: u64, count: u32, flags: u64 }       // range into existence + initial flags
AccountLinked       { parent_id: u64, type_id: u16, child_id: u64 }         // link a bucket to a parent
AccountFlagsUpdated { account_id: u64, prev_flags: u64, new_flags: u64 }    // set one account's flags word
```

They are written as **followers in the ADR-020 trailer layout** (followers first, the closing `TxMetadata` last), covered by its CRC and `sub_item_count`. They carry **no amount**, so a layout-only transaction is zero-sum-trivially-valid and may be mixed with `TxEntry` credit/debits in one transaction. A bulk open of N accounts is a single `AccountOpened` record. Recovery replays them into the cell flags, the link maps, and the allocator high-water (`next_account_id = max(begin+count)`), which is also persisted in the snapshot.

### 5. Snapshot phase — write-path-first, eventually-consistent reads

The snapshot's single writer applies committed records while the whole world reads. Priority is the write path: if it lags, snapshot lag explodes under load. Reads are low-pressure, client-cacheable, and **eventually consistent** (principle P2).

```rust
struct SnapshotAccount { balance: AtomicI64, flags: AtomicU64 }   // mirrors the transactor cell
// accounts: ArcSwap<Vec<SnapshotAccount>>   (swapped only on resize — see §1.1)
// links:    SkipMap<(u64, u16), u64>
```

- Balances and flags are **per-field atomics with `Relaxed` ordering** — no `Release`/`Acquire` barrier, so the write path is ≈ a plain store. Per-cell loads/stores stay atomic (no torn values); cross-field/-account views may be momentarily skewed and converge. Two independent atomics suffice because P2 means balance and flags are never read together atomically.
- Measured: the atomic vec sustains ~165–209 M writes/s under a concurrent reader, while a concurrent map is **25–500× slower** — a map would *be* the lag source. Maps are used only for cold links.
- The sealed snapshot **file** still gets a consistent `last_tx_id` watermark via the existing checkpoint mechanism — off both the live read path and the hot write path. The durable side stays strongly consistent; only live reads relax.

The transactor and snapshot are thus structurally parallel: AoS cell on both sides, same link key; the snapshot adds atomics + ordering for its concurrency and enumeration needs.

### 6. Operations and the WASM host surface

- **`OpenAccount { count }`** — user/admin operation; opens `OPEN` accounts (status lane 0 = OPEN).
- **`linked_account(account_id, type_id) -> child_id`** — WASM host function, **get-or-create**. On a miss it allocates the next sequential id, opens it `PROGRAMMED`, links `(account_id, type_id) → child`, and returns it — emitting `AccountOpened` + `AccountLinked` followers. This is how program-defined buckets come into existence (lazily, by the program) and is the basis of reserve/release.
  - It is a **mutating** host call, so the transactor's `rollback()` extends to reverse a create (restore the allocator, set the cell back to `NOT_EXISTENT`, remove the link) when the transaction later fails — matching the followers `rollback_to` discards. Without this, in-memory state would diverge from the committed WAL and recovery would not reproduce the bucket. **Correctness requirement.**
  - It can fail `ACCOUNT_LIMIT_EXCEEDED` when the id space is exhausted.
  - Determinism: only the leader runs WASM and allocates; followers and recovery replay the recorded ids without re-running WASM (consistent with ADR-014's execute-once / replicate-effects).
- **`credit` / `debit`** become existence-aware (fail `ACCOUNT_NOT_FOUND` on a non-existent target).
- **`get_flag` / `has_flag` / `set_flag`** are WASM host functions. **Flags are WASM-only** — there is no user-facing flags operation. A user who needs to manipulate flags (account type, freeze, policy, …) does so by writing a WASM function, which emits `AccountFlagsUpdated`. (One guard to settle: whether the status lane (0) is protected from WASM self-modification.)
- Reserve/release are ordinary WASM functions using `linked_account` + `credit`/`debit`. **No privileged built-in operation.**

Two creation paths by design: **user accounts** via `OpenAccount` (OPEN); **program buckets** lazily via `linked_account` (PROGRAMMED).

### 7. SYSTEM account and access guards

- **SYSTEM account (id 0)** is bootstrapped at genesis as an existent internal account (status `SYSTEM`) — a system constant, no `OpenAccount`, no WAL record, re-established on every startup. The normal existence check passes (nonzero flags); no hot-path special-case.
- **Write guard:** user-facing operations (Deposit/Withdrawal/Transfer) reject any *user-named* account whose status ≠ `OPEN`. SYSTEM and PROGRAMMED are reachable only via WASM or as internal counterparties (e.g. SYSTEM as the zero-sum side of a Deposit, where the user never names id 0). Zero-sum still bounds WASM, so touching SYSTEM cannot mint value.

### 8. GetBalance — main-only default, opt-in breakdown, internal accounts not queryable

```
GetBalance { account_id, include_linked: bool = false }
  -> { balance: i64, linked: [ { type_id: u16, balance: i64 } ] }   // `linked` empty unless include_linked
```

- Default returns **only** the account's own balance; linked buckets are **not exposed by default**.
- With `include_linked = true`, the response also lists each bucket as **`(type_id, balance)`** — deliberately **not** the child account_id. Internal bucket ids never leave the system; clients address buckets logically by `(parent, type_id)`. Enumeration uses the snapshot SkipMap range scan.
- **Internal accounts are not queryable.** GetBalance on a non-`OPEN` account errors: `NOT_FOUND` for `NOT_EXISTENT`, *not-queryable* for `PROGRAMMED`/`SYSTEM`. The read side reads the status lane from `SnapshotAccount` to enforce this. Bucket balances are visible only through their parent's breakdown — the read-side counterpart to the §7 write guard.
- This guard is on the **external** GetBalance API only; WASM's internal `get_balance(id)` is unrestricted (it already holds the id via `linked_account`).

### Principles

- **P1 — WAL entry types are concrete and self-defining.** Each WAL entry type does exactly one concrete thing, and its named fields fully and unambiguously define it. No nested discriminators, no op-codes reinterpreting fields, no overloaded/union fields — a replayer, auditor, or human understands a record from its kind and fields alone. (Should also become a doc-comment on `WalEntryKind`.)
- **P2 — Balances are eventually consistent (read side).** The online read view may lag and need not be strictly ordered or point-in-time. This prioritises the snapshot write path and permits `Relaxed` ordering plus client-side caching. The durable WAL and sealed snapshot files remain the strongly-consistent source of truth.

## Consequences

### Positive
- Accounts provably exist; mistyped ids fail instead of silently succeeding.
- Clean separation of user funds from internal/program-controlled funds, enforced symmetrically on the write path (op guard, §7) and read path (GetBalance guard, §8).
- Reserved/bonus/escrow balances are first-class, linked, and reportable; holds are modeled as ordinary atomic transfers into a bucket, reusing the existing zero-sum + rollback machinery.
- Programs create and own buckets ergonomically (`linked_account` get-or-create); internal bucket ids are never exposed to clients.
- The write hot path is untouched: balance mutation stays a dense `Vec` index; flags ride free in the cell's existing padding; links are off the mutation path and cold on the snapshot.
- Transactor and snapshot stages stay structurally parallel ("similar in mind").
- Memory scales with the *actual* account population (no fixed `max_account_count` to pre-allocate), and capacity grows online without a restart (§1.1).

### Negative
- Breaking on-disk and wire format (acceptable: pre-production, no users). Every account now requires a creation record; benchmarks/tests must `OpenAccount` first.
- Per-account memory roughly doubles (8 B → 16 B) — touches ADR-002's memory claims.
- `linked_account` lets a program grow the account space implicitly;

### Neutral
- Two distinct creation paths (user `OpenAccount` vs. program `linked_account`).
- Transactor links use a `HashMap`, the snapshot uses an ordered `SkipMap` — intentional asymmetry (point-resolve vs. enumerate).
- Eventual consistency for live reads becomes an explicit, relied-upon property.

## Implementation status / deviations
- **Account ids stay `u64` (not `u32`).** The §1/§2 draft narrowed ids to `u32` for memory; we kept `u64` — the per-account saving wasn't worth the churn across the storage records, transactor, snapshot, seal, gRPC wire, and every test. No `< 2^32` wire validation.
- **`ACCOUNT_LIMIT_EXCEEDED` removed.** The id space is effectively inexhaustible, so `open_accounts` / `linked_account` are infallible and **panic** on the unreachable allocator overflow instead of returning a handled `FailReason`. The `FailReason` slot (6) is retired.
- **Read guard (§8) covers PROGRAMMED only, not SYSTEM.** `GetBalance` rejects PROGRAMMED buckets (program-internal money); SYSTEM (id 0) stays observable as the well-known zero-sum counterparty (invariant tests read it).
- **WASM `set_flag` may write any lane, including status (0).** No self-modify guard — programs may change their accounts' status.

## References
- ADR-001 — Entries-Based Execution Model
- ADR-002 — Vec-Based Balance Storage
- ADR-006 — WAL, Snapshot, and Seal Durability
- ADR-014 — WASM Function Registry and Function Operation Execution
- ADR-020 — Trailer Metadata — Commit-Record WAL Layout
- `crates/ledger/src/transactor.rs` — `TransactorState`, `credit`/`debit`/`get_balance`, rollback
- `crates/ledger/src/snapshot.rs` — snapshot balances and reads
- `crates/storage/src/entities.rs` — `WalEntryKind`, record structs, `FailReason`, `SYSTEM_ACCOUNT_ID`
- `crates/ledger/src/wasm_runtime.rs` — WASM host linker
- `crates/proto/proto/ledger.proto` — gRPC API
