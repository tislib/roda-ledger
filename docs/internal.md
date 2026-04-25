# Internal

Single source of truth. Each `§` states one complete invariant or decision in
its own paragraph. Source code points back to these paragraph numbers;
ambiguous behaviour is resolved by what this document says.

Stages:

1. **The Ledger** — `§1..§11` (this document, current).
2. **The Storage** — pending.
3. **The Cluster** — pending.

---

## §1 Domain model

### §1.1 Accounts and the system account

An account is identified by a `u64`. Accounts are not created explicitly; they
come into existence the first time a transaction references them. The system
account is `SYSTEM_ACCOUNT_ID = 0` and serves as the source/sink for every
deposit and withdrawal — it is required by the zero-sum invariant (§4.11), not
a convention. The system account's balance is never tracked or queried.
[src/entities.rs:21]

### §1.2 Balances

A balance is an `i64`. Balances are *derived state*: the source of truth is the
ordered transaction log, and a balance is the result of applying every entry
that has touched the account. Arithmetic uses saturating add/sub so a malformed
operation cannot overflow the account silently. Default protection prevents
balances from going negative (§4.9), and only WASM functions may explicitly
bypass this by inspecting `get_balance` themselves. [src/balance.rs]

### §1.3 Operation vs. transaction

These are two distinct things and the distinction matters. An *operation* is
the caller's intent (`Deposit`, `Withdrawal`, `Transfer`, `Function`). A
*transaction* is the immutable record produced by executing an operation —
permanent, ordered, and never modified. The client submits operations; the
ledger writes transactions; the Transactor (§4) is the bridge. When a caller
queries a transaction it sees the *entries* (the credits/debits the Transactor
emitted), not the original operation.

### §1.4 The four operation types

Roda-ledger accepts exactly four operation types, all carrying a `user_ref`
idempotency key (§5):

- `Deposit { account, amount }` — credits `account`, debits the system account.
- `Withdrawal { account, amount }` — debits `account`, credits the system account.
- `Transfer { from, to, amount }` — debits `from`, credits `to`.
- `Function { name, params, user_ref }` — invokes a previously registered WASM
  module by name (§6).

`Function` is the single extension surface; any multi-account or domain-specific
logic that does not fit the named types is expressed as a registered function.
[src/transaction.rs]

### §1.5 Function operation parameters

`Function` parameters are a fixed `[i64; 8]`. Slots a caller does not need are
zero-padded. The arity is locked at the type level so the host ABI (§6.7) and
WAL/transaction representation never require variable-length parameter blocks.
[src/transaction.rs]

### §1.6 WAL record format invariants

Every WAL record is exactly 40 bytes; the size is enforced by compile-time
`const _: () = assert!(size_of::<...>() == 40)` for every record type. The
first byte of every record is its `entry_type` discriminant, which makes the
WAL stream `[record][record][record]…` self-describing without a separate
length-prefix or framing layer. All record `#[repr(C)]` structs are laid out
without implicit padding so they satisfy `bytemuck::Pod` for zero-copy
serialization. [src/entities.rs:235-240, src/entities.rs:67-70]

### §1.7 The six WAL record kinds

`WalEntryKind` enumerates the only six record types that may appear in the
log: `TxMetadata = 0`, `TxEntry = 1`, `SegmentHeader = 2`, `SegmentSealed = 3`,
`Link = 4`, `FunctionRegistered = 5`. New record types must extend this enum;
the discriminant is the first byte of the record (§1.6). `SegmentHeader` and
`SegmentSealed` are storage-side concerns and detailed in Stage 2.
[src/entities.rs:5-12]

### §1.8 TxMetadata

`TxMetadata` is the per-transaction header record. Its fields:

- `entry_count: u8` — number of `TxEntry` records that follow.
- `link_count: u8` — number of `TxLink` records that follow the entries.
- `fail_reason: FailReason` — `0` on success, otherwise see §1.12.
- `crc32c: u32` — covers `TxMetadata` (with this field zeroed) plus all
  following entries plus all following links.
- `tx_id: u64`, `timestamp: u64`, `user_ref: u64`.
- `tag: [u8; 8]` — execution-engine identification (e.g. `b"fnw\n" ++ crc[0..4]`
  for WASM-produced transactions; see §6.14).

The `u8` widths of `entry_count` and `link_count` are hard limits: a transaction
cannot emit more than 255 entries or 255 links. The Transactor rejects
overflow with `ENTRY_LIMIT_EXCEEDED` (§1.12). [src/entities.rs:53-65]

### §1.9 TxEntry

`TxEntry` is the per-credit/per-debit record produced by the Transactor:

- `kind: EntryKind` — `Credit = 0` or `Debit = 1`.
- `tx_id: u64` — links back to the owning `TxMetadata`.
- `account_id: u64`, `amount: u64` — integer minor units; no floats, no decimals.
- `computed_balance: i64` — the running balance of `account_id` *after* this
  entry is applied. The Transactor computes it on the write side; the
  Snapshotter and recovery use it directly without re-running operation logic.
  This field is what makes recovery O(entries) rather than O(operations) and
  is the foundation of the entries-based execution model. [ADR-0001]

[src/entities.rs:98-108]

### §1.10 TxLink

`TxLink` records a relationship between two transactions. `link_kind` is
either `Duplicate(0)` (this transaction was rejected as a duplicate of
`to_tx_id` — see §5.6) or `Reversal(1)` (this transaction reverses
`to_tx_id`). Links are immutable WAL records and form the audit trail of
non-trivial transaction relationships. [src/entities.rs:110-136, ADR-0009]

### §1.11 FunctionRegistered

`FunctionRegistered` is the WAL record for a register/unregister event. It
carries `name: [u8; 32]` (null-padded ASCII snake_case, validated ≤ 32 bytes
at registration), `version: u16` (per-name monotonic, starting at 1, max
65535), and `crc32c: u32` of the WASM binary. A `crc32c == 0` value signals
*unregister* — the audit trail is preserved as a versioned event rather than
a deletion. The on-disk binary file is the secondary signal; if the two
disagree the WAL wins. `FunctionRegistered` carries no `tx_id`, and the
pipeline's commit/snapshot indexes do not advance on it (it is not a
financial transaction). [src/entities.rs:147-187, ADR-0014]

### §1.12 FailReason space

`FailReason` is a `u8`. The space is partitioned:

| Code | Meaning |
|---|---|
| `0` | `NONE` (success) |
| `1` | `INSUFFICIENT_FUNDS` — balance check failed in `Withdrawal`/`Transfer` |
| `2` | `ACCOUNT_NOT_FOUND` |
| `3` | `ZERO_SUM_VIOLATION` — credits ≠ debits for the transaction (§4.11) |
| `4` | `ENTRY_LIMIT_EXCEEDED` — > 255 `TxEntry` records (§1.8) |
| `5` | `INVALID_OPERATION` — also wasmtime-layer failure: link, instantiation, trap (§6.11) |
| `6` | `ACCOUNT_LIMIT_EXCEEDED` — `account_id ≥ max_accounts` (§4.4) |
| `7` | `DUPLICATE` — `user_ref` matched within the active window (§5.6) |
| `8..=127` | reserved for future standard reasons |
| `128..=255` | user-defined; returned by WASM functions and passed through unchanged |

Anything in `128..=255` is the WASM author's contract with their callers; the
ledger never reinterprets it. [src/entities.rs:25-51, ADR-0001]

---

## §2 Pipeline structure

### §2.1 Stages and guarantees

The pipeline has four execution stages and one independent maintenance stage.
A transaction moves through Sequencer → Transactor → WAL → Snapshotter, and
each step adds exactly one guarantee:

| Stage | Guarantee | Status |
|---|---|---|
| Sequencer | Monotonic ID, permanent global order | `PENDING` |
| Transactor | Executed, serializable, result known | `COMPUTED` |
| WAL | Durable on disk, crash-safe | `COMMITTED` |
| Snapshotter | Visible to readers (`get_balance`, hot indexes) | `ON_SNAPSHOT` |

Seal runs in parallel and is responsible for closed-segment sidecars and
on-disk indexes (Stage 2). [docs/03-architecture.md:23-37]

### §2.2 Status is derived, not stored

A `TransactionStatus` is computed at query time from the pipeline's progress
indexes — never persisted as a record. A transaction is `Computed` iff
`compute_index ≥ tx_id`, `Committed` iff `commit_index ≥ tx_id`,
`OnSnapshot` iff `snapshot_index ≥ tx_id`. Failed transactions are recognised
through the rejection registry (§4.17). This avoids a per-transaction state
machine on disk and makes status a pure function of the pipeline. [src/transaction.rs]

### §2.3 Pipeline ownership and context slicing

`Pipeline` is a singleton `Arc<Pipeline>` and is the sole owner of every
inter-stage queue, every progress index, the shutdown flag, and the shared
wait strategy. Stages never own this state directly — each receives a typed
context wrapper (`SequencerContext`, `TransactorContext`, `WalContext`,
`SnapshotContext`, `SealContext`, `LedgerContext`) that exposes only the
slice that stage may legally read or publish. The contexts are thin Arc
clones; the wrapping is purely for type-level encapsulation. [src/pipeline.rs:22-50, 88-120]

### §2.4 Inter-stage queues

The three inter-stage channels are `crossbeam::ArrayQueue` instances —
lock-free, fixed-capacity, single-producer / single-consumer:

- `sequencer → transactor`: `TransactionInput`.
- `transactor → wal`: `WalEntry`.
- `wal → snapshot`: `SnapshotMessage`.

Capacity is fixed at construction from `queue_size` (§10.2). A full queue is
the only backpressure signal; producers spin/yield until space exists. There
is no other flow-control mechanism. [src/pipeline.rs:29-31]

### §2.5 Global progress indexes

The pipeline holds five `CachePadded` atomic indexes plus one shutdown flag:

- `sequencer_index: AtomicU64` — next `tx_id` to be handed out.
- `compute_index: AtomicU64` — last `tx_id` executed by the Transactor.
- `commit_index: AtomicU64` — last `tx_id` durably written by the WAL.
- `snapshot_index: AtomicU64` — last `tx_id` reflected in the Snapshotter.
- `seal_index: AtomicU32` — last segment id sealed.
- `running: AtomicBool` — global shutdown flag.

`CachePadded` is non-negotiable: progress updates from one stage must not
invalidate the cache line of an adjacent index. All indexes are monotonic —
they only ever move forward. The mapping `commit_index ≥ N` ⇒ every
transaction `≤ N` is committed (no committed gaps) is what makes per-call
wait levels (§9) meaningful. Memory ordering is `Release` on publication,
`Acquire` on observation; `running` uses `Relaxed` because shutdown
propagation is eventual. [src/pipeline.rs:33-50]

### §2.6 Index initialisation

`sequencer_index` initialises to `1` (not `0`); `last_sequenced_id` is
therefore `sequencer_index − 1`. All other indexes initialise to `0`. The
pre-increment avoids any collision with the sentinel value `0` and means a
freshly constructed pipeline that has assigned no IDs reports
`last_sequenced_id == 0`. On startup `sequencer_index` is restored to
`last_committed_tx_id + 1` (§11.2), preserving the same invariant.
[src/pipeline.rs:70-74]

### §2.7 Backpressure and shutdown

Backpressure is implicit and propagates through queue saturation alone: a
slow Snapshotter fills the wal→snapshot queue, which stalls the WAL, which
fills the transactor→wal queue, which stalls the Transactor, which fills the
sequencer→transactor queue, which stalls `submit()`. There is no rate
limiter, leaky bucket, or admission control. Shutdown is the inverse signal:
`Pipeline::shutdown()` clears `running`; every stage's idle loop checks the
flag and exits. The shutdown read is `Relaxed` because eventual visibility
is sufficient — the queues drain to a safe state regardless of the precise
moment a stage notices. [src/pipeline.rs]

---

## §3 Sequencer

### §3.1 Placement on the caller's thread

The Sequencer has no dedicated thread. It runs synchronously inside
`Ledger::submit` on whichever thread the caller invokes from. Its work is
intentionally trivial: one atomic increment plus one queue push. Adding a
thread (and the cross-thread channel that would imply) would only add
latency. [src/sequencer.rs:6-12]

### §3.2 ID assignment

ID assignment is a single `fetch_add(count, Acquire)` on `sequencer_index`.
`submit(operation)` reserves one ID and stamps the resulting `Transaction`.
`submit_batch(operations)` reserves a contiguous run of `len(operations)`
IDs in one atomic step; per-operation IDs inside the batch are
`start_tx_id + index`. Batches are atomic at the sequencer: their IDs are
guaranteed consecutive even under concurrent submitters.
[src/sequencer.rs:23-62]

### §3.3 Permanence of position

Once the Sequencer assigns an ID, the transaction's position in the global
order is permanent. No later stage can reorder, reassign, or skip an ID.
This is what allows downstream stages (Transactor, WAL, Snapshotter) to
publish progress as a single monotonically advancing index per stage —
there are no holes to fill or gaps to wait on. [docs/03-architecture.md:59-64]

### §3.4 Backpressure spin/yield policy

When the sequencer→transactor queue is full, the submitter spin-loops on
`crossbeam_queue::ArrayQueue::push`, calling `std::hint::spin_loop()` on
each retry and `std::thread::yield_now()` every 10 000 retries. There is no
timeout — `submit` returns only after the operation is queued. The choice
of *yield every 10 000* trades a small amount of pathological-case CPU for
the latency of being first in line when space appears.
[src/sequencer.rs:29-37]

---

## §4 Transactor — the single writer

### §4.1 Single-writer rule

The Transactor is one dedicated thread that processes one transaction at a
time, in strict ID order. This is the source of all correctness guarantees:
there are no races, no conflicts, no need for locks or conflict resolution.
Throughput does not come from parallelising execution — it comes from
sequencing, durability, and snapshotting running concurrently *around* the
Transactor. The single-writer ceiling is the upper bound on per-ledger
write throughput. [ADR-0001, docs/01-concepts.md:68-74]

### §4.2 Thread-local state

`TransactorState` holds the balance vector, the per-step entries buffer,
the `position` marker, the current `tx_id`, and the current `fail_reason`.
It lives in `Rc<RefCell<...>>` and never crosses a thread boundary —
neither does anything that holds it (notably the `WasmRuntimeEngine`, which
is `!Send`). All host-call implementations borrow `RefCell` mutably, but
because the Transactor thread is the only thread that touches the state,
there is no contention and `RefCell`'s runtime check costs only a single
flag bit per borrow. [src/transactor.rs]

### §4.3 Balance cache layout

The Transactor's balance cache is a `Vec<Balance>` indexed directly by
`account_id`. Lookups and updates are O(1) array accesses with no hashing.
The vector is pre-allocated to `max_accounts` at construction and never
resized. [ADR-0002]

### §4.4 The `max_accounts` ceiling

Any operation referencing an `account_id ≥ max_accounts` is rejected with
`ACCOUNT_LIMIT_EXCEEDED` (§1.12) before the operation can mutate any state.
This is the price of the direct-indexed `Vec<Balance>` layout (§4.3) — the
account-ID space must be sized at startup. Resizing would require pausing
the Transactor and reallocating every read-side cache (§7.3), which is not
worth the complexity given how rarely a deployed ledger needs to grow its
ceiling. [ADR-0002]

### §4.5 Linearizability of the write path

The Transactor never reads stale balances. Because it is the sole writer
and operates on the in-memory cache (§4.3), every read it performs during
operation execution observes the cumulative effect of every earlier
transaction. The write path is therefore linearizable by construction;
the read path requires an explicit `submit_wait(snapshot)` (§9.5) for the
same guarantee. [docs/01-concepts.md:70-74]

### §4.6 Per-step entries buffer

`TransactorState` carries a `Vec<WalEntry>` that accumulates every record
produced during a step (a step may execute many transactions in sequence).
The buffer is flushed as a single batch onto the transactor→wal queue.
Batching here amortises the cost of queue contention and gives the WAL
stage a contiguous slice to write into its in-memory buffer.

### §4.7 The `position` marker

While accumulating entries, the Transactor maintains a `position: usize`
pointer into the entries buffer marking the start of the *current*
transaction's records. `verify` (§4.11), `rollback` (§4.12), and CRC
computation (§4.10) all scope their iteration to `entries[position..]`
without searching. Incrementing `position` after a transaction completes
is what advances the per-step cursor. [src/transactor.rs]

### §4.8 Built-in operation semantics

For built-in operation types the Transactor runs native Rust:

- **`Deposit { account, amount }`** — `debit(SYSTEM_ACCOUNT_ID, amount)`,
  `credit(account, amount)`.
- **`Withdrawal { account, amount }`** — checks `balance(account) ≥ amount`;
  on success, `debit(account, amount)`, `credit(SYSTEM_ACCOUNT_ID, amount)`;
  on failure, fail with `INSUFFICIENT_FUNDS`.
- **`Transfer { from, to, amount }`** — checks `balance(from) ≥ amount`;
  on success, `debit(from, amount)`, `credit(to, amount)`; on failure,
  fail with `INSUFFICIENT_FUNDS`.

Each `credit` / `debit` produces one `TxEntry` record (§1.9) with the
running `computed_balance` field already populated.

### §4.9 Default INSUFFICIENT_FUNDS protection

For built-in operations, the Transactor refuses any change that would drive
an account balance negative (it checks before the debit). This is the
default protection. WASM functions (§6) are the only mechanism for
producing a negative balance: a function must explicitly call `get_balance`
itself and decide to proceed. There is no configuration knob to disable
the built-in check. [docs/01-concepts.md:19]

### §4.10 Per-transaction CRC32C

After verify and any rollback are complete, the Transactor computes a
CRC32C over `TxMetadata` (with its own `crc32c` field zeroed) followed by
all `TxEntry` records of this transaction followed by all `TxLink` records,
and writes the result into `TxMetadata.crc32c`. The CRC therefore covers
exactly one transaction's full record stream and uses CRC32C
(Castagnoli) — hardware-accelerated on modern CPUs. Because the field
participates in the digest while being part of the record, it must be
zeroed during computation; consumers that want to verify the CRC must
zero it before recomputing. [src/transactor.rs]

### §4.11 Zero-sum invariant

Every transaction must net to zero: `sum(credits) == sum(debits)` across
all `TxEntry` records of that transaction. The Transactor verifies this
after operation execution and before any state is committed; a violation
raises `ZERO_SUM_VIOLATION` (§1.12) and the transaction is rolled back
(§4.12). The invariant is per-transaction — there is no cross-transaction
arithmetic. Because each individual transaction nets to zero, the sum of
all balances across all accounts is always zero — an emergent property,
not a configuration option. [docs/01-concepts.md:58-64, ADR-0001]

### §4.12 Rollback

When verify fails or a WASM execution rejects, the Transactor reverses
every credit/debit produced in `entries[position..]` against the live
balance cache, truncates the buffer to keep only the leading `TxMetadata`,
sets `fail_reason`, and recomputes the CRC. The cost is O(entry_count) and
is acceptable because typical entry counts are small. The metadata is
preserved deliberately — see §4.13. [src/transactor.rs]

### §4.13 Failed transactions are journaled

A transaction that fails — whether by deduplication, an execution error,
or a zero-sum violation — still produces a `TxMetadata` record with a
non-zero `fail_reason`, plus any `TxLink` records that describe the
relationship (e.g. `Duplicate` linking to the original). This metadata
flows through the WAL and the rest of the pipeline like any other record.
The audit trail is therefore complete: every submitted operation that
reached the Transactor leaves a trace, success or failure. [ADR-0001]

### §4.14 No torn writes per account

Every individual credit or debit is a single atomic store. A concurrent
reader on the read-side balance cache (§7.3) will never observe a
partially-written balance for any single account — the value is always a
complete, valid `i64`. [docs/01-concepts.md:52]

### §4.15 Intermediate state is visible across accounts

Within a multi-step operation (a `Transfer`, or a `Function` issuing
several `credit`/`debit` calls), the Transactor applies each step in
sequence. A concurrent reader can observe some of these stores before
others have landed — for example, seeing a credit applied to one account
before the matching debit on another has landed. This is by design.
`last_tx_id` returned by `get_balance` is the signal that a transaction
is fully settled across all accounts: it is updated only after all
entries of a transaction have been applied (§7.6). Callers who need to
see a multi-account transaction atomically should wait for `last_tx_id`
to advance past their `tx_id` or use `submit_wait(snapshot)`.
[docs/01-concepts.md:54]

### §4.16 `tx_id` is on state, not on the host call

The current `tx_id` is set on `TransactorState` at the start of each
transaction and read by host calls (§6.7) via the shared
`Rc<RefCell<TransactorState>>`. It is never passed across the WASM
host-call boundary as a parameter. This avoids both the cost of an
extra wasmtime call argument and the risk of a function caching or
forging a stale `tx_id`.

### §4.17 Rejection registry

Rejected transactions are recorded in `Arc<SkipMap<u64, FailReason>>`,
shared with the Ledger so that status queries can resolve `Error(reason)`
for any failed `tx_id` (§2.2). The map is sparse — successes are not
recorded — because in steady state the overwhelming majority of
transactions succeed. [src/transactor.rs]

---

## §5 Deduplication and the active window

### §5.1 Always on, no global toggle

Deduplication is always on. There is no configuration that disables it
system-wide. The only opt-out is per-transaction (§5.5). [docs/01-concepts.md:122-130]

### §5.2 Active window and flip-flop cache

The active window size equals `transaction_count_per_segment` — the same
number that drives WAL segment rotation (Stage 2). The dedup cache is two
`FxHashMap<user_ref, tx_id>` maps, `active` and `previous`. When `tx_id`
crosses the segment boundary, the maps flip: `active` becomes `previous`,
the previous map's allocation is reused as the new (cleared) `active`.
Effective dedup coverage is therefore `N..2N` transactions — at least one
full window, at most two. [src/dedup.rs, ADR-0013]

### §5.3 Determinism: count-based, not time-based

The flip is driven by `tx_id`, not by wall-clock time. The same N
transactions always provide the same protection, whether they arrive in
one second or one hour. Idempotency guarantees do not degrade under load
spikes and do not silently widen during quiet periods.
[docs/01-concepts.md:128, ADR-0013]

### §5.4 `user_ref = 0` opt-out

A `user_ref` value of `0` opts the *individual* transaction out of the
dedup check. The cache itself remains active and continues to record
every `user_ref ≠ 0`. There is no path that disables the cache globally
short of recompiling. [docs/01-concepts.md:130]

### §5.5 Hit handling

On a hit, the Transactor emits `TxMetadata` (with `fail_reason = DUPLICATE`)
followed by `TxLink { kind: Duplicate, to_tx_id: original_tx_id }`. The
caller therefore always recovers the *original* `tx_id` for an idempotent
retry, not just a "rejected" status. [ADR-0009, §1.12]

### §5.6 Recovery seeding

On startup, `DedupCache::recover_entry(user_ref, tx_id, last_tx_id)` is
called for each WAL transaction inside the active window. Entries outside
`2 × window` are dropped; entries within the window are placed into the
correct half (`active` vs `previous`) based on their relative position.
Post-recovery dedup behaviour is therefore identical to pre-crash — a
duplicate submitted before and after a restart is rejected the same way.
[src/dedup.rs]

---

## §6 WASM runtime — embedded execution

### §6.1 Inside the Transactor, not a separate stage

The WASM runtime is **not** a separate pipeline stage. It is a component
*inside* the Transactor and executes on the Transactor thread.
`Operation::Function` is dispatched by the Transactor like any built-in
operation (§4.8); the only difference is *how* `TxEntry` records are
produced — by calling into a sandboxed WASM handler that issues
`credit`/`debit` host calls instead of running a hard-coded Rust branch.
This placement means a function execution inherits every Transactor
invariant: single-writer ordering (§4.1), zero-sum (§4.11), atomic
rollback (§4.12), deduplication (§5), and CRC (§4.10) — without any new
coordination. [docs/03-architecture.md:83-100, ADR-0014]

### §6.2 Pipeline-identical flow

A `Function` operation flows through Sequencer → Transactor → WAL →
Snapshotter exactly like a `Deposit`. WAL segmentation, sealing,
snapshotting, and replay treat function-produced entries identically to
native ones. There is no second persistence path, no second recovery
path, no second wait-level mechanism. [docs/03-architecture.md:91-97]

### §6.3 One Engine, one Linker per ledger

There is one `wasmtime::Engine` and one `Linker<WasmStoreData>` per
ledger, both held behind `Arc<WasmRuntime>` and cheap to clone. The
Linker carries the host imports (§6.7); the Engine compiles and caches
modules. Constructing either is expensive and is therefore done exactly
once. [src/wasm_runtime.rs]

### §6.4 Shared registry

The shared registry is a small `RwLock<HashMap<name, Registered{version,
crc32c, Arc<Module>}>>`. It is touched only on register, unregister, and
cache miss / revalidation — never on every transaction. The hot path
(§6.5) avoids the lock entirely. [src/wasm_runtime.rs]

### §6.5 Per-Transactor caller cache

Each Transactor holds a thread-local `HashMap<name, CachedHandler>`
where `CachedHandler` carries the compiled `TypedFunc` plus the
`update_seq` of the registry it was verified against. Resolution is:

1. Look up `name` in the local cache.
2. If the cached `update_seq` matches the registry's current value,
   the cached `TypedFunc` is used directly with no lock.
3. Otherwise, take the registry read lock, fetch the current
   `(version, crc32c, Module)`, instantiate, install into the cache,
   stamp the entry with the new `update_seq`.

Cache invalidation is per-name: registering or unregistering `foo` does
not evict the cached entry for `bar`. [src/wasm_runtime.rs, docs/03-architecture.md:140-143]

### §6.6 Required export

Every registered binary must export exactly one function:
`execute(i64, i64, i64, i64, i64, i64, i64, i64) → i32`. The eight
parameters correspond to the `Function` operation's `[i64; 8]` (§1.5).
The `i32` return is the function's status: `0` is success, anything
else is mapped per §1.12 / §6.12. [ADR-0014]

### §6.7 Host imports

The host module is `"ledger"` and exports exactly three imports:

- `ledger.credit(account_id: i64, amount: i64) → ()`
- `ledger.debit(account_id: i64, amount: i64) → ()`
- `ledger.get_balance(account_id: i64) → i64`

Each call borrows `Rc<RefCell<TransactorState>>` and either appends a
`TxEntry` to the per-step buffer (`credit`/`debit`) or reads from the
balance cache (`get_balance`). The `tx_id` is pulled from the state
(§4.16), not the call. [ADR-0014, src/transactor.rs]

### §6.8 Determinism rule

The host ABI is intentionally narrow: no clocks, no random, no I/O, no
threads, no atomics. Every legal function is therefore a pure mapping
from `(params, observed balances) → (status, credits, debits)`. This is
not a stylistic choice — it is what makes the WAL replayable on a
follower without re-running the WASM code (§6.9). Adding any
non-deterministic host call would make replication unsafe.
[docs/03-architecture.md:122-125]

### §6.9 Replication implication

Because of §6.8, a future Raft follower can apply the WAL records
produced by a function on the leader directly — without instantiating
the WASM module, without re-running `execute`, and without observing any
of the function's internal state. The leader is the only node that
needs to compile and run the binary; followers see only the
`TxEntry`/`TxLink`/`TxMetadata` stream the leader produced. This is what
enables the future cluster (Stage 3). [docs/03-architecture.md:124-125]

### §6.10 Atomicity of function execution

Host-call effects accumulate in an isolated host-side execution context
inside `TransactorState`. Nothing is applied to the live balance cache
until *all* of the following hold: `execute` returned `0`, the captured
credits and debits balance (§4.11), and no wasmtime-layer error
occurred. If any of those fail, the entire batch of host calls is
discarded — exactly as a built-in operation rollback (§4.12). The WAL
queue therefore never sees a partial function execution.
[docs/03-architecture.md:106-118]

### §6.11 Wasmtime-layer failure mapping

A wasmtime layer failure — link error, instantiation error, or runtime
trap — is mapped to `INVALID_OPERATION = 5` (§1.12) and rolled back. The
mapping is single-valued by design: from outside the runtime, *any*
infrastructure-level WASM failure looks the same. The function author's
own status codes (§6.12) live in a disjoint range. [src/wasm_runtime.rs, §1.12]

### §6.12 Status code passthrough

The `i32` return of `execute` is interpreted as follows:

- `0` → success (provided zero-sum holds; §4.11).
- `1..=7` → mapped to the corresponding standard `FailReason` (§1.12).
- `8..=127` → reserved; treated as `INVALID_OPERATION`.
- `128..=255` → user-defined; passed through into `TxMetadata.fail_reason`
  unchanged.
- Anything else (negative, > 255) → treated as `INVALID_OPERATION`.

User-defined codes are the WASM author's contract with their callers; the
ledger never reinterprets them. [ADR-0014]

### §6.13 Entry-limit enforcement

If a function emits more than 255 `credit`/`debit` calls in a single
execution, the `entry_count: u8` field of `TxMetadata` (§1.8) cannot
encode the count. The Transactor fails the transaction with
`ENTRY_LIMIT_EXCEEDED` (§1.12) and rolls it back. Function authors who
need more than 255 entries must split the work across multiple
transactions.

### §6.14 Audit tag

Every committed function-produced transaction stamps `TxMetadata.tag`
with `b"fnw\n" ++ crc32c[0..4]` — the four-byte literal `"fnw\n"`
followed by the first four bytes of the executing binary's CRC32C
(§1.11). Any past transaction can therefore be traced back to the exact
binary that produced it, even after registrations have changed.
[docs/03-architecture.md:116]

### §6.15 Names and versions

Function names are validated at registration: ASCII snake_case, ≤ 32
bytes, null-padded to 32 in the WAL record (§1.11). Each register or
override bumps the per-name `version` by one, starting at `1`, with a
ceiling of `65535`. The `(name, version, crc32c)` triple uniquely
identifies the executing binary in every transaction. [§1.11, ADR-0014]

### §6.16 Registration is durable

`RegisterFunction` only returns after all four of the following have
happened: the binary has been validated, written atomically to disk
(`tmp + rename`), a `FunctionRegistered` WAL record has been committed,
and the compiled handler has been installed in the live `WasmRuntime`.
A subsequent `Operation::Function` is therefore guaranteed to see the
new version. After a crash, recovery rebuilds the registry from a paired
function snapshot plus the `FunctionRegistered` records that follow it —
the live runtime always matches what the WAL says it should be. (Disk
specifics, snapshot format, and recovery order: Stage 2.)
[docs/03-architecture.md:127-138, ADR-0014]

### §6.17 Unregistration

Unregistration emits a `FunctionRegistered` record with `crc32c = 0`
(§1.11) and writes a 0-byte file under the next version on disk. The
binary history is preserved as a sequence of versions; the function
registry's *current* state is the last record seen. The WAL is
authoritative; the on-disk file is a secondary signal. [§1.11, ADR-0014]

---

## §7 Snapshotter — the read side

### §7.1 Apply, don't re-execute

The Snapshotter applies committed entries to the read-side state. It does
no business logic and never re-executes operations. Because every
`TxEntry` carries `computed_balance` (§1.9), the Snapshotter's job is a
sequence of `store(account_id, computed_balance)` operations plus index
maintenance. This is the entries-based execution model and the reason
recovery and replication are cheap. [ADR-0001]

### §7.2 Why a separate stage

The Transactor's balance cache (§4.3) is *write-side* truth, private and
optimised for writes. The Snapshotter maintains *read-side* truth and is
optimised for concurrent readers. Separating them means readers never
block writers, and the Transactor never needs to coordinate with query
threads. [docs/03-architecture.md:219-226]

### §7.3 Read-side balance cache

The read-side cache is `Arc<Vec<AtomicI64>>`, sized to `max_accounts`. The
Snapshotter publishes balance updates with `Relaxed` (the FIFO before
publication provides the necessary ordering); readers (`get_balance`) load
with `Acquire`. Multiple concurrent readers can call `get_balance`
without locks. The vector is pre-allocated at startup and never resized.
[ADR-0002]

### §7.4 `last_tx_id` freshness signal

`get_balance` returns the current balance plus `last_tx_id` — the highest
`tx_id` whose every entry has been applied to the read-side cache.
`last_tx_id` is the tool callers use to reason about freshness without
blocking, and it is the signal that confirms multi-account transactions
are fully settled (§4.15). For a linearizable read, the caller waits for
`last_tx_id` to pass their `tx_id`, or uses `submit_wait(snapshot)`
(§9.5). [docs/01-concepts.md:54]

### §7.5 Hot transaction index structure

The Snapshotter maintains two pre-allocated circular buffers:

- **circle1** — direct-mapped `tx_id → TxSlot` cache. `TxSlot` is 16 bytes
  (`tx_id: u64`, `offset: u32` into circle2, `entry_count: u8`); the index
  into circle1 is `tx_id & mask`. A slot with `tx_id == 0` is empty; a
  mismatch means the slot has been overwritten.
- **circle2** — entry storage. Each `IndexedTxEntry` is 48 bytes and
  carries the `TxEntry` payload plus `prev_link`, a 1-based index back to
  the same account's previous entry (`0` = no previous). Account chains
  are walked by following `prev_link` until eviction or `0`.

Account heads are a separate direct-mapped table from `account_id` to the
latest circle2 index. No allocation occurs after construction. Eviction is
silent — overwriting a slot does not raise an event. [src/index.rs, ADR-0008]

### §7.6 Hot index sizes and the power-of-two rule

Circle sizes are derived from `transaction_count_per_segment`:

- `circle1_size = next_power_of_two(transaction_count_per_segment)`
- `circle2_size = next_power_of_two(2 × transaction_count_per_segment)`

The power-of-two rule is required by the direct-mapped lookup — `tx_id &
mask` is only correct when `mask = size - 1`. A modulo-based lookup would
work but cost an extra division per query; the rounding-up cost is at most
2× memory in the pathological case. [src/config.rs:80-89]

### §7.7 Eviction and the cold-tier fallthrough

When a hot-index slot is overwritten, queries for the evicted `tx_id` /
account history miss silently. Misses fall through to the cold tier
(on-disk transaction and account indexes built by Seal — Stage 2). The
hot index is therefore correct by construction even under arbitrary
eviction; freshness is the only thing that varies.

### §7.8 Single FIFO for entries and queries

The Snapshotter consumes a single SPSC FIFO of `SnapshotMessage` carrying
both `WalEntry` and `QueryRequest` interleaved. This is what gives
read-your-own-writes for free: a query enqueued after a write sees the
write applied, because the FIFO orders both. There is no second queue,
no priority lane. [src/snapshot.rs]

### §7.9 Inline query execution

`GetTransaction` and `GetAccountHistory` execute inline on the
Snapshotter thread against the current read-side state. The result is
sent through a per-request response channel; the caller blocks on
`recv()`. There is no separate query thread, no shared lock around the
indexer — the Snapshotter owns the indexer exclusively. The Snapshotter
is therefore the bottleneck for concurrent queries, but the only work it
does for a query is a couple of array indexings, so the bottleneck is
generous. [docs/03-architecture.md:225-227]

---

## §8 Inter-stage queues, backpressure, wait strategies

### §8.1 Lock-free SPSC queues

Every inter-stage queue is `crossbeam::ArrayQueue` — single-producer,
single-consumer, lock-free, fixed-capacity. Each stage owns its data
completely; no shared mutable state crosses stage boundaries. The fixed
capacity is sized at construction from `queue_size` (§10.2).
[src/pipeline.rs]

### §8.2 Wait strategies

Every stage's idle / backpressure loop is driven by the shared
`WaitStrategy`. Four variants exist:

- `LowLatency` — every iteration is `spin_loop()`. CPU is burned
  unconditionally; latency is minimised. Suitable for dedicated cores.
- `Balanced` (default) — ~32 spins, then ~16 K `yield_now()` calls, then
  `park_timeout(1 ms)`. Adapts: under load it behaves like `LowLatency`;
  when idle it costs nothing.
- `LowCpu` — no spins, ~100 yields, then park. For deployments where
  background CPU cost matters more than latency.
- `Custom` — exposes the spin / yield / park parameters explicitly.

A single `WaitStrategy` value is shared by every stage of one Pipeline;
there is no per-stage override. [src/wait_strategy.rs, src/pipeline.rs:48-49]

### §8.3 Backpressure as queue saturation

There is no rate limiter, leaky bucket, or admission control. Slowness
in any stage propagates upstream as queue fullness, eventually stalling
`submit()` (§2.7). This is intentional: the queues are the only place
where stages observe each other's progress, and turning them into the
backpressure mechanism keeps the design free of explicit coordination.
[docs/03-architecture.md:240-241]

---

## §9 Wait levels

### §9.1 Four levels

The caller picks one of four wait levels per submission:

- `none` — return `tx_id` immediately. Maximum throughput.
- `transactor` — wait until `compute_index ≥ tx_id` (`COMPUTED`). The
  caller knows whether the transaction succeeded or was rejected.
- `wal` — wait until `commit_index ≥ tx_id` (`COMMITTED`). The
  transaction is durable on disk and survives a crash.
- `snapshot` — wait until `snapshot_index ≥ tx_id` (`ON_SNAPSHOT`). The
  effects are visible to readers via `get_balance`.

[ADR-0010, docs/01-concepts.md:140-147]

### §9.2 Implementation

`wait_for_transaction_level(tx_id, level)` polls the relevant pipeline
index against the requested threshold using the shared `WaitStrategy`
(§8.2). The polling read uses `Acquire` so that a successful wait
synchronises with every store that produced the index advance.
[src/transaction.rs]

### §9.3 Per-call dial

Wait level is per-submission; there is no global default. The caller
chooses the trade-off between latency and consistency on every call.
This is the primary performance dial of the system: a high-throughput
batch ingester might use `none`; a settlement step that must read its
own balance immediately uses `snapshot`.

### §9.4 Linearizable read recipe

The documented way to obtain a linearizable read is
`submit_wait(snapshot)` followed by `get_balance(account_id)`. After the
wait returns, `snapshot_index` is past the submitted `tx_id`, so
`get_balance` reflects the submitted transaction and everything before
it. This is the only blocking primitive a caller needs for linearizable
reads. [docs/01-concepts.md:108-110]

---

## §10 Configuration — ledger-side knobs

### §10.1 `max_accounts`

Pre-allocated capacity of every balance vector and read-side atomic
vector. Default `1_000_000`. Fixed at startup; cannot grow at runtime
(§4.4). Accounts referencing IDs ≥ this value are rejected with
`ACCOUNT_LIMIT_EXCEEDED`. The 1M default chosen because 1M × 8 bytes =
8 MB per balance vector, which fits comfortably in L2/L3 even with
read-side and seal-side duplicates. [src/config.rs:64, ADR-0002]

### §10.2 `queue_size`

Capacity of every inter-stage `ArrayQueue`. Default `1 << 14 = 16_384`.
Not exposed via `config.toml` (it is a `#[serde(skip)]` field). At
runtime, the Sequencer→Transactor, Transactor→WAL, and WAL→Snapshot
queues are *all* sized from this single value — there is no separate
WAL queue knob despite the constructor's `wal_queue_size` parameter
name. [src/config.rs:45,65, src/pipeline.rs:54-56]

### §10.3 `wait_strategy`

Pipeline-mode for every stage's idle/backpressure loop. Default
`Balanced` (§8.2). One value is shared across every stage of a single
Pipeline. [src/config.rs:67]

### §10.4 `log_level`

Default `Info`. Not exposed via `config.toml`; set programmatically via
`LedgerConfig::log_level`. `LedgerConfig::temp` and `LedgerConfig::bench`
both lower this to `Critical` to keep test output clean.
[src/config.rs:46-47,54-56]

### §10.5 `seal_check_internal` (note: storage-side)

Interval at which the Seal stage polls for unsealed segments. Default
`1 s`. Not exposed via `config.toml`. Listed here for completeness
because it is a `LedgerConfig` field; the Seal stage and its semantics
are detailed in Stage 2. [src/config.rs:48-49,57-59]

### §10.6 `disable_seal` (note: storage-side)

When `true`, the Seal stage is not started. Used by `LedgerConfig::bench`
to remove disk I/O from the hot path during benchmarks. Default `false`.
Not exposed via `config.toml`. Listed here for completeness; semantics
are in Stage 2. [src/config.rs:50-51,124-125]

### §10.7 Hot-index sizes are derived

Circle sizes (§7.6) are computed by `LedgerConfig::index_circle1_size()`
and `index_circle2_size()` from `storage.transaction_count_per_segment`.
There is no separate ledger-side knob; changing the active window changes
both the hot indexes and the dedup window in lockstep. [src/config.rs:80-89]

---

## §11 Transaction ID space and recovery posture

### §11.1 `tx_id` properties

`tx_id` is a `u64`. It is monotonic, globally unique per submission,
strictly ordered by submission sequence, and never reused. A `tx_id` is
a position in the global log — there are no gaps, no reorderings, and
the ID itself is the source of all per-transaction identity in the
WAL, the indexes, and the link records. [src/sequencer.rs, docs/01-concepts.md:82-83]

### §11.2 Restart restores `sequencer_index`

On startup, after recovery determines `last_committed_tx_id` from the
WAL, `sequencer_index` is set to `last_committed_tx_id + 1`. The next
submitted operation therefore receives the lowest available unused ID,
preserving monotonicity across restarts. [src/recover.rs]

### §11.3 In-flight IDs are not re-used

Any ID that the Sequencer assigned but the WAL did not commit before a
crash is *not* re-used after restart. Such transactions were not
durable; they are simply lost. The next submitted operation gets
`last_committed_tx_id + 1`, leaving a (possibly empty) gap in the
*Sequencer's* historical assignment without producing any gap in the
committed log. The committed log, by §11.4, has no gaps.

### §11.4 Committed transactions are never lost

Every committed transaction is in the WAL, and the WAL is always
replayed on recovery. A committed transaction therefore survives any
crash — including a crash mid-`fdatasync` — provided the underlying
storage honours its durability contract. (How the WAL achieves this:
Stage 2.) [docs/03-architecture.md:255]
