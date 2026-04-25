# Internal

Single source of truth. Each `§` states one complete invariant or decision in
its own paragraph. Ambiguous behaviour is resolved by what this document says;
where an ADR exists for a decision it is cited as `[ADR-NNNN]`.

Stages:

1. **The Ledger** — `§1..§16` (this document, current).
2. **The Storage** — pending. Covers the on-disk file format (segment file
   layout, WAL record byte layout, sidecar files, snapshot file format, function
   binary storage, on-disk index file format, ctl tools).
3. **The Cluster** — pending. Covers the network-facing surface (gRPC, client,
   proto) and Raft replication.

Anything described here is the in-memory ledger: structures, threads, queues,
indexes, the sequence in which records flow through the pipeline. The on-disk
*shape* of those records and files is in Stage 2; the *mechanism* by which the
ledger reads and writes them is here.

---

## §1 Domain model

### §1.1 Accounts and the system account

An account is identified by a `u64`. Accounts are not created explicitly; they
come into existence the first time a transaction references them. The system
account is `SYSTEM_ACCOUNT_ID = 0` and serves as the source/sink for every
deposit and withdrawal — it is required by the zero-sum invariant (§4.11), not
a convention. The system account's balance is never tracked or queried.

### §1.2 Balances

A balance is an `i64`. Balances are *derived state*: the source of truth is the
ordered transaction log, and a balance is the result of applying every entry
that has touched the account. Arithmetic uses saturating add/sub so a malformed
operation cannot overflow the account silently. Default protection prevents
balances from going negative (§4.9), and only WASM functions may explicitly
bypass this by inspecting `get_balance` themselves.

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

### §1.5 Function operation parameters

`Function` parameters are a fixed `[i64; 8]`. Slots a caller does not need are
zero-padded. The arity is locked at the type level so the host ABI (§6.7) and
WAL/transaction representation never require variable-length parameter blocks.

### §1.6 WAL record format invariants

Every WAL record is exactly 40 bytes; the size is enforced by compile-time
`const _: () = assert!(size_of::<...>() == 40)` for every record type. The
first byte of every record is its `entry_type` discriminant, which makes the
WAL stream `[record][record][record]…` self-describing without a separate
length-prefix or framing layer. All record `#[repr(C)]` structs are laid out
without implicit padding so they satisfy `bytemuck::Pod` for zero-copy
serialization.

### §1.7 The six WAL record kinds

`WalEntryKind` enumerates the only six record types that may appear in the
log: `TxMetadata = 0`, `TxEntry = 1`, `SegmentHeader = 2`, `SegmentSealed = 3`,
`Link = 4`, `FunctionRegistered = 5`. New record types must extend this enum;
the discriminant is the first byte of the record (§1.6). `SegmentHeader` and
`SegmentSealed` are emitted by the WAL stage at segment boundaries (§7.6); their
on-disk byte layout is in Stage 2.

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
overflow with `ENTRY_LIMIT_EXCEEDED` (§1.12).

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

### §1.10 TxLink

`TxLink` records a relationship between two transactions. `link_kind` is
either `Duplicate(0)` (this transaction was rejected as a duplicate of
`to_tx_id` — see §5.5) or `Reversal(1)` (this transaction reverses
`to_tx_id`). Links are immutable WAL records and form the audit trail of
non-trivial transaction relationships. [ADR-0009]

### §1.11 FunctionRegistered

`FunctionRegistered` is the WAL record for a register/unregister event. It
carries `name: [u8; 32]` (null-padded ASCII snake_case, validated ≤ 32 bytes
at registration), `version: u16` (per-name monotonic, starting at 1, max
65535), and `crc32c: u32` of the WASM binary. A `crc32c == 0` value signals
*unregister* — the audit trail is preserved as a versioned event rather than
a deletion. The on-disk binary file is the secondary signal; if the two
disagree the WAL wins. `FunctionRegistered` carries no `tx_id`, and the
pipeline's commit/snapshot indexes do not advance on it (it is not a
financial transaction). [ADR-0014]

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
| `7` | `DUPLICATE` — `user_ref` matched within the active window (§5.5) |
| `8..=127` | reserved for future standard reasons |
| `128..=255` | user-defined; returned by WASM functions and passed through unchanged |

Anything in `128..=255` is the WASM author's contract with their callers; the
ledger never reinterprets it. [ADR-0001]

---

## §2 Pipeline structure

### §2.1 Stages and guarantees

The pipeline has four execution stages plus one independent maintenance stage.
A transaction moves through Sequencer → Transactor → WAL → Snapshotter, and
each step adds exactly one guarantee:

| Stage | Guarantee | Status |
|---|---|---|
| Sequencer (§3) | Monotonic ID, permanent global order | `PENDING` |
| Transactor (§4) | Executed, serializable, result known | `COMPUTED` |
| WAL (§7) | Durable on disk, crash-safe | `COMMITTED` |
| Snapshotter (§8) | Visible to readers, indexed (§9) | `ON_SNAPSHOT` |

Seal (§11) runs in parallel and is responsible for finalising closed segments,
producing on-disk indexes, and writing snapshot files for recovery.

### §2.2 Status is derived, not stored

A `TransactionStatus` is computed at query time from the pipeline's progress
indexes — never persisted as a record. A transaction is `Computed` iff
`compute_index ≥ tx_id`, `Committed` iff `commit_index ≥ tx_id`,
`OnSnapshot` iff `snapshot_index ≥ tx_id`. Failed transactions are recognised
through the rejection registry (§4.17). This avoids a per-transaction state
machine on disk and makes status a pure function of the pipeline.

### §2.3 Pipeline ownership and context slicing

`Pipeline` is a singleton `Arc<Pipeline>` and is the sole owner of every
inter-stage queue, every progress index, the shutdown flag, and the shared
wait strategy. Stages never own this state directly — each receives a typed
context wrapper (`SequencerContext`, `TransactorContext`, `WalContext`,
`SnapshotContext`, `SealContext`, `LedgerContext`) that exposes only the
slice that stage may legally read or publish. The contexts are thin Arc
clones; the wrapping is purely for type-level encapsulation.

### §2.4 Inter-stage queues

The three inter-stage channels are `crossbeam::ArrayQueue` instances —
lock-free, fixed-capacity, single-producer / single-consumer:

- `sequencer → transactor`: `TransactionInput`.
- `transactor → wal`: `WalEntry`.
- `wal → snapshot`: `SnapshotMessage`.

Capacity is fixed at construction from `queue_size` (§15.2). A full queue is
the only backpressure signal; producers spin/yield until space exists. There
is no other flow-control mechanism.

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
wait levels (§14) meaningful. Memory ordering is `Release` on publication,
`Acquire` on observation; `running` uses `Relaxed` because shutdown
propagation is eventual.

### §2.6 Index initialisation

`sequencer_index` initialises to `1` (not `0`); `last_sequenced_id` is
therefore `sequencer_index − 1`. All other indexes initialise to `0`. The
pre-increment avoids any collision with the sentinel value `0` and means a
freshly constructed pipeline that has assigned no IDs reports
`last_sequenced_id == 0`. On startup `sequencer_index` is restored to
`last_committed_tx_id + 1` (§16.2), preserving the same invariant.

### §2.7 Backpressure and shutdown

Backpressure is implicit and propagates through queue saturation alone: a
slow Snapshotter fills the wal→snapshot queue, which stalls the WAL, which
fills the transactor→wal queue, which stalls the Transactor, which fills the
sequencer→transactor queue, which stalls `submit()`. There is no rate
limiter, leaky bucket, or admission control. Shutdown is the inverse signal:
`Pipeline::shutdown()` clears `running`; every stage's idle loop checks the
flag and exits. The shutdown read is `Relaxed` because eventual visibility
is sufficient — the queues drain to a safe state regardless of the precise
moment a stage notices.

---

## §3 Sequencer

### §3.1 Placement on the caller's thread

The Sequencer has no dedicated thread. It runs synchronously inside
`Ledger::submit` on whichever thread the caller invokes from. Its work is
intentionally trivial: one atomic increment plus one queue push. Adding a
thread (and the cross-thread channel that would imply) would only add
latency.

### §3.2 ID assignment

ID assignment is a single `fetch_add(count, Acquire)` on `sequencer_index`.
`submit(operation)` reserves one ID and stamps the resulting `Transaction`.
`submit_batch(operations)` reserves a contiguous run of `len(operations)`
IDs in one atomic step; per-operation IDs inside the batch are
`start_tx_id + index`. Batches are atomic at the sequencer: their IDs are
guaranteed consecutive even under concurrent submitters.

### §3.3 Permanence of position

Once the Sequencer assigns an ID, the transaction's position in the global
order is permanent. No later stage can reorder, reassign, or skip an ID.
This is what allows downstream stages (Transactor, WAL, Snapshotter) to
publish progress as a single monotonically advancing index per stage —
there are no holes to fill or gaps to wait on.

### §3.4 Backpressure spin/yield policy

When the sequencer→transactor queue is full, the submitter spin-loops on
`crossbeam_queue::ArrayQueue::push`, calling `std::hint::spin_loop()` on
each retry and `std::thread::yield_now()` every 10 000 retries. There is no
timeout — `submit` returns only after the operation is queued. The choice
of *yield every 10 000* trades a small amount of pathological-case CPU for
the latency of being first in line when space appears.

---

## §4 Transactor — the single writer

### §4.1 Single-writer rule

The Transactor is one dedicated thread that processes one transaction at a
time, in strict ID order. This is the source of all correctness guarantees:
there are no races, no conflicts, no need for locks or conflict resolution.
Throughput does not come from parallelising execution — it comes from
sequencing, durability, and snapshotting running concurrently *around* the
Transactor. The single-writer ceiling is the upper bound on per-ledger
write throughput. [ADR-0001]

### §4.2 Thread-local state

`TransactorState` holds the balance vector, the per-step entries buffer,
the `position` marker, the current `tx_id`, and the current `fail_reason`.
It lives in `Rc<RefCell<...>>` and never crosses a thread boundary —
neither does anything that holds it (notably the `WasmRuntimeEngine`, which
is `!Send`). All host-call implementations borrow `RefCell` mutably, but
because the Transactor thread is the only thread that touches the state,
there is no contention and `RefCell`'s runtime check costs only a single
flag bit per borrow.

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
the Transactor and reallocating every read-side cache (§8.3), which is not
worth the complexity given how rarely a deployed ledger needs to grow its
ceiling. [ADR-0002]

### §4.5 Linearizability of the write path

The Transactor never reads stale balances. Because it is the sole writer
and operates on the in-memory cache (§4.3), every read it performs during
operation execution observes the cumulative effect of every earlier
transaction. The write path is therefore linearizable by construction;
the read path requires an explicit `submit_wait(snapshot)` (§14.4) for the
same guarantee.

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
is what advances the per-step cursor.

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
the built-in check.

### §4.10 Per-transaction CRC32C

After verify and any rollback are complete, the Transactor computes a
CRC32C over `TxMetadata` (with its own `crc32c` field zeroed) followed by
all `TxEntry` records of this transaction followed by all `TxLink` records,
and writes the result into `TxMetadata.crc32c`. The CRC therefore covers
exactly one transaction's full record stream and uses CRC32C
(Castagnoli) — hardware-accelerated on modern CPUs. Because the field
participates in the digest while being part of the record, it must be
zeroed during computation; consumers that want to verify the CRC must
zero it before recomputing.

### §4.11 Zero-sum invariant

Every transaction must net to zero: `sum(credits) == sum(debits)` across
all `TxEntry` records of that transaction. The Transactor verifies this
after operation execution and before any state is committed; a violation
raises `ZERO_SUM_VIOLATION` (§1.12) and the transaction is rolled back
(§4.12). The invariant is per-transaction — there is no cross-transaction
arithmetic. Because each individual transaction nets to zero, the sum of
all balances across all accounts is always zero — an emergent property,
not a configuration option.

### §4.12 Rollback

When verify fails or a WASM execution rejects, the Transactor reverses
every credit/debit produced in `entries[position..]` against the live
balance cache, truncates the buffer to keep only the leading `TxMetadata`,
sets `fail_reason`, and recomputes the CRC. The cost is O(entry_count) and
is acceptable because typical entry counts are small. The metadata is
preserved deliberately — see §4.13.

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
reader on the read-side balance cache (§8.3) will never observe a
partially-written balance for any single account — the value is always a
complete, valid `i64`.

### §4.15 Intermediate state is visible across accounts

Within a multi-step operation (a `Transfer`, or a `Function` issuing
several `credit`/`debit` calls), the Transactor applies each step in
sequence. A concurrent reader can observe some of these stores before
others have landed — for example, seeing a credit applied to one account
before the matching debit on another has landed. This is by design.
`last_tx_id` returned by `get_balance` is the signal that a transaction
is fully settled across all accounts: it is updated only after all
entries of a transaction have been applied (§8.5). Callers who need to
see a multi-account transaction atomically should wait for `last_tx_id`
to advance past their `tx_id` or use `submit_wait(snapshot)`.

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
transactions succeed.

---

## §5 Deduplication and the active window

### §5.1 Always on, no global toggle

Deduplication is always on. There is no configuration that disables it
system-wide. The only opt-out is per-transaction (§5.4).

### §5.2 Active window and flip-flop cache

The active window size equals `transaction_count_per_segment` — the same
number that drives WAL segment rotation (§7.6). The dedup cache is two
`FxHashMap<user_ref, tx_id>` maps, `active` and `previous`. When `tx_id`
crosses the segment boundary, the maps flip: `active` becomes `previous`,
the previous map's allocation is reused as the new (cleared) `active`.
Effective dedup coverage is therefore `N..2N` transactions — at least one
full window, at most two.

### §5.3 Determinism: count-based, not time-based

The flip is driven by `tx_id`, not by wall-clock time. The same N
transactions always provide the same protection, whether they arrive in
one second or one hour. Idempotency guarantees do not degrade under load
spikes and do not silently widen during quiet periods.

### §5.4 `user_ref = 0` opt-out

A `user_ref` value of `0` opts the *individual* transaction out of the
dedup check. The cache itself remains active and continues to record
every `user_ref ≠ 0`. There is no path that disables the cache globally
short of recompiling.

### §5.5 Hit handling

On a hit, the Transactor emits `TxMetadata` (with `fail_reason = DUPLICATE`)
followed by `TxLink { kind: Duplicate, to_tx_id: original_tx_id }`. The
caller therefore always recovers the *original* `tx_id` for an idempotent
retry, not just a "rejected" status. [ADR-0009]

### §5.6 Recovery seeding

On startup, `DedupCache::recover_entry(user_ref, tx_id, last_tx_id)` is
called for each WAL transaction inside the active window (§12.3). Entries
outside `2 × window` are dropped; entries within the window are placed
into the correct half (`active` vs `previous`) based on their relative
position. Post-recovery dedup behaviour is therefore identical to
pre-crash — a duplicate submitted before and after a restart is rejected
the same way.

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
coordination.

### §6.2 Pipeline-identical flow

A `Function` operation flows through Sequencer → Transactor → WAL →
Snapshotter exactly like a `Deposit`. WAL segmentation, sealing,
snapshotting, and replay treat function-produced entries identically to
native ones. There is no second persistence path, no second recovery
path, no second wait-level mechanism.

### §6.3 One Engine, one Linker per ledger

There is one `wasmtime::Engine` and one `Linker<WasmStoreData>` per
ledger, both held behind `Arc<WasmRuntime>` and cheap to clone. The
Linker carries the host imports (§6.7); the Engine compiles and caches
modules. Constructing either is expensive and is therefore done exactly
once.

### §6.4 Shared registry

The shared registry is a small `RwLock<HashMap<name, Registered{version,
crc32c, Arc<Module>}>>`. It is touched only on register, unregister, and
cache miss / revalidation — never on every transaction. The hot path
(§6.5) avoids the lock entirely.

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
not evict the cached entry for `bar`.

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
(§4.16), not the call. [ADR-0014]

### §6.8 Determinism rule

The host ABI is intentionally narrow: no clocks, no random, no I/O, no
threads, no atomics. Every legal function is therefore a pure mapping
from `(params, observed balances) → (status, credits, debits)`. This is
not a stylistic choice — it is what makes the WAL replayable on a
follower without re-running the WASM code (§6.9). Adding any
non-deterministic host call would make replication unsafe.

### §6.9 Replication implication

Because of §6.8, a future Raft follower can apply the WAL records
produced by a function on the leader directly — without instantiating
the WASM module, without re-running `execute`, and without observing any
of the function's internal state. The leader is the only node that
needs to compile and run the binary; followers see only the
`TxEntry`/`TxLink`/`TxMetadata` stream the leader produced. This is what
enables the future cluster (Stage 3).

### §6.10 Atomicity of function execution

Host-call effects accumulate in an isolated host-side execution context
inside `TransactorState`. Nothing is applied to the live balance cache
until *all* of the following hold: `execute` returned `0`, the captured
credits and debits balance (§4.11), and no wasmtime-layer error
occurred. If any of those fail, the entire batch of host calls is
discarded — exactly as a built-in operation rollback (§4.12). The WAL
queue therefore never sees a partial function execution.

### §6.11 Wasmtime-layer failure mapping

A wasmtime layer failure — link error, instantiation error, or runtime
trap — is mapped to `INVALID_OPERATION = 5` (§1.12) and rolled back. The
mapping is single-valued by design: from outside the runtime, *any*
infrastructure-level WASM failure looks the same. The function author's
own status codes (§6.12) live in a disjoint range.

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

### §6.15 Names and versions

Function names are validated at registration: ASCII snake_case, ≤ 32
bytes, null-padded to 32 in the WAL record (§1.11). Each register or
override bumps the per-name `version` by one, starting at `1`, with a
ceiling of `65535`. The `(name, version, crc32c)` triple uniquely
identifies the executing binary in every transaction. [ADR-0014]

### §6.16 Registration is durable

`RegisterFunction` only returns after all four of the following have
happened: the binary has been validated, written atomically to disk, a
`FunctionRegistered` WAL record has been committed, and the compiled
handler has been installed in the live `WasmRuntime`. A subsequent
`Operation::Function` is therefore guaranteed to see the new version.
After a crash, recovery rebuilds the registry from a paired function
snapshot plus the `FunctionRegistered` records that follow it (§12.4) —
the live runtime always matches what the WAL says it should be.

### §6.17 Unregistration

Unregistration emits a `FunctionRegistered` record with `crc32c = 0`
(§1.11) and writes a 0-byte file under the next version on disk. The
binary history is preserved as a sequence of versions; the function
registry's *current* state is the last record seen. The WAL is
authoritative; the on-disk file is a secondary signal. [ADR-0014]

### §6.18 Snapshotter applies registry events

`FunctionRegistered` records flow through the WAL like any other record
(§7.4). The Snapshotter (§8.7) is the stage that actually loads or
unloads the compiled handler into the shared `WasmRuntime` — calling
`storage.read_function(name, version)` to fetch the binary on a load,
`unload_function(name)` on an unregister. This is what keeps the live
runtime synchronised with the WAL on every node, leader or follower.

---

## §7 WAL stage — durability orchestration

### §7.1 Two threads, one role each

The WAL stage runs as two cooperating threads. The **WAL Writer**
(`wal_write`) owns the active segment and all disk I/O for record writes;
the **WAL Committer** (`wal_commit`) owns `fdatasync` calls only. Splitting
the two means the Writer never blocks behind a `fdatasync` — it continues
appending records to the next batch while the Committer is mid-sync. The
hot path is dominated by `fdatasync` latency (~hundreds of µs on NVMe);
isolating it from record writes is what lets the WAL keep up with the
Transactor. [ADR-0011]

### §7.2 Atomics and shared state

The two threads coordinate through three shared values:

- `last_written_tx_id: Arc<AtomicU64>` — the highest `tx_id` whose record
  has been written to the active segment file (Writer publishes with
  Release; Committer reads with Acquire).
- `last_committed_tx_id: Arc<AtomicU64>` — the highest `tx_id` whose
  record has been `fdatasync`'d (Committer publishes with Release; Writer
  reads with Acquire). This is what `commit_index` exposes to the rest
  of the pipeline (§2.5) and what the `wal` wait level gates on (§14.1).
- `active_segment_sync: Arc<ArcSwap<Option<Syncer>>>` — a hot-swappable
  handle to the active segment's syncer. The Writer publishes a fresh
  `Syncer` on every rotation (§7.6); the Committer picks up the new
  handle on its next iteration.

There is no other coordination — no condvars, no channels.

### §7.3 Writer buffer

The Writer owns a `VecDeque<WalEntry>` sized to `input_capacity * 16`. It
serves two purposes: it is the read-ahead window for entries that have
been written to disk but not yet forwarded to the Snapshotter, and it
gives the Writer a place to look ahead by an entire transaction without
blocking the inbound queue. The buffer is never resized after
construction; if it would overflow, the Writer simply slows its
intake — the inbound queue then back-pressures the Transactor (§2.7).

### §7.4 Writer per-iteration loop

Each iteration the Writer does the following, in order:

1. Pop up to `min(inbound.len(), buffer_remaining_capacity)` entries from
   the transactor→wal queue. Each popped entry is appended to the active
   segment's pending write buffer (`append_pending_entry`) and pushed to
   the back of the local `VecDeque`. As entries are popped, the
   `pending_records` counter (§7.5) is updated.
2. If any entries were popped this iteration, flush the segment's pending
   write buffer (`write_pending_entries`) and publish
   `last_written_tx_id` with Release.
3. If the transaction-count threshold is met, rotate the segment (§7.6).
4. Drain the local `VecDeque` to the Snapshotter — but only entries whose
   `tx_id ≤ last_committed_tx_id` (Acquire). Entries whose `tx_id` is
   still pending an `fdatasync` are pushed back to the front of the
   buffer and the drain stops; they will be retried on the next
   iteration after the Committer advances. As each entry is forwarded,
   the pipeline's `commit_index` (§2.5) is advanced via
   `ctx.set_processed_index(tx_id)`.

### §7.5 Forwarding rule and `pending_records`

Entries flow from the WAL to the Snapshotter only after they are durable.
This is the *durability rule*: the Snapshotter — and therefore
`get_balance`, queries, and the snapshot wait level — never sees a
transaction that is not yet on disk. The Writer enforces this in step 4
of §7.4 by gating the drain on `last_committed_tx_id`. The
`pending_records: u8` counter on the Writer tracks how many records of
the current transaction (from `TxMetadata.entry_count + link_count`) are
still expected from the inbound queue; it is decremented as `TxEntry`
and `TxLink` records arrive and is what allows the Writer to detect end
of transaction without parsing semantics. The Writer keeps consuming the
inbound queue while `pending_records > 0` even if there is no more
buffer space — a partial transaction would otherwise leak into the wrong
segment.

### §7.6 Segment rotation

The Writer rotates segments on a transaction-count threshold:
when `last_received_tx_id - segment_start_tx_id ≥
transaction_count_per_segment`. This is the same boundary that drives
dedup window flips (§5.2) and hot-index sizing (§9.6). Rotation appends
a `SegmentSealed` record (`segment_id`, `last_tx_id`, `record_count`),
performs a *synchronous* `fdatasync` (`commit_sync` — the Writer cannot
defer this one to the Committer because the next step closes the file),
closes the segment, opens the next one, writes a fresh `SegmentHeader`
record, and publishes the new `Syncer` into `active_segment_sync` so
the Committer picks it up. Rotation is a transient cost: the rest of
the pipeline is paused only for the synchronous sync and the
`open + header` of the next file. [ADR-0013]

### §7.7 Committer loop

The Committer runs a tight loop driven by the shared `WaitStrategy`
(§13.2). On each iteration:

1. If `last_written_tx_id ≤ last_committed_tx_id`, there is nothing to
   sync; back off via the wait strategy and continue.
2. Load `active_segment_sync` via `ArcSwap::load`. If the syncer's id
   has changed since the previous iteration (i.e. the Writer rotated),
   clone it into the local handle.
3. Sample `last_written_tx_id`, call `syncer.sync()` (which is the
   actual `fdatasync`), publish that sample to `last_committed_tx_id`
   with Release.

The Committer never parses entries, never touches the buffer, never
inspects the queue. Its single responsibility is moving
`last_committed_tx_id` forward as fast as the storage allows.

### §7.8 Backpressure on the outbound queue

The Writer's drain to the Snapshotter (`push_outbound`) yields when the
wal→snapshot queue is full and re-checks `running` on each yield so a
shutdown is observed promptly. A persistently slow Snapshotter therefore
fills the wal→snapshot queue, which in turn slows the drain, which fills
the local `VecDeque`, which throttles intake from the inbound queue,
which back-pressures the Transactor (§2.7). The same mechanism that
gives the WAL its throughput is the one that protects it from a slow
reader.

### §7.9 Failure handling

`syncer.sync()` and `write_all` are `expect`'d — a sync or write
failure is a hard panic. There is no retry loop on durability failures;
returning an `Err` to a submitter that has already been told its
transaction was committed would be a worse violation of the contract
than a process exit. Recovery (§12) is responsible for putting the WAL
back into a consistent state on restart.

---

## §8 Snapshotter — apply path

### §8.1 Apply, don't re-execute

The Snapshotter applies committed entries to the read-side state. It does
no business logic and never re-executes operations. Because every
`TxEntry` carries `computed_balance` (§1.9), the Snapshotter's job is a
sequence of `store(account_id, computed_balance)` operations plus index
maintenance (§9). This is the entries-based execution model and the reason
recovery and replication are cheap. [ADR-0001]

### §8.2 Why a separate stage

The Transactor's balance cache (§4.3) is *write-side* truth, private and
optimised for writes. The Snapshotter maintains *read-side* truth and is
optimised for concurrent readers. Separating them means readers never
block writers, and the Transactor never needs to coordinate with query
threads.

### §8.3 Read-side balance cache

The read-side cache is `Arc<Vec<AtomicI64>>`, sized to `max_accounts`. The
Snapshotter publishes balance updates with `Release`; readers
(`get_balance`) load with `Acquire`. Multiple concurrent readers can call
`get_balance` without locks. The vector is pre-allocated at startup and
never resized. [ADR-0002]

### §8.4 SnapshotMessage and the single FIFO

The Snapshotter consumes a single SPSC FIFO of `SnapshotMessage` carrying
two variants interleaved:

- `Entry(WalEntry)` — a record forwarded by the WAL Writer (§7.4).
- `Query(QueryRequest)` — a query enqueued by a caller (§10).

A single FIFO for both is what gives read-your-own-writes for free: a
query enqueued after a write is enqueued *after* the write's entries
and is therefore guaranteed to see them applied. There is no second
queue, no priority lane, no out-of-band query path.

### §8.5 Apply algorithm and `last_processed_tx_id`

Per `Entry(WalEntry)`, the Snapshotter dispatches by record kind:

- `Metadata(m)` — call `indexer.insert_tx(m.tx_id, m.entry_count)`,
  set `pending_records = m.entry_count + m.link_count`,
  set `last_processed_tx_id = m.tx_id`.
- `Entry(e)` — call
  `indexer.insert_entry(tx_id, account_id, amount, kind, computed_balance)`
  to update circle2 and the per-account chain (§9.4); store
  `e.computed_balance` into `balances[e.account_id]` with Release;
  decrement `pending_records`.
- `Link(l)` — call
  `indexer.insert_link(last_processed_tx_id, l.kind, l.to_tx_id)`;
  decrement `pending_records`.
- `SegmentHeader` / `SegmentSealed` — ignored by the Snapshotter.
- `FunctionRegistered(f)` — handled per §8.7.

Once `pending_records == 0` for the current transaction, the
Snapshotter publishes `last_processed_tx_id` to `snapshot_index` via
`ctx.set_processed_index(...)`. This is the only point at which the
pipeline's read-side index advances — readers therefore never observe a
partially-applied transaction.

### §8.6 `last_tx_id` as the freshness signal

`get_balance` returns the current balance plus `last_tx_id` — the
highest `tx_id` whose every entry has been applied to the read-side
cache. `last_tx_id` is the tool callers use to reason about freshness
without blocking, and it is the signal that confirms multi-account
transactions are fully settled (§4.15). For a linearizable read, the
caller waits for `last_tx_id` to pass their `tx_id`, or uses
`submit_wait(snapshot)` (§14.4).

### §8.7 FunctionRegistered handling

`FunctionRegistered` records do not participate in `pending_records` and
do not advance `snapshot_index`. On a register record, the Snapshotter
calls `storage.read_function(name, version)` to read the binary off
disk, then `wasm_runtime.load_function(name, &binary, version, crc32c)`
to install it in the shared registry (§6.4). On an unregister record
(`crc32c == 0`, §1.11), the Snapshotter calls
`wasm_runtime.unload_function(name)`. Errors at either step are logged;
they do not stop the apply loop. The Snapshotter is therefore the
*only* stage that mutates the live runtime registry once the system is
running — the Transactor reads through the per-Transactor caller cache
(§6.5).

### §8.8 Snapshotter is the bottleneck for queries

The Snapshotter owns the indexer exclusively; queries (§10) execute
inline on the Snapshotter thread. Concurrent queries serialise on this
thread. The work per query is a couple of array lookups and at most a
chain walk bounded by `limit`, so the bottleneck is generous, but it
exists — a workload that is dominated by query throughput should
batch queries or accept that queries compete with apply for the same
thread.

---

## §9 Hot indexes — `TransactionIndexer`

### §9.1 Three buffers, all pre-allocated

The hot transaction index is a single struct `TransactionIndexer` owning
three pre-allocated buffers, sized at construction and never resized:

- `circle1: Vec<TxSlot>` — per-`tx_id` entry-list pointer.
- `circle2: Vec<IndexedTxEntry>` — entry storage with per-account chain
  links.
- `account_heads: Vec<(u64, u32)>` — per-account latest-entry pointer.

A separate `links: HashMap<u64, Vec<IndexedTxLink>>` holds link records
keyed by `tx_id`. Links are sparse — most transactions have none — so a
`HashMap` is an acceptable concession to non-pre-allocated storage. The
three array sizes must all be powers of two; this is asserted at
construction. [ADR-0008]

### §9.2 Why power-of-two

All three lookups reduce `id & mask` to find a slot, where
`mask = size - 1`. The power-of-two requirement makes the lookup a
single bitwise AND with no division. The cost is at most ~2× memory in
the pathological case where the desired size is just over a power of
two; the alternative (modulo) costs an integer division on every query,
which is far more expensive at ledger throughput.

### §9.3 `circle1` — direct-mapped tx slot table

`TxSlot` is 16 bytes: `(tx_id: u64, offset: u32, entry_count: u8)`. The
slot index is `(tx_id as usize) & circle1_mask`. A slot with
`tx_id == 0` is empty. A slot whose stored `tx_id` does not match the
queried `tx_id` is *evicted* — overwritten by a colliding tx that
mapped to the same slot. Eviction is silent: the slot simply now belongs
to a newer transaction. There is no eviction queue, no cleanup pass.
Lookups detect eviction by comparing the slot's `tx_id` to the queried
one and falling back to disk on mismatch (§10.2).

### §9.4 `circle2` — entry storage and per-account chains

`IndexedTxEntry` is 48 bytes: `tx_id`, `account_id`, `amount`, `kind`,
`computed_balance`, plus a `prev_link: u32`. `circle2` is written
sequentially via a `write_head2` counter that advances monotonically and
wraps via `& circle2_mask` on each access. On `insert_entry`, the
indexer reads `account_heads[account_id & mask]`; if the stored
account_id matches, the entry's `prev_link` is set to that head's
`circle2_idx + 1` (1-based: `0` is the sentinel meaning "no previous"),
otherwise `prev_link = 0`. Then the new entry is written at
`circle2[write_head2 & mask]`, `account_heads` is updated to point at
this slot, and `write_head2` is advanced. Account history walks
(§10.3) follow `prev_link − 1` to step backwards in time until either
the chain ends (`prev_link == 0`), an evicted slot is detected
(`account_id` mismatch), the transaction predates `from_tx_id`, or
`limit` entries have been collected.

### §9.5 `account_heads` — direct-mapped account head table

`account_heads[i] = (account_id, circle2_idx)`. The slot index is
`(account_id as usize) & account_heads_mask`. The size is
`next_power_of_two(max_accounts)`. The slot stores the *account_id* it
belongs to so a colliding lookup can detect eviction the same way
`circle1` does — a mismatch on `account_id` means the head was
overwritten, the chain head is gone, and the caller must fall back to
disk for any history older than the current chain. The sizing means in
practice every account has its own slot; collisions only occur when the
account-id space is sparser than `next_power_of_two(max_accounts)`.

### §9.6 Sizing rule

Sizes are derived from `transaction_count_per_segment`:

- `circle1_size = next_power_of_two(transaction_count_per_segment)` —
  one slot per transaction in the active window.
- `circle2_size = next_power_of_two(2 × transaction_count_per_segment)` —
  enough entry slots for the active window assuming an average of two
  entries per transaction (transfers).
- `account_heads_size = next_power_of_two(max_accounts)`.

Changing the active window changes the hot indexes and the dedup window
in lockstep; there are no separate knobs. [ADR-0013]

### §9.7 Recovery seeding

The Snapshotter exposes recovery hooks (`recover_index_tx_metadata`,
`recover_index_tx_entry`, `recover_index_tx_link`) so the recovery
sequence (§12) can replay WAL records from the last snapshot through
the active segment's tail directly into the indexer. By the time the
Snapshotter thread starts, the indexer is already populated for
everything that lives in the active window.

---

## §10 Query path — hot/cold routing

### §10.1 Query types

Two query types reach the Snapshotter through the FIFO (§8.4):

- `GetTransaction { tx_id }` — return the entries (and links) of one
  transaction.
- `GetAccountHistory { account_id, from_tx_id, limit }` — return up to
  `limit` entries that touched `account_id`, newest first, stopping at
  `from_tx_id` (exclusive lower bound; `0` disables the lower bound).

A third query type — `get_balance(account_id)` — is *not* routed
through the Snapshotter. It is a direct atomic load against the
read-side cache (§8.3) and never blocks.

### §10.2 `GetTransaction` resolution

The Snapshotter dispatches `GetTransaction(tx_id)` to
`indexer.get_transaction(tx_id)`, which: looks up
`circle1[tx_id & mask]`; returns `None` on empty (`tx_id == 0`) or
mismatch (eviction); otherwise reads `entry_count` consecutive slots
starting at `circle2[offset]`, comparing each slot's `tx_id` to the
queried `tx_id` — a mismatch means the circle2 slot was overwritten
between the metadata insert and the query, and the indexer returns
`None`. Links are fetched separately from the `links` map. The
`QueryResponse::Transaction(...)` carries `Option<TransactionResult>`:
`Some` on a hit, `None` on a miss. A `None` is the signal to the
caller that this transaction is no longer in the hot tier and must be
read from disk via the on-disk transaction index (Stage 2).

### §10.3 `GetAccountHistory` resolution

`GetAccountHistory` reads `account_heads[account_id & mask]`; if the
stored account_id does not match, returns an empty vec (account head
evicted). Otherwise walks `prev_link` backwards through `circle2`,
stopping on `account_id` mismatch (chain broken by eviction),
`tx_id < from_tx_id` (past the requested window), `limit` reached, or
`prev_link == 0` (chain head). Results are returned newest-first. An
empty vec from a query that should have had results is the
caller's signal to fall back to the on-disk account index (Stage 2).

### §10.4 Hot tier vs cold tier

The hot tier is everything currently in the indexer; the cold tier is
everything sealed to disk. The boundary is not a function of `tx_id`
absolute value but of how recently each `tx_id`'s slot has been
overwritten — a low-traffic account's chain can survive deep into
history, while a hot account's chain may be evicted within one segment.
Cold-tier reads are sealed-segment lookups (the on-disk transaction
index and the on-disk account index built by Seal — §11.4). The
mechanism by which the caller routes a hot miss to the cold tier is
the same for both query types: the hot result returns `None` / empty,
and the caller reissues against the cold-tier API.

### §10.5 Synchronous response via callback

`QueryRequest` carries a `respond: Box<dyn FnOnce(QueryResponse) +
Send>` callback. The convention is: caller creates a per-request
oneshot channel (`mpsc::sync_channel(1)`), wraps `tx.send` in a
closure, enqueues the request, and blocks on `rx.recv()`. The
Snapshotter computes the response inline (§8.8) and invokes the
callback before continuing the apply loop. There is no separate response
channel, no shared state for partial results, no async runtime.

### §10.6 Status and balance queries

Two non-FIFO query paths exist:

- `get_balance(account_id)` — a direct `Acquire` load on
  `balances[account_id]`. Does not block, does not touch the
  Snapshotter, returns whatever balance the read-side cache currently
  holds along with `last_tx_id` (§8.6).
- `get_transaction_status(tx_id)` — derived (§2.2) from the pipeline
  indexes (`compute_index`, `commit_index`, `snapshot_index`) plus the
  rejection registry (§4.17). Does not block.

Neither of these traverses the Snapshotter FIFO; both serve very high
read volumes without contention.

---

## §11 Seal stage — segment finalisation and snapshots

### §11.1 What sealing means

Sealing is the act of making a *closed* segment safe to recover from.
The WAL Writer (§7.6) closes a segment when the transaction-count
threshold is reached; the closed file on disk has a `SegmentSealed`
record at its tail but no integrity-check sidecars and no built indexes.
Sealing turns this into a fully recoverable artifact: it computes a
file-level CRC, writes sidecar files, builds the on-disk transaction
and account indexes for cold-tier queries (§10.4), and — at the
configured frequency — emits a balance snapshot and a function snapshot.
The exact file format of all of these is in Stage 2; the *mechanism* is
described here.

### §11.2 Independent thread, polling loop

Seal runs as one dedicated thread (`seal`) on a polling loop. There is
no queue from the WAL or any other stage to Seal — the WAL closes
segments by renaming files on disk, and Seal discovers work by listing
the storage directory each iteration. The poll interval is
`seal_check_internal` (§15.6, default 1 s). Between polls the thread
sleeps; on shutdown the loop checks `running` and exits. Seal is not on
the hot path of any submission; missing the threshold by a few seconds
is harmless. The *cost* of the poll is one directory listing per
interval.

### §11.3 Per-segment seal procedure

For each segment whose status is not `SEALED`, Seal performs:

1. `segment.load()` — open the closed segment.
2. `segment.seal()` — compute the file-level CRC, write the `.crc` and
   `.seal` sidecar files, mark the segment SEALED. Disk format details:
   Stage 2.
3. `segment.build_indexes()` — build the on-disk transaction index and
   account index files for this segment. These are what cold-tier
   queries (§10.4) walk when the hot index misses.
4. Replay the segment's WAL records into the Seal runner's *own*
   balance vector and *own* function map (§11.4) so the next snapshot
   has accurate state.
5. If `segment.id() % snapshot_frequency == 0`, write a balance
   snapshot and a function snapshot for this segment (§11.5).
6. Publish the sealed segment id to the pipeline via
   `ctx.set_processed_index(id)` (advances `seal_index`, §2.5).

A failure in step 2 or 3 is logged; the segment is left unsealed and
retried on the next poll.

### §11.4 Why Seal owns its own balance vector and function map

The Seal runner holds two pieces of state separate from the rest of
the pipeline:

- `balances: Vec<i64>` (sized to `max_accounts`) — updated as each
  `TxEntry` from the segment passes by, using
  `e.computed_balance` directly. This vector is the source for the
  balance snapshot (§11.5) and is not the read-side cache (§8.3).
- `functions: FxHashMap<String, (version, crc32c)>` — updated on each
  `FunctionRegistered` record. Unregister records (`crc32c == 0`) are
  kept in the map so the snapshot preserves the audit trail.

Why a separate copy: Seal must snapshot the *committed-and-sealed*
state, which lags `commit_index` and certainly lags
`compute_index`. The Snapshotter's read-side cache is the
*latest-committed* state and can race ahead. A snapshot taken from
either of those would either include uncommitted state or be racy.
Reading off the segment, on a thread that does not care about the
freshest tx, gives Seal a consistent point-in-time view at zero
synchronisation cost.

### §11.5 `snapshot_frequency` rule

When `snapshot_frequency > 0` and `segment_id % snapshot_frequency == 0`,
Seal emits two snapshot files for this segment:

- A balance snapshot — Seal filters `balances` to non-zero entries,
  sorts by `account_id`, and writes them via `segment.save_snapshot`.
- A function snapshot — Seal serialises `functions` (sorted by name for
  determinism) via `storage.save_function_snapshot`. The function
  snapshot is written even when the registry is empty so recovery can
  always jump straight to the latest snapshot boundary instead of
  replaying WAL from segment 1. [ADR-0014]

When `snapshot_frequency == 0`, no snapshots are emitted; the system
will replay from segment 1 on every recovery. The default is
`snapshot_frequency = 4`.

### §11.6 Pre-seal during recovery

Recovery (§12) may discover a closed-but-unsealed segment from a crash
between WAL rotation and the next Seal poll. Rather than start the
Seal thread early, Recover calls `Seal::recover_pre_seal(segment)`
synchronously, which performs the same `process_seal` as the running
Seal thread and publishes the resulting segment id. After recovery
finishes, the Seal thread starts and resumes its normal poll. This
keeps the Seal thread's lifecycle simple: one thread, one loop, one
shutdown — no special "first iteration" mode.

### §11.7 Seal does not touch the live indexes

Seal does not write to the Snapshotter's indexer (§9), the read-side
balance cache (§8.3), or any of the dedup state (§5). Its outputs are
on-disk only: sidecar files, on-disk indexes, snapshot files. The
in-memory state is owned by other stages and updated by them; Seal's
job is to make the disk side recoverable. The one in-memory effect Seal
has is publishing `seal_index` (§2.5), which `ctl` tools and recovery
read.

### §11.8 `disable_seal` for benchmarks

`disable_seal = true` (§15.7) skips starting the Seal thread entirely.
This removes all sealing I/O — sidecars, indexes, snapshots — from the
benchmarked path. It is *only* a benchmarking knob: a ledger run with
`disable_seal = true` cannot recover from a crash beyond the active
segment, and cold-tier queries (§10.4) will return empty for any
sealed segment. Production must always have it `false`.

---

## §12 Recovery — in-memory orchestration

### §12.1 Recovery is synchronous and runs before any stage starts

`Ledger::start` calls into `Recover` before spawning Sequencer,
Transactor, WAL, Snapshotter, or Seal threads. Recovery is therefore
single-threaded and observes a quiescent disk; nothing else is reading
or writing the data directory. Once recovery returns, the pipeline is
in a state that is observationally identical to the state that existed
at `last_committed_tx_id` immediately before the previous shutdown.

### §12.2 Pre-seal of unsealed segments

The first thing Recover does is scan for closed-but-unsealed segments
(§11.6). For each one it calls `Seal::recover_pre_seal`, which seals
the segment and publishes it to `seal_index`. After this step every
segment on disk except the active one is in `SEALED` state.

### §12.3 Snapshot load and WAL replay

Recovery proceeds in this fixed order:

1. **Find the latest balance snapshot** — its `segment_id` defines the
   replay starting point. The snapshot's records are loaded into the
   Transactor's balance vector (§4.3), the Seal runner's balance vector
   (§11.4), and the Snapshotter's read-side cache (§8.3) via
   `recover_balances`.
2. **Find the latest function snapshot** — load each
   `(name, version, crc32c)` triple. The Seal runner's `functions` map
   is seeded via `Seal::recover_function`. The WASM runtime is loaded by
   reading each binary off disk and calling `wasm_runtime.load_function`.
3. **Replay sealed segments after the snapshot** — for each segment
   strictly newer than the snapshot's `segment_id`, walk every record:
   - `TxEntry`: update Transactor balance, Seal balance, Snapshotter
     balance, and the hot indexer (`recover_index_tx_entry`); update
     dedup if the matching `TxMetadata`'s `user_ref ≠ 0`.
   - `TxMetadata`: update `last_tx_id`, hot index
     (`recover_index_tx_metadata`), dedup (`recover_entry`).
   - `TxLink`: update hot index (`recover_index_tx_link`).
   - `FunctionRegistered`: load/unload through `wasm_runtime`; update
     Seal's `functions` map.
4. **Replay the active segment** — same as step 3 but against the
   `wal.bin` file. Per-record CRC validation governs how far the
   active segment can be trusted; details (sidecar, broken-tail
   tolerance) are in Stage 2.
5. **Restore `sequencer_index`** to `last_tx_id + 1` (§16.2).

### §12.4 Order requirement

The order in §12.3 is not negotiable. Loading the balance snapshot
*before* replaying segments ensures the replay only re-applies entries
that postdate the snapshot. Loading the function snapshot *before*
replaying `FunctionRegistered` records ensures the runtime starts from
the snapshotted set and only diffs forward. Pre-sealing (§12.2)
*before* the snapshot search ensures the latest snapshot is always
available — without it, a snapshot trigger that fired on a segment
that was not yet sealed at crash time would be missed.

### §12.5 Recovery seeds, not replays, the dedup cache

Dedup recovery is not a replay of every WAL record into the cache: it
is a windowed seed (§5.6). Recover walks the active window's worth of
WAL records (the last `2 × transaction_count_per_segment`) and inserts
each `(user_ref, tx_id)` into the appropriate half of the flip-flop
cache. Older `user_ref`s are not re-inserted; they are outside the
dedup window and would be evicted on the next flip anyway.

### §12.6 Failed transactions are replayed too

A `TxMetadata` with `fail_reason ≠ 0` represents a transaction that
*failed* but was journaled (§4.13). Recovery still walks it and updates
the indexer and dedup, but the balance vectors are not modified
(failed transactions emit no `TxEntry` records that move balance, by
construction of the rollback in §4.12). After recovery the rejected
transaction's `tx_id` continues to resolve through the rejection
registry (§4.17), populated in the same pass.

### §12.7 No partial recovery

Recovery either succeeds completely or fails the process. There is no
partial-recovery mode where some segments are replayed and others
skipped. A corrupt segment in the middle of the chain is fatal at
startup; a corrupt tail of the active segment is the one tolerated
case (Stage 2). The principle: a started ledger is a ledger that is in
a known-good state.

---

## §13 Inter-stage queues, backpressure, wait strategies

### §13.1 Lock-free SPSC queues

Every inter-stage queue is `crossbeam::ArrayQueue` — single-producer,
single-consumer, lock-free, fixed-capacity. Each stage owns its data
completely; no shared mutable state crosses stage boundaries. The fixed
capacity is sized at construction from `queue_size` (§15.2).

### §13.2 Wait strategies

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
there is no per-stage override.

### §13.3 Backpressure as queue saturation

There is no rate limiter, leaky bucket, or admission control. Slowness
in any stage propagates upstream as queue fullness, eventually stalling
`submit()` (§2.7). This is intentional: the queues are the only place
where stages observe each other's progress, and turning them into the
backpressure mechanism keeps the design free of explicit coordination.

---

## §14 Wait levels

### §14.1 Four levels

The caller picks one of four wait levels per submission:

- `none` — return `tx_id` immediately. Maximum throughput.
- `transactor` — wait until `compute_index ≥ tx_id` (`COMPUTED`). The
  caller knows whether the transaction succeeded or was rejected.
- `wal` — wait until `commit_index ≥ tx_id` (`COMMITTED`). The
  transaction is durable on disk and survives a crash.
- `snapshot` — wait until `snapshot_index ≥ tx_id` (`ON_SNAPSHOT`). The
  effects are visible to readers via `get_balance`.

[ADR-0010]

### §14.2 Implementation

`wait_for_transaction_level(tx_id, level)` polls the relevant pipeline
index against the requested threshold using the shared `WaitStrategy`
(§13.2). The polling read uses `Acquire` so that a successful wait
synchronises with every store that produced the index advance.

### §14.3 Per-call dial

Wait level is per-submission; there is no global default. The caller
chooses the trade-off between latency and consistency on every call.
This is the primary performance dial of the system: a high-throughput
batch ingester might use `none`; a settlement step that must read its
own balance immediately uses `snapshot`.

### §14.4 Linearizable read recipe

The documented way to obtain a linearizable read is
`submit_wait(snapshot)` followed by `get_balance(account_id)`. After the
wait returns, `snapshot_index` is past the submitted `tx_id`, so
`get_balance` reflects the submitted transaction and everything before
it. This is the only blocking primitive a caller needs for linearizable
reads.

---

## §15 Configuration — ledger-side knobs

### §15.1 `max_accounts`

Pre-allocated capacity of every balance vector and read-side atomic
vector. Default `1_000_000`. Fixed at startup; cannot grow at runtime
(§4.4). Accounts referencing IDs ≥ this value are rejected with
`ACCOUNT_LIMIT_EXCEEDED`. The 1M default chosen because 1M × 8 bytes =
8 MB per balance vector, which fits comfortably in L2/L3 even with
read-side and seal-side duplicates.

### §15.2 `queue_size`

Capacity of every inter-stage `ArrayQueue`. Default `1 << 14 = 16_384`.
Not exposed via `config.toml` (it is a `#[serde(skip)]` field). At
runtime, the Sequencer→Transactor, Transactor→WAL, and WAL→Snapshot
queues are *all* sized from this single value — there is no separate
WAL queue knob despite the constructor's `wal_queue_size` parameter
name.

### §15.3 `wait_strategy`

Pipeline-mode for every stage's idle/backpressure loop. Default
`Balanced` (§13.2). One value is shared across every stage of a single
Pipeline.

### §15.4 `log_level`

Default `Info`. Not exposed via `config.toml`; set programmatically via
`LedgerConfig::log_level`. `LedgerConfig::temp` and `LedgerConfig::bench`
both lower this to `Critical` to keep test output clean.

### §15.5 `transaction_count_per_segment` (drives multiple subsystems)

Set on `StorageConfig`. Default `10_000_000`. Controls three things at
once:

- WAL segment rotation threshold (§7.6).
- Dedup active window size (§5.2).
- Hot index circle sizing (§9.6).

These are deliberately driven by one knob: changing the active window
should not desynchronise any of the three.

### §15.6 `seal_check_internal`

Interval between Seal polls (§11.2). Default `1 s`. Not exposed via
`config.toml`. The Committer (§7.7) does not use this; it is purely
the Seal thread's poll interval.

### §15.7 `disable_seal`

When `true`, the Seal thread is not started (§11.8). Default `false`.
Not exposed via `config.toml`. Benchmark-only: a ledger with
`disable_seal = true` is not crash-recoverable past the active
segment.

### §15.8 `snapshot_frequency`

Set on `StorageConfig`. Default `4`. Controls the Seal stage's snapshot
emission rule (§11.5): emit when `segment_id % snapshot_frequency == 0`.
`0` disables snapshots entirely. Smaller values mean less WAL replay on
recovery (faster startup) at the cost of more snapshot I/O during
operation.

### §15.9 Hot-index sizes are derived

Circle sizes (§9.6) are computed by `LedgerConfig::index_circle1_size()`
and `index_circle2_size()` from `transaction_count_per_segment`
(§15.5). There is no separate ledger-side knob; changing the active
window changes both the hot indexes and the dedup window in lockstep.

---

## §16 Transaction ID space and recovery posture

### §16.1 `tx_id` properties

`tx_id` is a `u64`. It is monotonic, globally unique per submission,
strictly ordered by submission sequence, and never reused. A `tx_id` is
a position in the global log — there are no gaps, no reorderings, and
the ID itself is the source of all per-transaction identity in the
WAL, the indexes, and the link records.

### §16.2 Restart restores `sequencer_index`

On startup, after recovery (§12.3) determines `last_tx_id` from the
WAL, `sequencer_index` is set to `last_tx_id + 1`. The next
submitted operation therefore receives the lowest available unused ID,
preserving monotonicity across restarts.

### §16.3 In-flight IDs are not re-used

Any ID that the Sequencer assigned but the WAL did not commit before a
crash is *not* re-used after restart. Such transactions were not
durable; they are simply lost. The next submitted operation gets
`last_committed_tx_id + 1`, leaving a (possibly empty) gap in the
*Sequencer's* historical assignment without producing any gap in the
committed log. The committed log, by §16.4, has no gaps.

### §16.4 Committed transactions are never lost

Every committed transaction is in the WAL, and the WAL is always
replayed on recovery. A committed transaction therefore survives any
crash — including a crash mid-`fdatasync` — provided the underlying
storage honours its durability contract. The forwarding rule (§7.5)
is what makes this true on the read side as well: the Snapshotter
never sees a transaction that is not yet on disk, so a transaction that
was visible to a reader before the crash is by definition committed
and is replayed on restart.
