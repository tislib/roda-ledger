# Internal

Single source of truth. Each `§` states one complete invariant or decision in
its own paragraph. Ambiguous behaviour is resolved by what this document says;
where an ADR exists for a decision it is cited as `[ADR-NNNN]`.

Stages:

1. **The Ledger** — `§1..§16`. The in-memory engine: structures, threads,
   queues, indexes, and the sequence in which records flow through the
   pipeline.
2. **The Storage** — `§17..§25`. The on-disk file format: segment file
   layout, WAL record byte layout, sidecar files, snapshot file format,
   function binary storage, on-disk index file format, durable cluster
   state (term and vote logs), and ctl tools.
3. **The Cluster** — `§26..§39`. The network-facing surface (gRPC, client,
   proto) and Raft replication.

Stage 1 specifies the *mechanism* by which the ledger reads and writes
records; Stage 2 specifies the on-disk *shape* of those records; Stage 3
specifies how independent ledgers replicate to one another over a network.

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

Every WAL record is exactly 40 bytes — a hard invariant for every record
type. The first byte of every record is its `entry_type` discriminant, which
makes the WAL stream `[record][record][record]…` self-describing without a
separate length-prefix or framing layer. Records are laid out for zero-copy
serialization: the bytes on the wire are the bytes in memory. A transaction is
a group of follower records (`TxEntry`/`TxLink`) terminated by a trailing
`TxMetadata` — its commit record (§18.3, [ADR-0020]).

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

The Pipeline is the sole owner of the transaction ring (§13.4), both
inter-stage queues, every progress index, the shutdown flag, and the shared
wait strategy. Stages never own this state directly — each receives a typed
context that exposes only the slice it may legally read or publish. The
wrapping is purely for type-level encapsulation; there is no runtime cost.

### §2.4 Inter-stage transport

The record stream rides a single lock-free **transaction ring** (§13.4): the
Transactor is the sole producer and the **WAL is the sole consumer** — it both
walks the ring and releases the slots it has consumed ([ADR-0021]). The
Snapshotter no longer touches the ring at all; it tails the *durable WAL* via a
`WalTailer` (§8.4). The former `transactor → wal` and `wal → snapshot` queues
are gone ([ADR-0019]).

Two lock-free, fixed-capacity, single-producer / single-consumer queues
remain, for the paths that are not the record stream:

- `sequencer → transactor` — submitted operations awaiting execution.
- `snapshot query` — `GetTransaction` / `GetAccountHistory` requests (§8.4).

Queue capacity is a fixed internal constant (`1024`, §15.2); ring
capacity comes from `ring_size` (§15.10). A full queue or a full ring is the
only backpressure signal; producers spin/yield until space exists. There is no
other flow-control mechanism.

### §2.5 Global progress indexes

The pipeline holds six monotonic progress counters plus a seal-progress
counter, a cluster seal gate, and a shutdown flag:

- `sequencer_index` — next `tx_id` to be handed out.
- `compute_index` — last `tx_id` executed by the Transactor.
- `write_index` — last `tx_id` the WAL Writer wrote to the active segment's
  *page cache* — buffered only, **not yet fsynced** (that gate is
  `commit_index`). It is the high-water mark the ring's release is keyed to
  (§13.5) and lets callers observe pipeline transit minus the fsync.
- `commit_index` — last `tx_id` *durably* written by the WAL (post-`fdatasync`).
- `snapshot_index` — last `tx_id` reflected in the Snapshotter.
- `seal_index` — last segment id sealed (a `u32` segment id, not a `tx_id`).
- `seal_step_id` — monotonic counter the seal stage bumps each pass, so
  `wait_for_seal` can observe progress without holding the `Seal`.
- `seal_watermark` — cluster-commit seal gate (§34, [ADR-0016 §10]): a segment
  may be sealed only once every transaction it contains is `≤ seal_watermark`.
  Defaults to `u64::MAX` ("no gate") for standalone ledgers; the cluster
  supervisor drives it from the raft cluster-commit index.
- `running` — global shutdown flag.

The indexes are padded to separate cache lines so that a publication on one
does not invalidate the cache line of an adjacent index. They are
strictly monotonic and never go backwards. The mapping
`commit_index ≥ N` ⇒ every transaction `≤ N` is committed (no committed
gaps) is what makes per-call wait levels (§14) meaningful: a single
threshold check on a single counter resolves any wait.

### §2.6 Backpressure and shutdown

Backpressure is implicit and propagates through ring and queue saturation: a
slow WAL Writer (the ring's sole consumer) stops releasing ring slots (it
releases right after the page-cache `write`, §13.5), so the ring fills and the
Transactor stops finding free slots; the stalled Transactor fills the
sequencer→transactor queue, which stalls `submit()`. There is no rate limiter, leaky bucket, or
admission control. Shutdown is the inverse signal:
`Pipeline::shutdown()` clears `running`; every stage's idle loop checks the
flag and exits. The shutdown signal does not need to be observed
immediately — the queues drain to a safe state regardless of the precise
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

ID assignment is a single atomic increment of `sequencer_index`.
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

When the sequencer→transactor queue is full, the submitter spins, yielding
the thread periodically while the spin persists. There is no timeout —
`submit` returns only after the operation is queued. Spinning (rather than
parking) is preferred here because submit-side latency is the primary
concern: a parked thread misses the moment space appears.

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

The Transactor's working state — balance cache, per-step entries buffer,
position marker, current `tx_id`, current `fail_reason` — lives entirely
on the Transactor thread and never crosses a thread boundary. The WASM
runtime instance bound to a Transactor inherits this constraint. Single
ownership means no locks are needed on the hot path; host calls (§6.7)
mutate the same state through interior mutability with no contention.

### §4.3 Balance cache layout

The Transactor's balance cache is a flat array indexed directly by
`account_id`. Lookups and updates are O(1) array accesses with no hashing
and no probing. The array is seeded to `initial_account_size` at construction
and **grows on demand** as higher account ids are referenced — it is not a
fixed ceiling. [ADR-0002, ADR-0022]

### §4.4 The growable account array

There is no `max_accounts` ceiling. When an operation references an
`account_id` beyond the current array length, the Transactor grows the array
before mutating any state: `grow_capacity` computes the next capacity
geometrically — `new_cap = ceil(cap × (1 + resize_factor))`, clamped up to
cover the requested id — and the array is resized in place, defaulting the new
slots. Growth is amortised O(1); the geometric factor keeps reallocations rare
even under a sparse id space. The `u64` id space is effectively inexhaustible
(`u32::MAX` accounts already exceeds 50 GB of state), so exhaustion is treated
as a bug and the allocator panics rather than returning a status. The retired
`ACCOUNT_LIMIT_EXCEEDED` reason (former status `6`) no longer exists. The same
geometric growth is applied symmetrically to the read-side cache (§8.3) and the
seal-side balance vector. [ADR-0002, ADR-0022]

### §4.5 Linearizability of the write path

The Transactor never reads stale balances. Because it is the sole writer
and operates on the in-memory cache (§4.3), every read it performs during
operation execution observes the cumulative effect of every earlier
transaction. The write path is therefore linearizable by construction;
the read path requires an explicit `submit_wait(snapshot)` (§14.4) for the
same guarantee.

### §4.6 In-place assembly in the ring

The Transactor writes the records it produces directly into the transaction
ring's uncommitted region — the slots above the write frontier that it owns
exclusively and no reader can see (§13.4). There is no separate staging buffer:
the ring *is* the per-transaction scratch space. As it pushes followers it
accumulates the two derived metadata fields — a running CRC digest (§4.10) and
a follower count (`pending_items`) — so finalize needs no second pass. On
finalize it assembles the trailing `TxMetadata` once with those values and
publishes the whole transaction atomically by advancing the write frontier; on
failure it discards the uncommitted tail (§4.12).

### §4.7 The `tx_start_index` marker

The Transactor records the ring index where the *current* transaction's first
follower lands (`tx_start_index`). Verify (§4.11) and rollback (§4.12) scope
their work to the slots from `tx_start_index` to the write head — the in-flight
transaction's records — and never touch already-published ones. The CRC is
folded incrementally as those records are pushed (§4.10), not re-walked from
the marker.

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

Each `credit` / `debit` produces one `TxEntry` record with the
running `computed_balance` field already populated.

### §4.9 Default INSUFFICIENT_FUNDS protection

For built-in operations, the Transactor refuses any change that would drive
an account balance negative (it checks before the debit). This is the
default protection. WASM functions (§6) are the only mechanism for
producing a negative balance: a function must explicitly call `get_balance`
itself and decide to proceed. There is no configuration knob to disable
the built-in check.

### §4.10 Per-transaction CRC32C

The Transactor folds a running CRC32C over each follower record as it is
pushed, in push order, then — when the transaction finalizes — appends the
trailing `TxMetadata` (with its own `crc32c` field zeroed) into the digest and
stores the result into that field. The CRC therefore covers the followers (in
order) followed by the zeroed-crc metadata, and is computed incrementally
rather than by re-walking the transaction after the fact. It uses CRC32C
(Castagnoli) — hardware-accelerated on modern CPUs. Because the field
participates in the digest while being part of the record, it is zeroed during
computation; a consumer verifies by recomputing in the same order — the
followers, then the zeroed metadata (§18.3, [ADR-0020]).

### §4.11 Zero-sum invariant

Every transaction must net to zero: `sum(credits) == sum(debits)` across
all `TxEntry` records of that transaction. The Transactor verifies this
after operation execution and before any state is committed; a violation
raises `ZERO_SUM_VIOLATION` and the transaction is rolled back
(§4.12). The invariant is per-transaction — there is no cross-transaction
arithmetic. Because each individual transaction nets to zero, the sum of
all balances across all accounts is always zero — an emergent property,
not a configuration option.

### §4.12 Rollback

When verify fails or a WASM execution rejects, the Transactor reverses every
credit/debit it produced for this transaction against the live balance cache
and discards the uncommitted tail in the ring (rolling the write head back to
`tx_start_index`, §4.7). It then sets `fail_reason` and finalizes the
transaction as a lone trailing `TxMetadata` (`sub_item_count = 0`) — see §4.13.
There is no leading metadata to preserve: the metadata is materialized only at
finalize. The cost is O(entry_count) and is acceptable because typical entry
counts are small.

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

The current `tx_id` is set on the Transactor's thread-local state at
the start of each transaction; host calls (§6.7) read it from there.
It is never passed across the WASM host-call boundary as a parameter.
This avoids the cost of an extra call argument and removes the risk of
a function caching or forging a stale `tx_id`.

### §4.17 Rejection registry

Rejected transactions are recorded in a concurrent map keyed by `tx_id`,
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
`user_ref → tx_id` hash maps, *active* and *previous*. When `tx_id`
crosses the segment boundary, the maps flip: *active* becomes *previous*,
the previous map's storage is reused as the new (cleared) *active*.
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

On startup, the dedup cache is seeded by walking each WAL transaction
inside the active window (§12.3). Entries outside `2 × window` are
dropped; entries within the window are placed into the correct half
(*active* vs *previous*) based on their relative position. Post-recovery
dedup behaviour is therefore identical to pre-crash — a duplicate
submitted before and after a restart is rejected the same way.

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

### §6.3 One engine, one linker per ledger

There is one wasmtime engine and one host-import linker per ledger,
shared across every Transactor. The linker carries the host imports
(§6.7); the engine compiles and caches modules. Constructing either is
expensive, so a ledger does it once.

### §6.4 Shared registry

The shared registry maps `name → (version, crc32c, compiled module)`. It
is protected by a reader-writer lock and is touched only on register,
unregister, and cache miss / revalidation — never on every transaction.
The hot path (§6.5) avoids the lock entirely.

### §6.5 Per-Transactor caller cache

Each Transactor holds its own local cache of `name → compiled handler`,
tagged with a sequence number that records the registry version the
entry was verified against. Resolution is:

1. Look up `name` in the local cache.
2. If the cached sequence matches the registry's current value, use the
   cached handler directly with no lock.
3. Otherwise, take the registry read lock, fetch the current entry,
   instantiate, install into the local cache, stamp the entry with the
   new sequence.

Cache invalidation is per-name: registering or unregistering `foo` does
not evict the cached entry for `bar`.

### §6.6 Required export

Every registered binary must export exactly one function:
`execute(i64, i64, i64, i64, i64, i64, i64, i64) → i32`. The eight
parameters correspond to the `Function` operation's `[i64; 8]` (§1.5).
The `i32` return is the function's status: `0` is success, anything
else is mapped per §6.12. [ADR-0014]

### §6.7 Host imports

The host module is `"ledger"` and exports exactly three imports:

- `ledger.credit(account_id: i64, amount: i64) → ()`
- `ledger.debit(account_id: i64, amount: i64) → ()`
- `ledger.get_balance(account_id: i64) → i64`

Each call mutates the Transactor's thread-local state (§4.2): `credit`
and `debit` append a `TxEntry` to the per-step buffer; `get_balance`
reads the live balance cache. The current `tx_id` is pulled from the
state (§4.16), not passed through the call. [ADR-0014]

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

Host-call effects accumulate in an isolated execution context on the
Transactor side, not in WASM-visible globals. Nothing is applied to the
live balance cache until *all* of the following hold: `execute` returned
`0`, the captured credits and debits balance (§4.11), and no
wasmtime-layer error occurred. If any of those fail, the entire batch
of host calls is discarded — the same rollback machinery as a built-in
operation (§4.12). Nothing is published to the transaction ring, so no reader
ever sees a partial function execution.

### §6.11 Wasmtime-layer failure mapping

A wasmtime layer failure — link error, instantiation error, or runtime
trap — is mapped to `INVALID_OPERATION = 5` and rolled back. The
mapping is single-valued by design: from outside the runtime, *any*
infrastructure-level WASM failure looks the same. The function author's
own status codes (§6.12) live in a disjoint range.

### §6.12 Status code passthrough

The `i32` return of `execute` is interpreted as follows:

- `0` → success (provided zero-sum holds; §4.11).
- `1..=7` → mapped to the corresponding standard `FailReason`.
- `8..=127` → reserved; treated as `INVALID_OPERATION`.
- `128..=255` → user-defined; passed through into `TxMetadata.fail_reason`
  unchanged.
- Anything else (negative, > 255) → treated as `INVALID_OPERATION`.

User-defined codes are the WASM author's contract with their callers; the
ledger never reinterprets them. [ADR-0014]

### §6.13 Entry-limit enforcement

If a function emits more than 65535 `credit`/`debit` calls in a single
execution, the `u16` `sub_item_count` field of `TxMetadata` cannot
encode the count. The Transactor fails the transaction with
`ENTRY_LIMIT_EXCEEDED` and rolls it back. Function authors who
need more than 65535 entries must split the work across multiple
transactions.

### §6.14 Audit tag

Every committed function-produced transaction stamps `TxMetadata.tag`
with `b"fnw\n" ++ crc32c[0..4]` — the four-byte literal `"fnw\n"`
followed by the first four bytes of the executing binary's CRC32C. Any
past transaction can therefore be traced back to the exact binary that
produced it, even after registrations have changed.

### §6.15 Names and versions

Function names are validated at registration: ASCII snake_case, ≤ 32
bytes, null-padded to 32 in the WAL record. Each register or
override bumps the per-name `version` by one, starting at `1`, with a
ceiling of `65535`. The `(name, version, crc32c)` triple uniquely
identifies the executing binary in every transaction. [ADR-0014]

### §6.16 Registration is durable

`RegisterFunction` only returns after all four of the following have
happened: the binary has been validated, written atomically to disk, a
`FunctionRegistered` WAL record has been committed, and the compiled
handler has been installed in the live WASM runtime. A subsequent
`Operation::Function` is therefore guaranteed to see the new version.
After a crash, recovery rebuilds the registry from a paired function
snapshot plus the `FunctionRegistered` records that follow it (§12.4) —
the live runtime always matches what the WAL says it should be.

### §6.17 Unregistration

Unregistration emits a `FunctionRegistered` record with `crc32c = 0`
and writes a 0-byte file under the next version on disk. The
binary history is preserved as a sequence of versions; the function
registry's *current* state is the last record seen. The WAL is
authoritative; the on-disk file is a secondary signal. [ADR-0014]

### §6.18 Snapshotter applies registry events

`FunctionRegistered` records flow through the WAL like any other record
(§7.4). The Snapshotter (§8.7) is the stage that actually loads or
unloads the compiled handler in the shared WASM runtime: it reads the
binary off disk on a load, and unloads on an unregister. This is what
keeps the live runtime synchronised with the WAL on every node, leader
or follower.

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

### §7.2 Coordination between Writer and Committer

The two threads share two counters and one segment handle:

- *last-written* — the highest `tx_id` whose record has been written
  (but not necessarily synced) to the active segment file. The Writer
  publishes; the Committer observes.
- *last-committed* — the highest `tx_id` whose record has been
  `fdatasync`'d. The Committer publishes; the Writer observes. This is
  what `commit_index` exposes to the rest of the pipeline (§2.5) and
  what the `wal` wait level (§14.1) gates on.
- The active segment's sync handle, swappable across rotations (§7.6)
  so the Committer always syncs the segment that is currently open.

There is no other coordination — no condvars, no channels.

### §7.3 Writer buffer

The Writer copies records out of the ring into a bounded pending-write buffer,
batched so a whole iteration's worth of records reaches disk in one `write`
syscall. The buffer is sized in advance — never resized — so the WAL stage's
memory footprint is fixed. As the **sole ring consumer**, the Writer releases
the ring slots it has copied out (§13.5) immediately after that buffered
`write`. It forwards records nowhere: the Snapshotter tails the durable WAL
independently (§8.4).

### §7.4 Writer per-iteration loop

Each iteration the Writer copies the newly published ring records into the
active segment's pending-write buffer, performs a single buffered `write`
syscall (one per iteration, not per record), advances *last-written*
(`write_index`, §2.5), **releases the consumed ring slots** (§13.5), and rotates
the segment if the transaction-count threshold is met (§7.6). The release
happens right after the page-cache `write`, gated on `write_index` — not on
`fdatasync` — so a stalled Writer back-pressures the Transactor while the fsync
stays decoupled. It does not gate or forward anything to the Snapshotter —
durability visibility is the Snapshotter's own concern (§7.5). As records are
synced by the Committer, `commit_index` (§2.5) advances.

### §7.5 Durability rule, enforced reader-side

The durability rule still holds: the Snapshotter — and therefore `get_balance`,
queries, and the snapshot wait level — never sees a transaction that is not yet
on disk. But the WAL no longer enforces it by forwarding only durable records.
Instead the Snapshotter **tails the durable WAL** and gates *itself*: it applies
a transaction only once the trailing `TxMetadata`'s `tx_id ≤ commit_index`
(§8.4). The Writer therefore keeps no per-transaction "expected record count"
bookkeeping; it writes whatever the ring publishes, and a transaction is
recognised as complete at its trailing metadata. A transaction may span a
segment boundary (§18.3, [ADR-0020]).

### §7.6 Segment rotation

The Writer rotates segments on a transaction-count threshold: when the
number of transactions in the active segment reaches
`transaction_count_per_segment`. This is the same boundary that drives
dedup window flips (§5.2) and hot-index sizing (§9.6). Rotation performs
a *synchronous* `fdatasync` (the Writer cannot defer this one to the
Committer because the next step closes the file), closes the segment,
opens the next one, and hands the new sync handle to the Committer. No
in-band markers are written — segment boundaries are determined by file
rename and the `.seal` marker created later by the seal thread. Rotation
is a transient cost: the pipeline pauses only for the synchronous sync
and the open of the next file. [ADR-0013]

### §7.7 Committer loop

Driven by the shared wait strategy (§13.2), the Committer samples
*last-written*, calls `fdatasync`, and publishes the sample as
*last-committed*; it picks up a fresh sync handle whenever rotation
swaps the active file. It never parses entries, never touches the
Writer's buffer, and never inspects any queue — its single
responsibility is moving *last-committed* forward as fast as storage
allows.

### §7.8 Backpressure from a slow WAL

There is no outbound queue to fill. Because the WAL Writer is the ring's **sole
consumer and sole releaser** (§13.5), a persistently slow Writer stops releasing
ring slots, so the ring eventually fills and the Transactor — the sole producer
— stalls for want of free slots (§2.6). The Snapshotter is not on this path at
all: it tails the durable WAL (§8.4) rather than the ring, so a slow Snapshotter
can fall behind on read-side visibility but never throttles the ring or the
Transactor. Snapshotter slowness only bounds how stale `get_balance` is, not
ingest throughput.

### §7.9 Failure handling

A failed write or `fdatasync` is a hard panic. There is no retry loop
on durability failures: returning an error to a submitter that has
already been told its transaction was committed would be a worse
violation of the contract than a process exit. Recovery (§12) is
responsible for putting the WAL back into a consistent state on
restart.

---

## §8 Snapshotter — apply path

### §8.1 Apply, don't re-execute

The Snapshotter applies committed entries to the read-side state. It does
no business logic and never re-executes operations. Because every
`TxEntry` carries `computed_balance`, the Snapshotter's job is a
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

The read-side cache is a flat array of atomic balances, seeded to
`initial_account_size`. The Snapshotter writes; many readers can load
concurrently without locks. Like the write-side array (§4.4) it **grows on
demand** rather than sitting at a fixed ceiling: when an `AccountOpened`
follower covers an id beyond the current length, the Snapshotter grows the
array geometrically (`resize_factor`) via `ensure_capacity`, swapping in a
larger generation through the `ArcSwap` so concurrent readers never see a
torn vector. [ADR-0002, ADR-0022]

### §8.4 The durable-WAL tailer and the query queue

The Snapshotter draws from two independent sources, in a fixed order each
iteration:

- **A `WalTailer` over the durable WAL** — *not* the transaction ring. The
  tailer streams the on-disk segments forward from a cursor and, via
  `tail_transactions`, groups each transaction's followers with its closing
  `TxMetadata` for the apply step (zero-copy: a borrowed view, no owned `Vec`).
  The ring is the WAL's private input (§2.4); the Snapshotter never reads it.
- **A query-only SPSC queue** — `GetTransaction` / `GetAccountHistory` requests
  enqueued by callers (§10).

A query is no longer interleaved with entries in one FIFO, so read-your-own-
writes is not automatic from enqueue ordering; it rests on the wait levels
(§14). A caller that waits for `snapshot` before querying is guaranteed
`snapshot_index ≥ tx_id`, and because the loop applies a transaction's entries
before advancing `snapshot_index`, the subsequent query observes them. A query
issued without that wait races the apply loop — the same trade-off a
`none`-level submit makes.

### §8.5 Apply algorithm

The Snapshotter tails the durable WAL from its cursor; `tail_transactions`
hands it each transaction as its `TxEntry` / `TxLink` followers grouped with the
trailing `TxMetadata`. It first checks the durability gate: if
`meta.tx_id > commit_index` the transaction is not yet fsynced, so the tailer
retains it and the walk resumes once it commits. Otherwise it applies the
buffered followers together — updating the hot index (§9.4) and publishing each
`TxEntry`'s `computed_balance` to the read-side balance cache — then advances
`snapshot_index` (§2.5) to this `tx_id`. The Snapshotter does **not** release
any ring slots: that is the WAL's job, as the ring's sole consumer (§13.5).
Applying at the trailer is the *only* point the read-side index advances, so
readers never observe a partially-applied or
not-yet-durable transaction. `FunctionRegistered` takes the registry path of
§8.7.

### §8.6 `last_tx_id` as the freshness signal

`get_balance` returns the current balance plus `last_tx_id` — the
highest `tx_id` whose every entry has been applied to the read-side
cache. `last_tx_id` is the tool callers use to reason about freshness
without blocking, and it is the signal that confirms multi-account
transactions are fully settled (§4.15). For a linearizable read, the
caller waits for `last_tx_id` to pass their `tx_id`, or uses
`submit_wait(snapshot)` (§14.4).

### §8.7 FunctionRegistered handling

`FunctionRegistered` records are not financial transactions and do not
advance `snapshot_index`. On a register record, the Snapshotter reads
the binary off disk and installs it in the shared WASM registry
(§6.4). On an unregister record (`crc32c == 0`, §6.17), the Snapshotter
unloads the function from the registry. Errors at either step are
logged; they do not stop the apply loop. The Snapshotter is therefore
the *only* stage that mutates the live runtime registry while the
system is running — the Transactor reads through its per-Transactor
caller cache (§6.5), which revalidates lazily on miss.

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

### §9.1 Two buffers, all pre-allocated

The hot transaction index is a single in-memory structure with two
pre-allocated buffers, sized at construction and never resized:

- **circle1** — for each `tx_id`, a pointer to that transaction's
  followers in circle2.
- **circle2** — follower storage: each transaction's followers (entries
  and links alike) stored as raw `WalEntry` records, as-is.

Both array sizes must be powers of two. The index is a `tx_id`-keyed
cache only; account history is *not* served from here (§10.3). [ADR-0008]

### §9.2 Why power-of-two

Both lookups reduce to "id AND mask" to find a slot, where mask is
`size − 1`. The power-of-two requirement turns the lookup into a single
bitwise operation. The cost is at most ~2× memory in the worst case
(desired size just above a power of two); the alternative (modulo)
costs a division on every query, which is far more expensive at ledger
throughput.

### §9.3 circle1 — direct-mapped tx slot table

Each circle1 slot stores a `tx_id`, an offset into circle2, and the
entry count for that transaction. The slot index for a transaction is
its `tx_id` masked by the circle1 size. A slot whose stored `tx_id`
does not match the queried `tx_id` is **evicted** — overwritten by a
colliding transaction that mapped to the same slot. Eviction is silent:
the slot simply now belongs to a newer transaction. There is no
eviction queue, no cleanup pass. Lookups detect eviction by comparing
the slot's `tx_id` to the queried one and falling back to disk on
mismatch (§10.2).

### §9.4 circle2 — follower storage

Each circle2 slot stores one follower record as a raw `WalEntry` (a
`TxEntry`'s `tx_id`, `account_id`, `amount`, `kind`, `computed_balance`,
or a link record — stored as-is), tagged with the owning `tx_id` so a
colliding overwrite is detectable. circle2 is written sequentially with
a write head that advances on every insert and wraps modulo circle2's
size. On insert, the indexer writes the transaction's followers into
consecutive slots from the current write head and records the start
offset and follower count in that transaction's circle1 slot. There is
no per-account chaining and no per-account head table — circle2 is a
flat, `tx_id`-keyed follower store. Per-account history is reconstructed
on demand by scanning the WAL (§10.3), independent of this index.

### §9.5 Recovery seeding

The recovery sequence (§12) seeds the indexer directly: as it walks
WAL records from the last snapshot through the active segment's tail,
each metadata, entry, and link record is replayed into the indexer
through the Snapshotter's recovery hooks. By the time the Snapshotter
thread starts, the indexer is already populated for everything in the
active window.

### §9.6 Sizing rule

Both sizes are derived from `transaction_count_per_segment` (§15.1):

- circle1 — one slot per transaction in the active window, rounded up
  to a power of two.
- circle2 — `4 ×` the circle1 size, giving headroom for several
  followers per transaction (entries plus any links).

Changing the active window changes the hot index and the dedup window
in lockstep; there are no separate knobs. [ADR-0013]

---

## §10 Query path — hot/cold routing

### §10.1 Query types

Two query types reach the Snapshotter through its query queue (§8.4):

- `GetTransaction { tx_id }` — return the entries (and links) of one
  transaction.
- `GetAccountHistory { account_id, from_tx_id, limit }` — return up to
  `limit` entries that touched `account_id`, newest first, stopping at
  `from_tx_id` (exclusive lower bound; can be omitted).

A third query type — `get_balance(account_id)` — is *not* routed
through the Snapshotter. It is a direct read against the read-side
cache (§8.3) and never blocks.

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

`GetAccountHistory` does not consult the in-memory transaction index at
all — there is no per-account head table or link chain.
`Ledger::get_account_history` runs a backward WAL scan
(`wal_scanner().scan(from_tx_id, …)`), sweeping transactions
newest→oldest from `from_tx_id` and keeping those whose entries
reference the account (`entry_references_account`: a matching
`TxEntry.account_id`, an account-open range, a link, or a flag update).
Results are returned newest-first, bounded by the requested window and
`limit`. Because it reads the durable WAL directly, it is uniform across
recent and sealed history — there is no hot-miss/cold-fallback step.

### §10.4 Hot tier vs cold tier

Hot/cold tiering applies to `get_transaction` lookups, not to account
history. The hot tier is whatever `tx_id`s are currently resident in the
indexer; the cold tier is everything sealed to disk. The boundary is not
the absolute `tx_id` value but how recently each slot has been
overwritten — a `tx_id` from a quiet period can survive deep into
history, while a busy stretch may be evicted within one segment.
Cold-tier reads are sealed-segment lookups (the on-disk transaction
index built by Seal — §11.4); on a hot miss the indexer returns `None`
and the caller reissues against the cold-tier API. `GetAccountHistory`
is tier-agnostic: it always scans the durable WAL (§10.3), so it has no
hot-miss/cold-fallback path.

### §10.5 Synchronous response via callback

A query carries a one-shot callback. The convention is: caller creates
a per-request channel, wraps the sender in a closure, enqueues the
request, and blocks on the receiver. The Snapshotter computes the
response inline (§8.8) and invokes the callback before continuing the
apply loop. There is no separate response channel, no shared state for
partial results, no async runtime.

### §10.6 Status and balance queries

Two query paths bypass the query queue entirely:

- `get_balance(account_id)` — a direct atomic load on the read-side
  balance cache. Does not block, does not touch the Snapshotter,
  returns whatever balance the cache currently holds along with
  `last_tx_id` (§8.6).
- `get_transaction_status(tx_id)` — derived (§2.2) from the pipeline
  indexes plus the rejection registry (§4.17). Does not block.

Neither of these traverses the Snapshotter's query queue; both serve very high
read volumes without contention.

---

## §11 Seal stage — segment finalisation and snapshots

### §11.1 What sealing means

Sealing is the act of making a *closed* segment safe to recover from.
The WAL Writer (§7.6) closes a segment when the transaction-count
threshold is reached; the closed file on disk has no integrity-check
sidecars and no built indexes. Sealing turns this into a fully
recoverable artifact: it computes a file-level CRC, writes sidecar
files, builds the on-disk transaction and account indexes for cold-tier
queries (§10.4), and — at the configured frequency — emits a balance
snapshot and a function snapshot. The exact file format of all of
these is in Stage 2; the *mechanism* is described here.

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

For each closed-but-unsealed segment, Seal computes the file-level
CRC, writes the `.crc` and `.seal` sidecars, builds the on-disk
transaction and account indexes (§22), replays the segment into
Seal's private balance vector and function map (§11.4), and — at the
snapshot frequency boundary (§11.5) — writes the paired snapshot
files. The sealed segment id is then published (advances `seal_index`,
§2.5). A failure during sidecar or index construction is logged; the
segment is left unsealed and retried on the next poll.

### §11.4 Why Seal owns its own balance vector and function map

Seal holds two pieces of state separate from the rest of the pipeline:

- A balance array seeded from `initial_account_size` (and grown on demand the
  same way as the other account arrays, §4.4), updated from each entry's
  `computed_balance` as Seal walks the segment. This is the source
  for the balance snapshot (§11.5); it is *not* the read-side cache
  (§8.3).
- A map of `name → (version, crc32c)`, updated on each
  `FunctionRegistered` record. Unregister records (`crc32c == 0`) are
  kept in the map so the snapshot preserves the audit trail.

Why a separate copy: Seal must snapshot the *committed-and-sealed*
state, which lags `commit_index`. The Snapshotter's read-side cache is
the *latest-committed* state and can race ahead. A snapshot taken from
either of those would either include uncommitted state or be racy.
Reading off the segment, on a thread that does not care about the
freshest tx, gives Seal a consistent point-in-time view at zero
synchronisation cost.

### §11.5 `snapshot_frequency` rule

When `snapshot_frequency > 0` and the just-sealed segment's id is a
multiple of `snapshot_frequency`, Seal emits two snapshot files for
that segment:

- A **balance snapshot** — only non-zero balances, sorted by account
  id for determinism.
- A **function snapshot** — every entry from Seal's function map,
  sorted by name. Written even when the map is empty so recovery can
  always jump straight to the latest snapshot boundary instead of
  replaying WAL from segment 1. [ADR-0014]

When `snapshot_frequency == 0`, no snapshots are emitted and the
system must replay from segment 1 on every recovery. The default is
`snapshot_frequency = 4`.

### §11.6 Pre-seal during recovery

Recovery (§12) may discover a closed-but-unsealed segment from a crash
between WAL rotation and the next Seal poll. Rather than start the
Seal thread early, Recover invokes the same per-segment seal procedure
(§11.3) synchronously and publishes the resulting segment id. After
recovery finishes, the Seal thread starts and resumes its normal poll.
This keeps the Seal thread's lifecycle simple: one thread, one loop,
one shutdown — no special "first iteration" mode.

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
or writing the data directory. `Recover` builds a single `ActiveSnapshot`
struct — the reconstructed `last_tx_id`, account/flag map, function set, links,
KV/constants, dedup `user_ref → tx_id` maps, and the active segment's
transactions — and threads it into each stage's `recover_from`/seed step, so
every stage starts from one consistent reconstructed baseline. Once recovery
returns, the pipeline is in a state that is observationally identical to the
state that existed at `last_committed_tx_id` immediately before the previous
shutdown.

### §12.2 Pre-seal of unsealed segments

The first thing Recover does is scan for closed-but-unsealed segments
(§11.6). For each one it runs the per-segment seal procedure
synchronously and publishes the resulting id. After this step every
segment on disk except the active one is sealed.

### §12.3 Snapshot load and WAL replay

Recovery loads the latest balance and function snapshots — seeding the
Transactor's balance cache (§4.3), Seal's balance vector (§11.4), the
read-side balance cache (§8.3), and the live WASM runtime — then
replays sealed segments newer than the snapshot's segment id, then
replays the active segment — buffering each transaction's followers until its
trailing `TxMetadata` validates the group, and trusting the tail only as far as
the last complete metadata boundary (Stage 2). The follower buffer carries
across segment seams, so a transaction split over a boundary reassembles.
Finally `sequencer_index` is restored to `last_tx_id + 1` (§16.2). The fixed
ordering is the load-bearing constraint (§12.4).

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

## §13 Inter-stage transport, backpressure, wait strategies

### §13.1 Lock-free transport

The record stream uses a single lock-free **transaction ring** (§13.4); the
submit and query paths use lock-free, fixed-capacity, single-producer /
single-consumer queues (§2.4). Each stage owns its data completely; no shared
mutable state crosses stage boundaries. Queue capacity is a fixed internal
constant (`1024`, §15.2); the ring is sized from `ring_size` (§15.10).

### §13.2 Wait strategies

Every stage's idle / backpressure loop is driven by the shared wait
strategy. Four variants exist:

- **LowLatency** — pure spin. CPU is burned unconditionally; latency
  is minimised. Suitable for dedicated cores.
- **Balanced** (default) — short spin, then yield, then park briefly.
  Adapts: under load it behaves like `LowLatency`; when idle it costs
  nothing.
- **LowCpu** — no spin, short yield, then park. For deployments where
  background CPU cost matters more than latency.
- **Custom** — explicit spin / yield / park parameters.

A single wait strategy is shared by every stage of one Pipeline; there
is no per-stage override.

### §13.3 Backpressure as ring and queue saturation

There is no rate limiter, leaky bucket, or admission control. Slowness anywhere
propagates upstream: a stalled WAL Writer (the ring's sole consumer) stops
releasing ring slots (§13.5), filling the ring and stalling the Transactor,
which fills the sequencer→transactor queue and eventually stalls `submit()`
(§2.6). This is intentional — the ring and queues are the only place stages
observe each other's progress, and using them as the backpressure mechanism
keeps the design free of explicit coordination.

### §13.4 Transaction ring transport

The transaction ring is a fixed-capacity, lock-free buffer that is the sole
transport for the record stream between the Transactor and the WAL
([ADR-0019], [ADR-0021]). It is single-producer, **single-consumer**:

- The **Transactor** is the only writer. It writes records into the ring's
  uncommitted region (slots above the *write frontier*), builds each transaction
  in place there (§4.6), and publishes by advancing the write frontier — so the
  consumer only ever observes whole, committed transactions.
- The **WAL** is the only reader, *and* the only releaser. It walks the ring,
  copies each record out (a slot may be reused once released, so it never holds
  a borrow across reclamation), and advances the release frontier itself
  (§13.5). The read window is `[released, write)`; because the writer is gated
  on the release index that only the WAL advances, in-window slots are never
  overwritten, so reads copy out directly with no per-entry window check.
- The **Snapshotter is not a ring reader.** It tails the durable WAL (§8.4),
  decoupled from the ring entirely.

Positions are absolute, monotonically increasing indices mapped to physical
slots on access; the capacity (`ring_size`, §15.10) is a power of two and must
be at least the largest possible single transaction, since a transaction is
published atomically and must fit in the uncommitted region. The producer never
blocks inside the ring: when no free slots exist it backs off under the shared
wait strategy (§13.2) and retries — backpressure is caller-driven, not hidden in
the transport.

### §13.5 Write-gated reclamation

A ring slot may be reused once the record it held has been **written to the
active segment's page cache** — *not* once it is fsynced ([ADR-0021]). The WAL,
as the ring's sole consumer and sole releaser, advances the reclamation point
(the *release frontier*) right after each buffered `write` syscall, in lockstep
with `write_index`: it publishes `write_index` and then calls `release_to` on
the same batch. The frontiers stay ordered `write_index ≥ released`, and
reclamation is therefore **write-gated, not durability-gated** — the
`fdatasync`/`commit_index` step is fully decoupled and runs on the Committer's
own schedule (§7.7). Durability is instead enforced *reader-side*: the
Snapshotter applies a transaction only once `TxMetadata.tx_id ≤ commit_index`
(§8.5), and it reads the durable WAL, not the ring, so the read side never
depends on a slot still being live. If the WAL Writer stalls (slow disk
`write`s), reclamation stalls with it and the producer eventually blocks once
the ring fills — the bounded-transport form of backpressure.

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

### §14.2 Implementation — reactive index hook

A wait is **edge-triggered, not a poll** ([ADR-0027]). The pipeline exposes a
single `IndexHook` — `Fn(PipelineIndexKind, u64)` — fired inline on the
publishing thread each time `compute_index`, `commit_index`, or `snapshot_index`
advances (the sequencer index is deliberately *not* hooked: waiters never block
on it, so firing per-submit would be pure hot-path cost). A consumer registers
one hook via `set_index_hook` (first registration wins) and routes each
`(kind, value)` into a per-index reactive watch. The network wait path (§36.4,
the cluster `Waiter`) does exactly this: each watch is a `u64` plus a `Notify`,
and an async waiter calls `wait_reach(target)` — it registers interest, checks
the value, and parks on the `Notify` edge with no spinning. When the index
crosses the threshold the parked future is woken and synchronises with the store
that produced the advance. The legacy `CommitHandler` / `on_commit` callback
(a `Fn(u64)` fired only on commit) is **superseded and dead** — it has no
production caller; the unified `IndexHook` replaced it.

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

### §15.1 `initial_account_size` and `resize_factor`

The account arrays are **growable**, not fixed (§4.4). `initial_account_size`
is the seed capacity of every balance vector and read-side atomic vector at
construction; `resize_factor` is the geometric growth increment applied when a
higher id is referenced — `new_cap = ceil(cap × (1 + resize_factor))`. Both are
exposed via `config.toml`. There is no `max_accounts` ceiling and no
`ACCOUNT_LIMIT_EXCEEDED` rejection. Size the seed so the common-case balance
vector fits comfortably in L2/L3 even with read-side and seal-side duplicates;
growth past it is amortised and rare. [ADR-0022]

### §15.2 Inter-stage queue capacity

The two inter-stage queues — sequencer→transactor and the Snapshotter's query
queue (§2.4) — are **not** sized from a config field. Their capacity is a fixed
internal constant (`1024`) set in `Pipeline::new`. `queue_size` is only a
test/bench parameter (`Pipeline::with_sizes`), not a `LedgerConfig` knob. The
record-stream ring is sized separately (`ring_size`, §15.10), which *is* a real
field.

### §15.3 `wait_strategy`

Pipeline-mode for every stage's idle/backpressure loop (§13.2). One
value is shared across every stage of a single Pipeline.

### §15.4 `log_level`

Not exposed via `config.toml`; set programmatically. The temp and
bench config presets both lower this to keep test output clean.

### §15.5 `transaction_count_per_segment` (drives multiple subsystems)

Set on `StorageConfig`. Controls three things at once:

- WAL segment rotation threshold (§7.6).
- Dedup active window size (§5.2).
- Hot index circle sizing (§9.6).

These are deliberately driven by one knob: changing the active window
should not desynchronise any of the three.

### §15.6 `seal_check_internal`

Interval between Seal polls (§11.2). Not exposed via `config.toml`.
The Committer (§7.7) does not use this; it is purely the Seal
thread's poll interval and is not on the hot path.

### §15.7 `disable_seal`

When `true`, the Seal thread is not started (§11.8). Not exposed via
`config.toml`. Benchmark-only: a ledger with `disable_seal = true` is
not crash-recoverable past the active segment.

### §15.8 `snapshot_frequency`

Set on `StorageConfig`. Controls the Seal stage's snapshot emission
rule (§11.5): emit when `segment_id % snapshot_frequency == 0`.
Smaller values mean less WAL replay on recovery (faster startup) at
the cost of more snapshot I/O during operation.

### §15.9 Hot-index sizes are derived

Hot-index sizes (§9.6) are derived from `transaction_count_per_segment`
(§15.5) and the current account-array length (§15.1). There are no separate
knobs; changing the active window changes both the hot indexes and the dedup
window in lockstep.

### §15.10 `ring_size`

Capacity of the transaction ring (§13.4). A power of two; not exposed via
`config.toml`, an internal tuning knob. Because a transaction
is published atomically and assembled in place, the ring must be at least as
large as the biggest possible single transaction (`sub_item_count ≤ u16::MAX`
followers plus the trailing metadata); otherwise the Transactor cannot make
progress.

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
storage honours its durability contract. The Snapshotter's durability gate
(§7.5, §8.5) makes this true on the read side as well: it never applies a
transaction whose trailing `TxMetadata.tx_id` exceeds `commit_index`, so a
transaction that was visible to a reader before the crash is by definition
committed and is replayed on restart.

---

# Stage 2 — The Storage

The previous sections describe the in-memory engine. This stage describes
the on-disk shape: how records are laid out byte by byte, how files are
named, how the writer and reader cooperate to keep the disk consistent
across a crash, and what the `ctl` tooling can read and write.

---

## §17 Storage layout and segment lifecycle

### §17.1 The data directory is a flat namespace

A ledger owns one data directory. Every file belonging to that ledger
lives directly in the directory; the only subdirectory is `functions/`
(§21.1). The names are `wal.bin`, `wal_NNNNNN.bin`, `wal_NNNNNN.crc`,
`wal_NNNNNN.seal`, `wal_index_NNNNNN.bin`, `account_index_NNNNNN.bin`,
`snapshot_NNNNNN.bin`, `snapshot_NNNNNN.crc`,
`function_snapshot_NNNNNN.bin`, `function_snapshot_NNNNNN.crc`,
`term.log`, `vote.log`, and `wal.stop`. The flat layout means recovery
can enumerate every artifact in one syscall and partition the results
purely by name parse.

### §17.2 Segment file naming

The active WAL segment is always `wal.bin`. Sealed segments are
`wal_NNNNNN.bin` where `NNNNNN` is the segment id zero-padded to six
decimal digits. The pad width is a hard limit; segment ids beyond
999_999 are unsupported by the on-disk naming. At the default
`transaction_count_per_segment = 10_000_000` (§15.5) that bound is
~10^13 transactions per ledger, comfortably beyond any expected scale.
Index, snapshot, sidecar, and seal-marker files reuse the same
six-digit segment id so a single text match identifies every artifact
for a given segment.

### §17.3 Three lifecycle states

A segment is in exactly one of three states: **Active** — it is
`wal.bin`, the writer is appending records to it; **Closed** — it has
been renamed to `wal_NNNNNN.bin` and `fdatasync`ed, but the seal marker
and CRC sidecar are not yet present; **Sealed** — the per-segment seal
procedure (§11.3) has computed the file CRC, written the `.crc`
sidecar, and written the empty `.seal` marker. Cold-tier index lookups
(§22) require the Sealed state; recovery (§12.2) re-seals any
Closed-but-not-Sealed segment it finds.

### §17.4 Rotation is rename-then-open

When the WAL Writer crosses the rotation threshold (§7.6), it performs
a synchronous `fdatasync` on `wal.bin`, then renames `wal.bin` to
`wal_NNNNNN.bin` and opens a fresh `wal.bin` for the next segment. The
rename is atomic on every supported filesystem. A reader that opened
`wal.bin` before the rename retains its handle to the now-renamed
inode; a reader that opens after the rename gets the new file. This is
the foundation of the inode-based rotation detection used by
replication (§23.3).

### §17.5 The `wal.stop` clean-shutdown marker

A clean `Ledger::shutdown` writes an empty `wal.stop` file before
exiting. On startup, recovery uses the marker to decide what to trust:
both `wal.stop` and `wal.bin` present means the previous shutdown was
clean and the active segment is fully trustworthy; only `wal.bin`
present means the process exited without flushing and the active
segment must be tail-trimmed (§23.4). The marker is removed at the
start of every `Ledger::start` so a crashed restart cannot mistake a
stale marker for clean state.

### §17.6 Inode rotation detection

Both the leader-side replication tailer (§23.3) and any external reader
of the active segment detect rotation by inode comparison: at open time
they stash the inode of `wal.bin`; on every read after EOF they compare
the current on-disk inode with the stashed one. A mismatch means the
active segment was renamed and the reader must advance to the next
segment. The pattern works because the rename in §17.4 produces a new
inode for the new `wal.bin`; the old inode lives on as long as anyone
holds a handle to it. [ADR-0006, ADR-0013]

---

## §18 WAL record byte layout

### §18.1 The 40-byte invariant

Every WAL record is exactly 40 bytes. This is the same invariant stated
in §1.6 from the in-memory side; this section states the byte-level
consequences. The first byte is the record's `entry_type` discriminant;
the remaining 39 bytes are the record's payload. All multi-byte fields
are little-endian. Records are dense in the file: there is no
inter-record padding, no length prefix, no framing layer. A reader can
scan a segment by stepping 40 bytes at a time and dispatching on
byte 0.

### §18.2 Zero-copy serialization

The in-memory layout matches the on-disk layout exactly. Records are
defined as `#[repr(C)]` plain-old-data structs with explicit `_pad`
fields filling unused bytes; the bytes that go to disk are obtained by
casting the struct reference to a byte slice via `bytemuck::bytes_of`.
There is no per-field encode step, no allocator pressure, no
conversion between in-memory and on-disk representation. The same
pattern is used on the read side: a 40-byte slice is cast back to the
appropriate struct once the discriminant has been examined.

### §18.3 CRC envelope and torn-write detection

A transaction's CRC lives in its trailing `TxMetadata.crc32c` (§18.1) and covers
the followers (`TxEntry` / `TxLink`) in stream order followed by the metadata
record with that field zeroed for the digest. The CRC field sitting inside its
own coverage forces the zero-while-computing convention. Because the metadata is
the commit record, a transaction is complete only when its trailing metadata is
present and its CRC matches; a torn write inside a transaction is caught when the
metadata is missing or its CRC fails — there is no per-record CRC, and none is
needed. `FunctionRegistered` carries no `tx_id` and is outside any transaction's
CRC envelope; the `crc32c == 0` value is the unregister signal (§6.17).
[ADR-0001, ADR-0014, ADR-0020]

---

## §19 Sidecar files

### §19.1 The CRC sidecar

Every Sealed segment has a paired `wal_NNNNNN.crc` file carrying the
segment's full-file CRC, the segment size at seal time, and a magic
constant. The sidecar is written by the per-segment seal procedure
(§11.3, §25.5) after the segment file itself has been closed. The
size field catches truncation that does not change the CRC of the
surviving prefix; the magic disambiguates a `.crc` against any
unrelated file of the same width that might end up in the directory.

### §19.2 The `.seal` marker

Alongside the `.crc` sidecar, Seal writes an empty `wal_NNNNNN.seal`
file. Presence of the marker is what distinguishes a Sealed segment
from a Closed-but-not-yet-sealed one: a crash between the WAL rename
and the seal procedure leaves a Closed segment that recovery (§12.2)
re-seals on the next startup. The marker file carries no content; only
its existence matters. An empty marker file is cheaper than a flag
inside the sidecar because its presence can be detected by a stat call
alone.

### §19.3 The `wal.stop` clean-shutdown marker

`wal.stop` is the symmetric marker for the active segment: present iff
the last `Ledger::shutdown` call completed cleanly (§17.5). It is the
one file in the directory whose *absence* is informative. Recovery
uses its absence to decide whether the active segment needs
tail-trimming (§23.4) versus full-trust load.

### §19.4 Missing sidecars are tolerated

A Sealed segment that is missing its `.crc` sidecar is loaded with a
warning, not a failure. The rationale: the sidecar is a redundant
check; the segment's own per-transaction CRCs (§18.3) are still
authoritative. Missing sidecars are most commonly produced by a crash
after the segment was renamed but before Seal ran, or by external tools
that copied a segment without copying its sidecar. Neither case
warrants a refusal to start. [ADR-0006]

---

## §20 Snapshot files

### §20.1 Naming and pairing

Every snapshot is a pair: `snapshot_NNNNNN.bin` plus
`snapshot_NNNNNN.crc`. The function snapshot equivalent uses
`function_snapshot_NNNNNN.bin` plus `function_snapshot_NNNNNN.crc`.
The six-digit suffix is the segment id of the segment whose sealing
triggered the snapshot (§11.5); the snapshot reflects the state
through the end of that segment. A pair is the unit of trust: a
snapshot whose `.crc` is missing is not loaded, even if the `.bin`
parses cleanly.

### §20.2 Double-CRC discipline

A balance snapshot file carries two independent CRCs: an inline
`data_crc32c` in the header validates the records independent of the
compression layer, and the sidecar CRC (§20.5) validates the written
file as a whole. The first catches corruption introduced by a buggy
decompressor; the second catches truncation or bit-rot after the
rename.

### §20.3 Body sort and non-zero filter

The body is sorted by `account_id` so the same set of balances always
produces the same bytes — reproducible across restarts and amenable
to offline diffing. Only non-zero balances are emitted; account ids
that have never been touched are omitted.

### §20.4 Snapshot atomic write

Snapshots are written atomically with the `.tmp` rename pattern: write
the body to `snapshot_NNNNNN.bin.tmp`, `fsync` it, write the `.crc` to
`snapshot_NNNNNN.crc.tmp`, `fsync` it, then rename both to their final
names. If the process crashes between any two steps the partial work
is identifiable by the `.tmp` suffix and is discarded on the next
startup. The two renames are independent — a snapshot is not
considered loaded until *both* finals exist (§20.1) — so the order of
the two renames does not matter for correctness.

### §20.5 The snapshot CRC sidecar

`snapshot_NNNNNN.crc` is the same 16-byte format as the WAL CRC
sidecar (§19.1) but with a different magic
(`SNAPSHOT_MAGIC = 0x534E4150`). The fields cover the entire
`snapshot_NNNNNN.bin` file as written, so a truncation or bit-flip
after the rename is detected on the next load.

### §20.6 Function snapshot

`function_snapshot_NNNNNN.bin` mirrors the balance snapshot's shape: a
header (`FUNCTION_SNAPSHOT_MAGIC = 0x46554E43` — `"FUNC"`), an
LZ4-compressed body, a paired `.crc` sidecar. The body is a
sorted-by-name sequence of `(name, version, crc32c)` triples — the
registry's complete state at the time of the snapshot, including names
whose latest record is an unregister (`crc32c == 0`). Recording
unregisters in the snapshot is what lets recovery jump straight to the
latest snapshot boundary instead of replaying from segment 1 (§12.3).

### §20.7 Empty function snapshots are still written

Even when the registry is empty, the function snapshot is emitted at
every snapshot boundary. The reason is symmetry with the balance
snapshot: recovery always loads the *paired* snapshot for a given
segment id, and the absence of a function snapshot would otherwise
require a special "function snapshot may legitimately be missing" code
path. An empty file is cheaper than a special case. [ADR-0014]

---

## §21 Function binary storage

### §21.1 The `functions/` subdirectory

WASM binaries live in `{data_dir}/functions/`. This is the only
subdirectory of the data directory; it is created on first
registration. Each binary is a file named `{name}_v{N}.wasm` where
`name` is the ASCII snake_case identifier from the
`FunctionRegistered` record and `N` is the per-name monotonic
version starting at `1`. The directory is the secondary signal of the
registry's history; the WAL is authoritative.

### §21.2 Atomic write on registration

`RegisterFunction` writes the binary to `{name}_v{N}.wasm.tmp`,
fsyncs, then renames to the final `{name}_v{N}.wasm`. The rename is
atomic on every supported filesystem; readers therefore never observe
a partial file. Only after the rename is committed does the
registration emit its `FunctionRegistered` WAL record (§6.16); the
disk-then-WAL order means a crash at any point leaves either no record
of the registration, or both the file and the WAL record in their
final states.

### §21.3 Unregister produces a 0-byte file

Unregistration writes a 0-byte `{name}_v{N}.wasm` (where `N` is the
*next* version after the last registered one) and emits a
`FunctionRegistered` record with `crc32c == 0` (§6.17). The 0-byte
file is the on-disk audit trail; the WAL record is the authoritative
signal. Past versions of the binary are preserved; the on-disk history
of a function is therefore the directory listing of `{name}_v*.wasm`
files in version order. [ADR-0014]

---

## §22 On-disk indexes

### §22.1 Per-segment, written at seal

Each Sealed segment has a paired transaction index
(`wal_index_NNNNNN.bin`) and account index
(`account_index_NNNNNN.bin`), both built once at seal time (§11.3)
and never modified afterwards. The hot tier (§9) covers the active
window; the on-disk indexes cover everything older. A query that
misses in the hot tier (§10.4) consults the on-disk indexes for the
segment that contains the requested `tx_id` or account history. The
boundary between hot and cold is not bytes-on-disk — it is *eviction
in the hot tier*.

### §22.2 Query path

A cold-tier `GetTransaction` binary-searches the per-segment
transaction index on `tx_id` and issues a positional read into the
segment file to fetch the metadata plus its dependent entries and
links. A cold-tier `GetAccountHistory` partition-points the per-segment
account index on `account_id`, then iterates the `tx_id` range
forward until the lower bound or limit is hit. Iteration order is
ascending in `tx_id`; the caller reverses for newest-first.

### §22.4 Why flat sorted arrays

The format has no per-record framing, no inline pointers, no tree
metadata. The cost of binary search is `O(log N)` with predictable
cache behaviour; the cost of construction is one `sort_unstable` over
the segment's records. The trade-off compared to a B-tree or hash
index is that updates require a full rewrite — but indexes are
written exactly once per segment and never updated, so the trade-off
favours the simpler format. [ADR-0008]

---

## §23 WAL-tail reader and runtime truncation

### §23.1 `WalTailer` is a stateful cursor

The leader-side replication path (§31) reads raw WAL bytes via a
stateful cursor that holds an open file handle and a byte position
across calls. Reads use positional I/O (`pread`), so concurrent reads
do not interfere with the writer's append position. The cursor
amortises the cost of opening the file: once seeded, every subsequent
call pays only for the new bytes since the previous call's position.

### §23.2 Whole-record reads only

The tailer always returns a multiple of 40 bytes (§18.1). When the
underlying file ends mid-record — a torn write or a write still in
progress — the tailer rounds the byte count down and stops at the
last full record boundary. The partial bytes are not lost; the next
call resumes from the same position and re-reads them along with
whatever new bytes have arrived. The replication path therefore never
sees a partial record on the wire.

### §23.3 Inode-based rotation detection

When the tailer opens `wal.bin`, it stashes the inode of the file. On
EOF, it compares the stashed inode against the inode currently bound
to the path: a mismatch means rotation happened (§17.4) and the
tailer advances to the next segment file (`wal_NNNNNN.bin` if the
rotation has fully completed, the new `wal.bin` once the next segment
starts taking writes). The detection is cheap — one `stat` call per
EOF — and avoids any coordination with the writer.

### §23.4 Tail-trim on cold-boot recovery

On startup, recovery (§12) inspects the active segment for a torn tail. The
procedure: forward-scan the segment, buffering each transaction's followers
until a `TxMetadata` closes and validates the group (buffered follower count
equals `sub_item_count`, CRC matches). The valid region must end exactly on a
metadata boundary; if the scan ends with buffered followers and no closing
metadata, or on a torn sub-40-byte record, the file is truncated back to the
last metadata boundary. This is the *one* tolerated case of corruption (§12.7);
a torn middle is fatal. The trim is performed via `set_len` followed by an
`fsync_data`, so the post-recovery file matches what is on disk. [ADR-0020]

### §23.5 Runtime truncation classifies segments by watermark

Divergence-driven runtime truncation (§33) classifies each segment by
its range of `tx_id`s relative to the recovery watermark. A segment
whose last `tx_id` is at or below the watermark is kept untouched. A
segment whose first `tx_id` is strictly above the watermark is removed
entirely — `wal_NNNNNN.bin`, the `.crc` and `.seal` sidecars, the
transaction and account indexes, the balance snapshot and its sidecar,
the function snapshot and its sidecar — every artifact for that
segment id. A segment that straddles the watermark is byte-truncated immediately after the
trailing `TxMetadata` of the highest transaction whose `tx_id ≤ watermark` — so
the followers of the first over-watermark transaction are dropped along with its
metadata, leaving the surviving prefix ending exactly on a metadata boundary.
The surviving prefix is treated as Closed-but-not-yet-Sealed and resealed on the
next start.

### §23.6 Sealed segments are immutable

A Sealed segment is never modified in place. The straddle case
(§23.5) can only land in the active segment or in a segment that was
Closed but not yet Sealed at the moment recovery began. The watermark
is gated at the storage layer to prevent landing inside a Sealed
segment; the higher layer (§33) is responsible for choosing a
watermark that respects this. A request to truncate a Sealed segment
is a programming error and panics rather than corrupt the on-disk
audit trail. [ADR-0006, ADR-0016 §9, §10]

---

## §24 Term and vote logs

### §24.1 Two files, one purpose each

Cluster Raft state is persisted as two append-only files in the data
directory: `term.log` and `vote.log`. They are storage-layer artifacts
because their durability rules and on-disk format are storage's
responsibility, even though their semantic meaning belongs to the
cluster (§28). Keeping them as separate files means the hot path that
queries the current term (§36.4) does not pay the cost of pollution
from a vote-grant write.

### §24.2 Records share the WAL's 40-byte width

Term and vote records are exactly 40 bytes — the same width as a WAL
record (§18.1) — so the same offline tooling can inspect them. Each
file has its own magic constant (`"TERM"` / `"VOTE"`) so a misrouted
record is rejected on read.

### §24.3 Append, then `fdatasync`, then publish

The writer takes a per-file mutex, appends the record, calls
`sync_data`, and only then publishes the corresponding atomic value
(`current_term`, `voted_for`). Readers observe the atomic with
`Acquire` ordering. The discipline enforces the same "atomic value
observed ⇒ record on disk" rule that the WAL stage enforces for its
own commit index (§7.5). A crash between append and publish is
harmless: the next restart's scan re-derives the atomic from the last
record on disk.

### §24.4 Crash-tolerant scan

Both files are read by walking from the head and parsing one 40-byte
record at a time. The reader stops cleanly on EOF or on the first
record whose magic mismatches or whose CRC fails — it does not throw.
A torn write to either file is therefore tolerated: the surviving
prefix is the authoritative state. The append path's `sync_data`
ordering means a record observed by the scanner was on disk before
the writer published its atomic value; no committed term or vote is
ever lost to a crash.

### §24.5 No truncation, no rotation

The two logs grow without bound. In practice the term log adds one
record per election and the vote log adds one record per granted
vote; both are negligible in steady state. There is no rotation, no
compaction, no truncation. A future bound (e.g. truncating the term
log to records covering only the active window) is not part of the
current design.

---

## §25 ctl tools

### §25.1 The `roda_ctl` binary

`roda_ctl` is the offline tooling for inspecting and reconstructing
WAL segments. It is *not* a control plane: it does not connect to a
running ledger, does not synchronise with the running writer, and does
not modify any in-memory state. It operates purely on files in the
data directory and assumes no other process holds a write handle to
the artifacts it touches. Four subcommands exist: `unpack`, `pack`,
`verify`, and `seal`.

### §25.2 `unpack`

`roda_ctl unpack SEGMENT [--out FILE] [--ignore_crc]` reads a WAL
segment binary and emits one JSON object per record to standard
output (or `--out FILE`). The JSON shape is one-to-one with the
record's fields; CRC errors are surfaced as a `"crc_error": true`
field on the offending record rather than aborting the dump, unless
`--ignore_crc` is used to skip the check entirely. `unpack` is the
diagnostic tool of record: any question about what is in a segment is
answered by piping its output to `jq`.

### §25.3 `pack`

`roda_ctl pack INPUT [--out FILE] [--no_validate]` is the inverse of
`unpack`: it parses NDJSON from `INPUT` (or stdin) and writes a
binary segment to `--out` (or stdout). It re-encodes each record
using the zero-copy serialization (§18.2) and recomputes each transaction's
CRC32C over its followers (in order) then the zeroed-crc trailing `TxMetadata`
(§18.3). By default it validates that `tx_id` values are monotonically increasing.
`--no_validate` disables the check for cases where a deliberately
partial or malformed segment is needed (typically for testing
recovery).

### §25.4 `verify`

`roda_ctl verify DIR [--segment ID | --range FROM..TO]` runs an
integrity audit over a data directory or a subset of its segments.
Per segment it checks: the file-level CRC against the `.crc` sidecar;
the per-transaction CRC; the cumulative `computed_balance` chain (no
unexplained jumps); the declared `sub_item_count` matches the actual follower
count. Across segments it checks that sealed segments
form a contiguous `tx_id` chain with no gaps and no overlaps. The
output is a structured report; a non-zero exit code signals at least
one failure.

### §25.5 `seal`

`roda_ctl seal SEGMENT [--force]` finalises a Closed segment to
Sealed state by computing the file-level CRC32C, writing the
`wal_NNNNNN.crc` sidecar, and writing the empty `wal_NNNNNN.seal`
marker. It refuses to operate on Active or already-Sealed segments
unless `--force` is given. The operation is idempotent under
`--force`: the sidecar and marker are simply rewritten. `seal` exists
primarily for recovery scenarios where the running ledger crashed
after a rotation but before its Seal thread (§11) had a chance to
finish. [ADR-0007]

---

# Stage 3 — The Cluster

The previous stages describe a single-node ledger and its on-disk
artifacts. This stage describes the network layer that wraps the
ledger to provide replication and high availability. The cluster
module owns both gRPC services — the client-facing Ledger service and
the peer-facing Node service — and the Raft state machine that drives
leader election and log replication. The Ledger itself remains
unchanged except for a small, narrow contract surface (§26.2).

---

## §26 The cluster layer

### §26.1 Single node is a cluster with zero peers

There is one binary and one configuration shape for both single-node
and multi-node deployments. A "single node" is a cluster whose peer
list contains only itself; a multi-node cluster has additional peers.
There is no separate single-node binary, no feature flag for gRPC,
and no distinct configuration path. The simplification is deliberate:
every observable behaviour is exercised by the same code path that
runs in production, and operational tooling does not need to branch
by mode. [ADR-0015 decisions 1, 2]

### §26.2 The minimal Ledger contract

The cluster module never reaches into the Ledger's internals. It uses
exactly four entry points beyond the standard query and submit
surface:

- `append_wal_entries(Vec<WalEntry>)` — the follower-side write path
  that bypasses the Transactor and feeds pre-validated bytes directly
  to the WAL stage.
- `wal_tailer()` — produces a stateful raw-WAL byte cursor (§23.1)
  used by the leader to ship bytes to followers and by the Snapshotter
  to tail the durable WAL (§8.4).
- `set_index_hook()` — registers the single `IndexHook` (§14.2) the cluster
  uses to drive its reactive index watches (compute/commit/snapshot), which in
  turn feed the cluster-commit driver (§30.3) and the client wait paths.
- `start_with_recovery_until(watermark)` — an alternative to `start()`
  invoked only by the runtime divergence path (§33).

Nothing else changes inside the Ledger. A standalone Ledger ignores
all four. [ADR-0017, ADR-0027, ADR-0016 §10]

### §26.3 Two gRPC surfaces

The cluster runs two distinct gRPC services on two distinct ports.
The **Ledger service** (`proto/ledger.proto`) is the client-facing
API: submit, query, status, wait. The **Node service**
(`proto/node.proto`) is the peer-facing API: append entries, request
vote, ping, install snapshot. The two services are entirely separate;
a client never speaks the Node service and a peer never speaks the
Ledger service. Both servers are spawned by the supervisor and survive
every role transition (§38.2) — only their *posture* (writable vs
read-only) changes when the node's role transitions.

### §26.4 Naming follows role, not transport

Within the cluster module, types are named by their role (`Leader`,
`Follower`, `LedgerHandler`, `NodeHandler`, `Server`,
`NodeServerRuntime`) rather than by their transport. gRPC is an
implementation detail; the same role surface could in principle be
served by another transport without renaming the role types.
[ADR-0015 decision 6]

---

## §27 Roles and the runtime state machine

### §27.1 Four roles encoded in one atomic

A node is in exactly one of four roles at any time: `Initializing`
(post-boot or post-teardown, awaiting a signal), `Candidate` (running
an election round), `Leader` (writable), or `Follower` (read-only,
receiving the replication stream, §35.2). The role is published through a
`tokio::sync::watch` channel that tasks subscribe to (`role_subscribe`); reads
of the underlying atomic use `Acquire`, writes use `Release`. The role is shared
by every gRPC handler so the writability check on every submit RPC is one
cache-line read.

### §27.2 The supervisor is the sole writer

The role supervisor is the only entity that mutates the role atomic.
Handlers, replication tasks, and ledger threads all read the atomic
freely but never write it. The single-writer rule means a transitioning
role cannot leave handlers observing a half-applied state: the
supervisor publishes the new role only after the old role's tasks
have been torn down and the new role's tasks have been brought up.

### §27.3 Cancel-and-respawn on every transition

There is no `LeaderHandles`/`FollowerHandles` struct. Role-specific work — the
leader's per-peer pusher tasks — is governed by `CancellationToken`s, not an
RAII bundle. The long-lived `replication_push_loop` (§31.1) watches the role
channel: on `→ Leader` it spawns the peer pushers under a child token; on
`Leader →` anything it cancels that token and joins the tasks. There is no
"modify role in place" path; a node that goes Leader → Follower → Leader has
fully re-spawned its peer tasks, re-seeded each peer's match index from a fresh
handshake (§30.4), and flipped the handler's writable posture each time. The
discipline is what eliminates stale-state bugs during failover.

### §27.4 What survives a transition

The durable, node-lifetime state is deliberately carried across role
boundaries: the `Arc<Ledger>` slot (so the node's data does not have to be
recovered on every transition), the durable `term.log` and `vote.log` (so each
keeps a single owner), the shared `Waiter` (§14.2), and the `RaftNode` itself
(its quorum/cluster-commit state, §30). Only the cancellable per-role tasks die
on a transition. The one exception to even the Ledger surviving is divergence
(§33), in which case the slot's Arc is reseeded via `start_with_recovery_until`.

### §27.5 `Initializing` is the post-boot and post-teardown state

`Initializing` is the role a node is in immediately after the
supervisor starts (before the first election timer fires) and
immediately after a teardown (before the next role's bring-up). The Node gRPC
server is already running so the node can participate in elections and accept a
replication stream (§35.2), but neither the writable client handler nor any
peer-pusher task is up. The
client-facing handler in this state behaves like a follower's
read-only handler: queries succeed; submits return
`FAILED_PRECONDITION`. [ADR-0016 §2, §3]

---

## §28 Term and vote durability

### §28.1 `term.log` and `vote.log` separate the hot from the cold

The two durable Raft state files (§24) are kept distinct precisely
because their access patterns are different. `term.log` is on the
`GetStatus` hot path: every status query reads the current term. It
grows by one record per term boundary, which is rare in steady state.
`vote.log` is purely cold: it is written by the `RequestVote` handler
when a vote is granted, and it is read only at boot to seed the
in-memory `(current_term, voted_for)` pair. Mixing them in one log
would force the hot path to scan past vote records.

### §28.2 Append-then-publish ordering

A term bump goes: take the term mutex, append the `TermRecord`,
`sync_data`, publish the new term to the in-memory atomic with
`Release`, drop the mutex. A vote grant goes: take the vote mutex,
append the `VoteRecord`, `sync_data`, publish `voted_for` with
`Release`, drop the mutex. A reader that observes either atomic via
`Acquire` is therefore guaranteed the on-disk record exists. This is
the same discipline used for the WAL's own commit index (§7.5) and is
what makes "committed term" meaningful across a crash.

### §28.3 Monotonic term

Both `Term::observe(term, start_tx_id)` and `Vote::observe_term(term)`
reject regressions: a request with a term smaller than the current
term is a no-op. Term values are monotonic across the entire cluster's
lifetime; `observe` is the path by which a follower or candidate
adopts a higher term seen on an incoming RPC. A peer that receives a
higher term clears its `voted_for` (since `voted_for` is per-term)
before publishing the new term.

### §28.4 The term hot ring

In-memory, `Term` keeps a bounded ring (capacity 10_000) of recent
records to serve `GetStatus`-style queries without disk I/O. A query
for a `tx_id` whose covering term is in the ring returns directly; a
miss falls through to a mutex-locked disk scan. The ring is sized so
that under normal failover frequencies every term boundary in the
recent past is in memory; clusters with extreme term churn fall back
to the cold path more often but still see correct answers.
[ADR-0016 §4, §11]

---

## §29 Election protocol

### §29.1 Randomised election timer

Every node not in `Leader` role runs an election timer with a
randomised deadline in `[election_timer_min_ms,
election_timer_max_ms]`. The deadline is re-randomised on every
reset. Reset triggers: any valid leader activity on the replication stream
(`WalUpdate` or `Heartbeat`, §35.2) noted via `note_leader_activity`, and
the granting of a `RequestVote`. Without a reset, the timer fires and
the node transitions to `Candidate`. The randomisation defeats split
votes: two nodes that timed out at exactly the same instant would
collide otherwise.

### §29.2 Candidate round

A candidate round consists of: durably bumping the term via
`Term::new_term(last_commit_id)`, durably self-voting via
`Vote::vote(new_term, self_id)`, fanning out `RequestVote` to every
other peer in parallel, and tallying responses. The outcome is one of
three values: **Won** — at least majority granted, transition to
`Leader`; **HigherTermSeen** — any peer reported a term higher than
ours, transition back to `Initializing` after observing the new term;
**Lost** — the deadline expired with neither outcome, transition back
to `Initializing` and re-arm the timer with a new randomised
deadline.

### §29.3 Majority is `(node_count / 2) + 1`

Majority counts the candidate itself: a 5-node cluster needs 3 votes
(of which 1 is the self-vote, so 2 peer grants suffice). A single-node
cluster trivially wins by self-vote with no RPCs sent. The arithmetic
matches the RaftNode's cluster-commit majority (§30.1) but is computed
independently — the election's vote count is not the running quorum.

### §29.4 `RequestVote` grant rule

Implements Raft §5.4.1 verbatim — grant iff the request's term is at
least the current term, `voted_for` is unset or already this candidate
within the term, and the candidate's `(last_term, last_tx_id)` is
lexicographically at least ours. The log-up-to-date clause is what
enforces leader completeness: a candidate whose log lacks a
quorum-committed transaction cannot win. A granted vote fsyncs
`vote.log` *before* the RPC reply is sent (§28.2).

### §29.5 Pre-vote round

A candidate first runs a *pre-vote* round before bumping its term: it
asks peers whether they would grant a vote under the third clause of
§29.4 (log up-to-date check) without persisting `voted_for` or
bumping its own term. Only if pre-vote succeeds does the candidate
proceed to the real round. Pre-vote prevents a partitioned node from
forcing the rest of the cluster to step down on every reconnect —
without it, a node coming back online would unconditionally bump its
term high enough to depose the current leader.

### §29.6 Step-down on higher term

Any term observed in any direction (a `RequestVote`, a replication
handshake/frame, or even a `Ping`) that is higher than the node's current term
triggers an immediate step-down: `Term::observe` durably records the new term,
`voted_for` is cleared, and the node transitions to `Initializing`. The
role-change watch then cancels whatever per-role tasks were running — a leader's
peer pushers (§31.1, §31.6), a follower's session. The step-down is the
universal correctness anchor — no Raft node ever continues operating in an old
term once it has seen a higher one. [ADR-0016 §5, §6, §13]

---

## §30 Cluster-commit in the pure RaftNode

### §30.1 Quorum lives in the consensus state machine, not the cluster crate

There is no standalone `Quorum` slot-array in the cluster crate. Quorum and
cluster-commit are owned by the **pure `RaftNode`** in the `raft` crate
([ADR-0017]) — the consensus state machine has no async, no I/O, and no upward
dependency on `ledger`. The cluster layer (`Consensus`, `consensus/state.rs`)
holds the `RaftNode` behind a `Mutex` and only feeds it observations. The
majority size — `(node_count / 2) + 1` — is a property of the RaftNode's peer
set.

### §30.2 Two indexes: local and cluster

The RaftNode tracks a *local* index (how far this node's own WAL has
progressed) and a *cluster-commit* index (the highest `tx_id` a majority has
acknowledged). `advance_local_index(new_local)` raises the local index (and the
leader's own quorum contribution); `advance_cluster_index(new_cluster)` raises
the cluster-commit index, clamped to the local index so a node never claims to
have cluster-committed past what it holds. `cluster_commit_index()` is the
lock-free read the wait paths (§34) consult. The leader's per-peer match indexes
live in the RaftNode's `Replication` view, advanced by
`replication().peer(id).append_result(...)` as acknowledgements arrive.

### §30.3 The leader's self-progress is fed reactively, not by a callback

The leader's own contribution to quorum is fed **reactively off the
snapshot-index watch**, not by an `on_commit` hook. `run_cluster_commit_driver`
parks on the ledger's snapshot `IndexWatch` (§14.2) and, on every advance, calls
`self_advance` → `advance_local_index(ledger.last_snapshot_id())`, then
publishes the resulting `cluster_commit_index()` into the waiter's
`cluster_commit` watch. This is what advances cluster-commit for a singleton
(no peers) and keeps the leader's self-progress fresh without polling. It runs
for every role; on a follower `self_advance` is a no-op on quorum. The legacy
`on_commit` callback path is gone (§14.2).

### §30.4 Peer acknowledgements advance the leader's view

Each peer's match index advances when an `IndexUpdate` arrives from that
follower over the replication stream (§31): the leader applies
`append_result(Success { term, last_commit_id })` to the peer's `Replication`
slot and then calls `self_advance`, which recomputes `cluster_commit_index()`
and republishes it. A freshly-elected leader seeds each peer's match index from
the handshake response (`last_term_curr_tx_id`) rather than carrying stale
state forward, so quorum is always recomputed from a clean per-session baseline.
[ADR-0016 §3, ADR-0017]

---

## §31 Replication driver and per-peer streams

### §31.1 One driver, two long-lived loops

Replication is not "one RPC per heartbeat." `run_replication_driver` spawns two
long-lived loops that live for the node's whole lifetime, regardless of role:

- `replication_stream_loop` — the **follower** side: it accepts inbound
  `Replication` streams handed in by the gRPC handler (§35.2) and runs a
  follower session per stream.
- `replication_push_loop` — the **leader** side: it subscribes to role changes
  and, on becoming `Leader`, spawns one `run_peer_push` task per peer
  (`spawn_peer_pushers`); on losing leadership it cancels them. There is no
  `PeerReplication` struct and no `LeaderHandles` — task lifetime is governed
  by `CancellationToken`s, not an RAII handles bundle.

### §31.2 Each peer pusher's session

A peer pusher (`run_peer_push`) opens **one bidirectional `Replication` stream**
to the peer, sends a single `Handshake` frame, and waits for the
`HandshakeResponse`. On accept it seeds the peer's match index from the
response's `last_term_curr_tx_id` (§30.4) and runs a `run_peer_session`, which
splits into two parallel tasks over the *same* stream:

- a **sender** (`run_peer_sender`) owning a `WalTailer` pre-positioned just
  after the handshake anchor; and
- a **receiver** (`run_peer_receiver`) consuming the follower's `IndexUpdate`
  frames.

On a rejected handshake the pusher records the follower's reported
`last_term_curr_tx_id` as an anchor override and reconnects; transient
connect/stream failures back off and retry.

### §31.3 The sender loop (streaming, not request/reply)

The sender does not wait for a per-batch ack. Each pass it calls `tailer.tail`
into a fixed buffer: if bytes come back it sends a `WalUpdate` frame (the raw
WAL bytes plus the leader's current `cluster_commit_id`) and, while the buffer
keeps filling, stays tight (`yield_now`, no park) so leader→follower throughput
is not artificially capped; if the WAL is drained (partial buffer) or empty it
sends a `Heartbeat` frame carrying only `cluster_commit_id`, then parks. The
park is reactive: it waits on the `commit` `IndexWatch` to reach
`last_sent_tx_id + 1` (new durable bytes to ship) or on the raft heartbeat
interval, whichever fires first — never a fixed sleep while WAL is pending.

### §31.4 Acks are a separate, reactive stream

Acknowledgement is decoupled from sending. On the follower side the session
splits into a *receiver* (applies leader frames to the ledger, no inline ack)
and an *acker* (`run_follower_acker`) that parks on the follower's snapshot
`IndexWatch` and emits an `IndexUpdate(local_commit_id)` each time it advances —
reporting the follower's *true* post-fsync applied index, not a stale read taken
at enqueue. The leader's receiver feeds each `IndexUpdate` into the peer's match
index and calls `self_advance` (§30.4). Because applying never blocks on the ack
and the ack reflects real durability, quorum tracking is both live and accurate.

### §31.5 Idle heartbeats keep cluster-commit fresh

When the WAL is idle the sender still emits a `Heartbeat` every raft heartbeat
interval, carrying the leader's `cluster_commit_id`; a data `WalUpdate` resets
the timer (a data frame counts as the heartbeat, per Raft). Followers mirror the
carried `cluster_commit_id` via `advance_cluster_index` (clamped to their own
local commit). Transport errors tear down the stream and the pusher reconnects;
a heartbeat is observational, not a durability event.

### §31.6 Step-down on higher term

A higher term observed anywhere in the consensus state machine drives the node
out of `Leader`; the role-change watch then fires and `replication_push_loop`
cancels every peer pusher (§31.1). The write side is drained the same way it is
on any role transition (§27.3): in-flight client submits return the error
generated by the rebuilt handler. [ADR-0016 §3, ADR-0017]

---

## §32 Per-peer replication and consistency

### §32.1 Raft naming map

The Roda cluster module uses Raft's terminology where possible: the sender's
`WalTailer` cursor is `nextIndex`; the peer's match index in the RaftNode's
`Replication` view is `matchIndex` (§30.4). The `tx_id` stream is the Raft log;
the term boundary records in `term.log` (§28) supply the term for any historical
`tx_id`. This mapping is documented so a reader who knows Raft can navigate the
implementation without re-deriving the correspondence.

### §32.2 The consistency anchor lives in the handshake, once per stream

Roda does **not** carry a `prev_tx_id`/`prev_term` precondition on every
appended batch. The Log-Matching anchor is sent **once, in the `Handshake`
frame** that opens a replication stream (§35.2): the leader sets
`prev_log_tx_id = local_commit_index` and `prev_log_term = term_at_tx(that)`,
plus `leader_term`, `leader_term_first_tx_id`, and `leader_commit`. The follower
validates the anchor in `validate_handshake`; once accepted, every subsequent
`WalUpdate` on that stream is appended in order with no per-frame precondition.
If the leader's term changes, a fresh stream (and fresh handshake) is
established. This is the central difference from textbook AppendEntries:
consistency is a per-*session* check, not a per-*message* one.

### §32.3 The handshake decision

`validate_handshake` returns `Accept` or `Reject { reason, truncate_after }`.
Accept seeds the leader's match index from the response and lets the WAL stream
flow. Reject maps to a `RejectReason` on the `HandshakeResponse`: `TermBehind`
→ `REJECT_TERM_STALE` (leader term below the follower's), `LogMismatch` →
`REJECT_PREV_MISMATCH` (the anchor is not in the follower's log, or sits at a
different term — divergence, §33). On a `LogMismatch` carrying a
`truncate_after`, the follower reseeds its ledger to that watermark *before*
replying, so the leader's first `WalUpdate` lands on the truncated state.

### §32.4 The follower reports its anchor back

A rejected handshake is not a dead end: the `HandshakeResponse` carries the
follower's own `last_term`, `last_term_first_tx_id`, and `last_term_curr_tx_id`.
The leader's pusher records `last_term_curr_tx_id` as an anchor override and
reopens the stream from there (§31.2), walking back until the handshake is
accepted. The remaining `RejectReason` variants in the proto
(`REJECT_CRC_FAILED`, `REJECT_SEQUENCE_INVALID`, `REJECT_WAL_APPEND_FAILED`,
`REJECT_NOT_FOLLOWER`) are reserved for append-side and role failures.

### §32.5 Batch sizing is exact

WAL records are exactly 40 bytes (§18.1), so the leader's per-frame byte cap is
always a clean multiple of 40 — there is no padding, no partial record at the
end of a `WalUpdate`. The configured `append_entries_max_bytes` (still the knob
name) is rounded down to the nearest multiple of 40 at startup and sizes the
sender's tail buffer.

### §32.6 gRPC message-size override

Tonic's default decoding/encoding limit of 4 MiB would otherwise reject
`WalUpdate` frames sized at exactly `append_entries_max_bytes`. The Node server
and client both raise `max_decoding_message_size` and `max_encoding_message_size`
to `append_entries_max_bytes × 2 + 4 KiB` to cover both the WAL byte payload and
protobuf framing overhead with margin. [ADR-0016 §8, ADR-0017]

---

## §33 Log divergence and runtime ledger reseed

### §33.1 The live WAL is append-only *while running*

Truncation of the WAL is permitted, but only via a Ledger restart.
The running Ledger never modifies records that have already been
written. This rule preserves the in-memory pipeline's invariants —
the Snapshotter (§8) and the indexes (§9) assume a monotonically
growing WAL and would not survive in-place truncation. The runtime
divergence path (§33.3) achieves truncation by *restarting the
Ledger*, which lets the Recover path do the truncation safely against
a quiescent disk.

### §33.2 Divergence detection at the handshake

Divergence is detected when a replication **handshake** arrives whose anchor —
`prev_log_tx_id`/`prev_log_term` (§32.2) — the follower has on disk but at a
disagreeing term. `validate_handshake` returns `Reject { reason: LogMismatch,
truncate_after }`, where `truncate_after` is the highest `tx_id` the follower
may keep. This is a per-session decision, not a per-message one: there is no
`AppendEntries` carrying a precondition on each batch.

### §33.3 Reseed sequence (inline in the handshake handler)

The reseed happens **inline in `replication_follower_handshake`, before the
handshake response is sent** (so the leader's first `WalUpdate` lands on the
truncated state). On a `LogMismatch` carrying a `truncate_after`, the handler
calls `LedgerSlot::reseed(after)`, which builds a fresh
`Ledger::start_with_recovery_until(after)` and atomically swaps it into the slot
via `ArcSwap` (the old `Arc<Ledger>` drops once outstanding handlers release it,
§33.6). The follower then reports its post-reseed anchor in the handshake
response; the leader resumes streaming from there (§32.4). There is no separate
`FollowerHandles` to drop — the session simply runs against the reseeded slot.

### §33.4 `start_with_recovery_until` semantics

`start_with_recovery_until(watermark)` is the alternative entry point
to `Ledger::start`. It walks the WAL and replays snapshots up to and
including `watermark`, physically truncates any WAL content above the
watermark (§23.5), and skips any snapshot whose covered-up-to-tx
exceeds the watermark — recovery falls back to an earlier snapshot
or to genesis. Cold boot (first start of a process) always uses plain
`start()`; `start_with_recovery_until` is strictly a runtime response
to divergence and is never invoked at boot.

### §33.5 Balances rewind for free

Because balances are a derived state of the WAL (§1.2), rewinding the
WAL rewinds balances as a side-effect of `Recover` re-walking the
truncated log. There is no undo log, no in-place rollback of applied
entries, no staged balance layer. The simplification is what makes
divergence recovery tractable: the Ledger's recovery path already
knows how to reconstruct balances from a WAL prefix.

### §33.6 `LedgerSlot` is the indirection that lets handlers survive

A reseed swaps the Ledger out from under the gRPC handlers without
restarting the gRPC servers. The mechanism is `LedgerSlot` —
`Arc<ArcSwap<Ledger>>`. Every handler reads the current Arc on every call
(`slot.current()`) and never retains it across operations. The swap is
lock-free; the old Arc's strong count drops to zero once outstanding handlers
release it, joining the old Ledger's pipeline threads on drop, which never
blocks a gRPC request because handlers have already released their reference.
The single ledger `IndexHook` is registered through the slot so it survives the
swap (§26.2). [ADR-0016 §9, §10]

---

## §34 Cluster commit and the `cluster_commit` wait level

### §34.1 A fifth wait level

Stage 1 introduces four wait levels (§14.1). The cluster adds a fifth:
`cluster_commit`. It is reactive like the rest (§14.2): the waiter awaits the
**`commit`, `snapshot`, and `cluster_commit` `IndexWatch`es in turn** —
requiring the tx to be locally durable *and* locally queryable *and*
quorum-replicated, completing when the last of the three crosses `tx_id`. There
is no spin/poll on a quorum atomic; the `cluster_commit` watch is fed from the
RaftNode's `cluster_commit_index()` at every advance site
(`publish_cluster_commit`).

### §34.2 The leader feeds its own progress reactively

The leader's contribution to quorum is not a callback-driven slot write. The
snapshot-index driver (§30.3) calls `self_advance` on each snapshot advance,
raising the RaftNode's local index and republishing `cluster_commit_index()`;
each peer's `IndexUpdate` advances that peer's match index and triggers another
`self_advance` (§30.4). Both contributions are required for the cluster-commit
index to move — without the leader's own progress it would be stuck at the
slowest peer.

### §34.3 Local commit is independent of quorum

The leader's *own* `commit_index` (§2.5) advances from its local WAL stage as
soon as the local `fdatasync` returns, independent of any peer's progress. Only
the client's wait choice determines the guarantee: a client that picks `wal`
sees the local commit; a client that picks `cluster_commit` waits for quorum.
The two indexes coexist in the same pipeline; nothing about cluster mode slows
down the local commit path. [ADR-0017]

### §34.4 Followers mirror, but do not own, cluster commit

`cluster_commit_id` arrives on every `WalUpdate` and `Heartbeat` (§31.5). The
follower mirrors it via `advance_cluster_index`, clamped to its own local commit
index, so a follower can answer `cluster_commit`-level waits for transactions it
has both applied and seen acknowledged — but its view is derived from the
leader's stream, not authoritative on its own. The leader's
`cluster_commit_index()` remains the source of truth. A divergent handshake also
carries a `truncate_after` watermark used to reseed the follower (§32.3, §33.2).

---

## §35 gRPC: Node service

### §35.1 Service-level invariant

The Node service (`proto/node.proto`) is the peer-facing API. Every
handler in the service except `Ping` shares a single per-node
`tokio::sync::Mutex` so that updates to `(current_term, voted_for,
log)` appear atomic to peers. Tonic dispatches RPCs in parallel by
default; without the mutex, two concurrent `RequestVote` handlers
could race on `vote.log` and grant the same term to two candidates.
[ADR-0016 §7]

### §35.2 `Replication` (bidirectional stream)

There is no unary `AppendEntries` RPC. Peer replication is a single
**bidirectional streaming** RPC, `Replication(stream
ReplicationLeaderMessage) returns (stream ReplicationFollowerMessage)`. The
leader→follower stream carries one `Handshake` (the consistency anchor, §32.2)
followed by an open-ended sequence of `WalUpdate` (raw WAL bytes +
`cluster_commit_id`) and `Heartbeat` (`cluster_commit_id` only) frames. The
follower→leader stream carries one `HandshakeResponse` followed by `IndexUpdate`
frames reporting the follower's applied commit id (§31.4). The handler hands the
inbound stream to the replication driver (§31.1), which runs the per-session
loops; a term change opens a fresh stream rather than mutating an existing one.

### §35.3 `RequestVote`

Implements the grant rule of §29.4 verbatim and persists the vote via
`Vote::vote` before replying. A grant that requires a term observation
also durably bumps the term via `Vote::observe_term`.

### §35.4 `Ping`

Reads only atomics — no WAL, no vote log — and bypasses the per-node
mutex. Its purpose is operational: health checks, RTT measurement, and
leader discovery without the cost of the full Raft handler stack.

### §35.5 `InstallSnapshot`

The wire format mirrors Raft's `InstallSnapshot` and exists in the
proto to lock in the shape. The handler is scaffolded but not fully
implemented; followers that fall too far behind a leader's retention
window will need this RPC to catch up via a snapshot rather than via
WAL backfill. Currently returns `UNIMPLEMENTED`.

---

## §36 gRPC: Ledger service

### §36.1 Submit family

`SubmitOperation`, `SubmitAndWait`, `SubmitBatch`,
`SubmitBatchAndWait`. All four are gated on `role.is_leader()`; a
non-leader returns `FAILED_PRECONDITION` with the leader's id (the
node's best-known leader hint) carried in response metadata. Clients
hitting a non-leader can therefore redirect without a separate lookup
RPC. The wait variants accept any of the five wait levels (§14, §34)
and block until the threshold is crossed.

### §36.2 Query family

`GetTransaction`, `GetTransactionStatus`, `GetTransactionStatuses`,
`GetAccountHistory`, `GetBalance`, `GetBalances`. `GetBalance` and
`GetBalances` are direct atomic reads against the read-side balance
cache and never touch the snapshot query queue (§10.6); the rest enqueue
queries on the Snapshotter's query queue (§8.4). Reads are served on every
role including `Initializing` and `Follower`; only the freshness
signal (`last_tx_id`) tells the caller how stale the answer is.

### §36.3 `GetPipelineIndex`

Returns the five progress indexes — `compute`, `commit`, `snapshot`,
`term`, `cluster_commit` — in one call. Used by the `load_cluster`
benchmark and external monitoring to track replication lag and
pipeline depth. Cheap: every field is one atomic load.

### §36.4 `WaitForTransaction`

Request fields: `tx_id`, `wait_level`, optional `term` fence.
Response field: `outcome` enum with `REACHED`, `NOT_FOUND`,
`TERM_MISMATCH`. The optional `term` fence tells the server "I expect
to be talking to term N"; if the server's current term differs, the
response carries `TERM_MISMATCH` along with the actual term and
`term_start_tx_id` so the client can decide whether to redirect or
re-query.

### §36.5 Function registry RPCs

`RegisterFunction`, `UnregisterFunction`, `ListFunctions`. Writes are
gated on `role.is_leader()`; on a leader they go through the
Transactor like any other operation (§6.16, §6.17). `ListFunctions`
reads the live registry and can be served on any role.

### §36.6 Read-only mode on followers and `Initializing`

A follower's client handler is constructed in read-only mode; every
`submit_*` and `register_function` returns `FAILED_PRECONDITION`
unconditionally, regardless of leader-hint availability. The
`Initializing` role uses the same read-only handler. This is what
prevents a partitioned former leader from accepting writes that would
later have to be discarded. [ADR-0004, ADR-0015]

---

## §37 Client library

### §37.1 `LedgerClient` is a thin tonic wrapper

The client library is one type, `LedgerClient`, holding a tonic
`Channel` (cloneable; tonic pools internally). Construction is
`LedgerClient::connect(addr).await`. The library does not maintain
its own connection pool, retry policy, or leader-discovery cache —
applications wrap calls themselves and use response metadata (term,
leader hint) when steering.

### §37.2 One async method per RPC

Every gRPC RPC has a corresponding `async fn` on `LedgerClient` that
takes the natural Rust types as parameters and returns a Rust-side
response type (`SubmitResult`, `Balance`, `Transaction`,
`PipelineIndex`, `WaitOutcome`, `FunctionInfo`, …). The wrapper
hides protobuf message construction; callers never touch generated
types. The methods take `&self` so a single `LedgerClient` can be
cloned and shared across many concurrent callers — the underlying
tonic `Channel` handles multiplexing.

### §37.3 No built-in failover

A `LedgerClient` is bound to one address. Cluster-aware applications
construct one client per node, make the call against the suspected
leader, and on a `FAILED_PRECONDITION` with leader hint redirect to
the indicated peer. Implementing leader-hint following inside the
client library is deliberately deferred: the hint is on every error
metadata, but the application owns the policy (try once and fail vs.
retry on the hinted node vs. consult an external service).

---

## §38 Cluster startup, supervisor, shutdown

### §38.1 `ClusterNode::new` constructs the Ledger and the Term log

`ClusterNode::new(config)` constructs the in-process Ledger via
`Ledger::start()` — the cold-boot path, never the divergence path —
opens `Term`, durably bumps the term once on every restart so that
any in-flight election is forced to start from a higher term than was
observed pre-crash, and wraps the Ledger in `LedgerSlot` for later
indirection (§33.6).

### §38.2 Supervisor boot

The supervisor builds the long-lived shared resources: the `Consensus` wrapping
the pure `RaftNode` (which owns quorum/cluster-commit state, §30), the shared
`Waiter` (§14.2), and a node-wide `CancellationToken`. It then spawns the
client-facing gRPC server, the peer-facing gRPC server, the replication driver
(§31.1), the cluster-commit driver (§30.3), and the role-driver loop. The two
gRPC servers are bound for the entire lifetime of the process; they never
restart on a role transition.

### §38.3 Initial role

A single-node cluster (`peers.len() == 1` and the only entry is
`self`) is brought up with `initial_role = Leader` so the node is
immediately writable without waiting for an election timer. A
multi-node cluster starts in `Initializing` and waits for the first
election timer to expire before becoming a candidate.

### §38.4 The role-driver loop

The driver awaits the appropriate signal per role: an election-timer
expiry while in `Initializing` or `Follower`; the candidate round's
outcome while in `Candidate`; loss of leader contact while in `Leader`. Each
transition publishes the new role on the watch channel (§27.1); the long-lived
replication driver reacts by cancelling and respawning the per-role tasks
(§27.3, §31.1). The driver itself owns no role-specific task state; it is purely
the coordinator that decides which role to bring up next.

### §38.5 Shutdown ordering

Shutdown cancels the node-wide `CancellationToken`, which every spawned task
selects on (§31). Lifecycle is RAII: dropping `ClusterNode` triggers the cancel
and then *awaits* the tasks cooperatively rather than aborting them — the
replication driver tears down its child sessions, the drivers exit their select
loops, and the gRPC servers run their tonic graceful-shutdown protocol. No
`running: AtomicBool` flag and no abort-in-order step is involved.

---

## §39 Standalone, single-node, and testing modes

### §39.1 Standalone (no `[cluster]` config block)

When the configuration has no `[cluster]` section, `ClusterNode` does
not invoke the supervisor at all. It constructs a single client-facing
`Server` task with a hardcoded `Role::Leader` posture, no role watch,
no Node gRPC server, no `Term`, no `Vote`, no `RaftNode`, no peer tasks.
The deployment is identical in observable behaviour to a single-node
cluster but pays none of the supervisor's coordination cost. This is
the configuration `roda` ships with by default.

### §39.2 Single-node cluster (`peers.len() == 1`)

A cluster whose configuration lists exactly one peer (itself) runs
the full supervisor infrastructure: a role watch, a `Term`, a `Vote`, and the
`RaftNode` (with a peer-less quorum). The node boots as `Leader` (§38.3) and the
replication push loop has zero peers to fan out to; the cluster-commit driver
(§30.3) advances cluster-commit purely from the leader's own snapshot-index
advances, since there is no one else to wait for. This mode exercises the
cluster code path against a trivial cluster shape and is the one used by most
cluster integration tests.

### §39.3 Multi-node cluster

A cluster with two or more peers runs the full Raft state machine: a
node boots as `Initializing`, awaits the election timer, transitions
through `Candidate` to either `Leader` or back to `Initializing`,
runs `Leader` with peer replication tasks until it sees a higher term
or loses its majority, etc. This is the production deployment shape.

### §39.4 `ClusterTestingControl`

`ClusterTestingControl` is the in-process test harness for composing
N-node clusters in a single process: distinct ports on localhost,
temporary data directories per node, helpers to inject network
partitions, drop messages, kill nodes mid-flight, and wait for
specific cluster-wide states. It is not a production code path; it
exists to give the test suite, the `load_cluster` benchmark, and
manual debugging a uniform way to construct and tear down clusters
without spawning processes.
