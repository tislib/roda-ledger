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
serialization: the bytes on the wire are the bytes in memory.

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

The Pipeline is the sole owner of every inter-stage queue, every progress
index, the shutdown flag, and the shared wait strategy. Stages never own
this state directly — each receives a typed context that exposes only the
slice it may legally read or publish. The wrapping is purely for type-level
encapsulation; there is no runtime cost.

### §2.4 Inter-stage queues

The three inter-stage channels are lock-free, fixed-capacity,
single-producer / single-consumer queues:

- `sequencer → transactor`
- `transactor → wal`
- `wal → snapshot`

Capacity is fixed at construction from `queue_size` (§15.2). A full queue
is the only backpressure signal; producers spin/yield until space exists.
There is no other flow-control mechanism.

### §2.5 Global progress indexes

The pipeline holds five monotonic atomic counters and one shutdown flag:

- `sequencer_index` — next `tx_id` to be handed out.
- `compute_index` — last `tx_id` executed by the Transactor.
- `commit_index` — last `tx_id` durably written by the WAL.
- `snapshot_index` — last `tx_id` reflected in the Snapshotter.
- `seal_index` — last segment id sealed.
- `running` — global shutdown flag.

The indexes are padded to separate cache lines so that a publication on one
does not invalidate the cache line of an adjacent index. They are
strictly monotonic and never go backwards. The mapping
`commit_index ≥ N` ⇒ every transaction `≤ N` is committed (no committed
gaps) is what makes per-call wait levels (§14) meaningful: a single
threshold check on a single counter resolves any wait.

### §2.6 Backpressure and shutdown

Backpressure is implicit and propagates through queue saturation alone: a
slow Snapshotter fills the wal→snapshot queue, which stalls the WAL, which
fills the transactor→wal queue, which stalls the Transactor, which fills the
sequencer→transactor queue, which stalls `submit()`. There is no rate
limiter, leaky bucket, or admission control. Shutdown is the inverse signal:
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
`account_id`. Lookups and updates are O(1) array accesses with no hashing,
no probing, no resizing. The array is pre-allocated to `max_accounts` at
construction and never grows. [ADR-0002]

### §4.4 The `max_accounts` ceiling

Any operation referencing an `account_id ≥ max_accounts` is rejected with
`ACCOUNT_LIMIT_EXCEEDED` (§1.12) before the operation can mutate any state.
This is the price of the direct-indexed array layout (§4.3) — the
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

The Transactor accumulates every WAL record it produces during a step (a
step may execute many transactions in sequence) into a buffer, then flushes
the buffer as a single batch onto the transactor→wal queue. Batching
amortises the cost of queue handoff and gives the WAL stage a contiguous
slice to write.

### §4.7 The `position` marker

While accumulating entries, the Transactor tracks the start of the
*current* transaction's records in the buffer. Verify (§4.11), rollback
(§4.12), and CRC computation (§4.10) all scope their work to that
transaction's records only — they never re-walk earlier transactions in
the same batch.

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
else is mapped per §1.12 / §6.12. [ADR-0014]

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
operation (§4.12). The WAL queue therefore never sees a partial function
execution.

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
handler has been installed in the live WASM runtime. A subsequent
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

The Writer holds a bounded queue of WAL records that have been written
to the segment file but not yet forwarded to the Snapshotter. The
buffer is sized so the Writer can stay ahead of the Committer while it
syncs, and is sized in advance — never resized — so the WAL stage's
memory footprint is fixed.

### §7.4 Writer per-iteration loop

On each iteration the Writer does the following, in order:

1. Pop entries from the transactor→wal queue, append each to the active
   segment's pending-write buffer, and push it onto the local buffer.
   While popping, the per-record bookkeeping (§7.5) tracks which
   transactions are now complete.
2. Flush the segment's pending-write buffer (one syscall per
   iteration, not per record) and publish *last-written*.
3. If the transaction-count threshold is met, rotate the segment
   (§7.6).
4. Drain the local buffer toward the Snapshotter, but only entries
   whose `tx_id ≤ last-committed`. Entries whose transaction is still
   pending an `fdatasync` are left in the buffer and retried next
   iteration. As each entry is forwarded, `commit_index` (§2.5)
   advances.

### §7.5 Forwarding rule and per-record bookkeeping

Entries flow from the WAL to the Snapshotter only *after* they are
durable. This is the **durability rule**: the Snapshotter — and
therefore `get_balance`, queries, and the snapshot wait level — never
sees a transaction that is not yet on disk. Step 4 of §7.4 enforces
this by gating the drain on *last-committed*. The Writer also tracks
how many records of the current transaction it still expects (from
`TxMetadata.entry_count + link_count`) and keeps consuming the inbound
queue until that count reaches zero, even if buffer space is tight —
a partial transaction must never leak across a segment boundary.

### §7.6 Segment rotation

The Writer rotates segments on a transaction-count threshold: when the
number of transactions in the active segment reaches
`transaction_count_per_segment`. This is the same boundary that drives
dedup window flips (§5.2) and hot-index sizing (§9.6). Rotation appends
a `SegmentSealed` record, performs a *synchronous* `fdatasync` (the
Writer cannot defer this one to the Committer because the next step
closes the file), closes the segment, opens the next one, writes a
fresh `SegmentHeader` record, and hands the new sync handle to the
Committer. Rotation is a transient cost: the pipeline pauses only for
the synchronous sync and the open of the next file. [ADR-0013]

### §7.7 Committer loop

The Committer runs a tight loop driven by the shared wait strategy
(§13.2). On each iteration:

1. If *last-written* has not advanced past *last-committed*, there is
   nothing to sync; back off and continue.
2. Pick up a new sync handle from the Writer if rotation has happened.
3. Sample *last-written*, call `fdatasync`, publish that sample as
   *last-committed*.

The Committer never parses entries, never touches the Writer's buffer,
never inspects any queue. Its single responsibility is moving
*last-committed* forward as fast as the storage allows.

### §7.8 Backpressure on the outbound queue

A persistently slow Snapshotter fills the wal→snapshot queue, which
slows the Writer's drain (§7.4 step 4), which fills the Writer's local
buffer, which throttles intake from the inbound queue, which
back-pressures the Transactor (§2.6). The same mechanism that gives the
WAL its throughput is the one that protects it from a slow reader.

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

The read-side cache is a flat array of atomic balances, sized to
`max_accounts`. The Snapshotter writes; many readers can load
concurrently without locks. The array is pre-allocated at startup and
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

### §8.5 Apply algorithm

For each WAL entry the Snapshotter dispatches by record kind:

- `TxMetadata` — register the transaction in the hot index, remember
  how many records are still expected (`entry_count + link_count`),
  remember the current `tx_id`.
- `TxEntry` — write the entry into the hot index (updating circle2 and
  the per-account chain — §9.4) and publish the carried
  `computed_balance` to the read-side balance cache.
- `TxLink` — record the link against the current `tx_id` in the hot
  index.
- `SegmentHeader` / `SegmentSealed` — ignored.
- `FunctionRegistered` — handled per §8.7.

Once every record of a transaction has been applied, the Snapshotter
publishes that `tx_id` as the new `snapshot_index` (§2.5). This is the
*only* point at which the pipeline's read-side index advances — readers
therefore never observe a partially-applied transaction.

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
(§6.4). On an unregister record (`crc32c == 0`, §1.11), the Snapshotter
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

### §9.1 Three buffers, all pre-allocated

The hot transaction index is a single in-memory structure with three
pre-allocated buffers, sized at construction and never resized:

- **circle1** — for each `tx_id`, a pointer to that transaction's
  entries in circle2.
- **circle2** — entry storage with per-account chain links.
- **account_heads** — for each account, a pointer to that account's
  latest entry in circle2.

A separate map keyed by `tx_id` holds link records (sparse — most
transactions have none — so a hash map is acceptable here). All three
array sizes must be powers of two. [ADR-0008]

### §9.2 Why power-of-two

All three lookups reduce to "id AND mask" to find a slot, where mask is
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

### §9.4 circle2 — entry storage and per-account chains

Each circle2 slot stores one `TxEntry`'s payload (`tx_id`, `account_id`,
`amount`, `kind`, `computed_balance`) plus a *previous-link* — the
circle2 index of the same account's previous entry, or zero if there
is no previous. circle2 is written sequentially with a write head that
advances on every insert and wraps modulo circle2's size. On insert,
the indexer reads the account's head from `account_heads`; if the head
is for the same account, the new entry's previous-link is set to it,
otherwise the link is empty. Then the entry is written, `account_heads`
is updated to point at the new slot, and the write head advances.
Account history walks (§10.3) follow the previous-link chain backwards
in time until the chain ends, an evicted slot is detected (account-id
mismatch), the transaction predates the requested lower bound, or the
caller's limit is reached.

### §9.5 account_heads — direct-mapped account head table

Each `account_heads` slot stores `(account_id, circle2_idx)`. The slot
index is the account id masked by the account_heads size. The slot
stores the account id it belongs to so a colliding lookup can detect
eviction the same way circle1 does: a mismatch means the head was
overwritten and the chain head is gone, and the caller must fall back
to disk for any history older than the current chain. The size is
`next_power_of_two(max_accounts)`, which means in practice every
account has its own slot; collisions only occur if the account-id space
is sparser than the table.

### §9.6 Sizing rule

The three sizes are derived from `transaction_count_per_segment` and
`max_accounts`:

- circle1 — one slot per transaction in the active window, rounded up
  to a power of two.
- circle2 — enough entry slots for the active window assuming roughly
  two entries per transaction (transfers), rounded up.
- account_heads — one slot per account, rounded up.

Changing the active window changes the hot indexes and the dedup window
in lockstep; there are no separate knobs. [ADR-0013]

### §9.7 Recovery seeding

The recovery sequence (§12) seeds the indexer directly: as it walks
WAL records from the last snapshot through the active segment's tail,
each metadata, entry, and link record is replayed into the indexer
through the Snapshotter's recovery hooks. By the time the Snapshotter
thread starts, the indexer is already populated for everything in the
active window.

---

## §10 Query path — hot/cold routing

### §10.1 Query types

Two query types reach the Snapshotter through the FIFO (§8.4):

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

A query carries a one-shot callback. The convention is: caller creates
a per-request channel, wraps the sender in a closure, enqueues the
request, and blocks on the receiver. The Snapshotter computes the
response inline (§8.8) and invokes the callback before continuing the
apply loop. There is no separate response channel, no shared state for
partial results, no async runtime.

### §10.6 Status and balance queries

Two non-FIFO query paths exist:

- `get_balance(account_id)` — a direct atomic load on the read-side
  balance cache. Does not block, does not touch the Snapshotter,
  returns whatever balance the cache currently holds along with
  `last_tx_id` (§8.6).
- `get_transaction_status(tx_id)` — derived (§2.2) from the pipeline
  indexes plus the rejection registry (§4.17). Does not block.

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

For each closed-but-unsealed segment, Seal performs:

1. Open the closed segment.
2. Compute its file-level CRC and write the integrity sidecar files
   that mark the segment as sealed. Disk format details: Stage 2.
3. Build the on-disk transaction index and account index files for
   this segment. These are what cold-tier queries (§10.4) walk when
   the hot index misses.
4. Replay the segment's WAL records into Seal's own balance vector and
   function map (§11.4) so the next snapshot has accurate state.
5. If this segment's id is on the snapshot boundary (§11.5), write a
   balance snapshot and a function snapshot.
6. Publish the sealed segment id (advances `seal_index`, §2.5).

A failure in steps 2 or 3 is logged; the segment is left unsealed and
retried on the next poll.

### §11.4 Why Seal owns its own balance vector and function map

Seal holds two pieces of state separate from the rest of the pipeline:

- A balance array sized to `max_accounts`, updated from each entry's
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
or writing the data directory. Once recovery returns, the pipeline is
in a state that is observationally identical to the state that existed
at `last_committed_tx_id` immediately before the previous shutdown.

### §12.2 Pre-seal of unsealed segments

The first thing Recover does is scan for closed-but-unsealed segments
(§11.6). For each one it runs the per-segment seal procedure
synchronously and publishes the resulting id. After this step every
segment on disk except the active one is sealed.

### §12.3 Snapshot load and WAL replay

Recovery proceeds in this fixed order:

1. **Find the latest balance snapshot** — its segment id defines the
   replay starting point. The snapshot's records are loaded into the
   Transactor's balance cache (§4.3), Seal's balance vector (§11.4),
   and the read-side balance cache (§8.3).
2. **Find the latest function snapshot** — load each
   `(name, version, crc32c)` triple. Seal's function map is seeded;
   each binary is read off disk and installed in the WASM runtime.
3. **Replay sealed segments after the snapshot** — for every segment
   strictly newer than the snapshot's segment id, walk every record:
   - `TxEntry`: update each balance cache; update the hot index;
     update dedup if the owning transaction has `user_ref ≠ 0`.
   - `TxMetadata`: advance `last_tx_id`; update the hot index; update
     dedup.
   - `TxLink`: update the hot index.
   - `FunctionRegistered`: load or unload through the WASM runtime;
     update Seal's function map.
4. **Replay the active segment** — same as step 3 but against the
   active (un-rotated) segment file. Per-record CRC validation governs
   how far the active segment can be trusted; details (sidecar,
   broken-tail tolerance) are in Stage 2.
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

Every inter-stage queue is single-producer, single-consumer, lock-free,
fixed-capacity. Each stage owns its data completely; no shared mutable
state crosses stage boundaries. The fixed capacity is sized at
construction from `queue_size` (§15.2).

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

### §13.3 Backpressure as queue saturation

There is no rate limiter, leaky bucket, or admission control. Slowness
in any stage propagates upstream as queue fullness, eventually stalling
`submit()` (§2.6). This is intentional: the queues are the only place
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

A wait is a poll of the relevant pipeline index against the requested
`tx_id`, driven by the shared wait strategy (§13.2). When the index
passes the threshold, the wait completes and synchronises with every
store that produced the advance.

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

Capacity of every inter-stage queue. Default `16 384`. Not exposed via
`config.toml`; an internal tuning knob. The Sequencer→Transactor,
Transactor→WAL, and WAL→Snapshot queues are all sized from this single
value.

### §15.3 `wait_strategy`

Pipeline-mode for every stage's idle/backpressure loop. Default
`Balanced` (§13.2). One value is shared across every stage of a single
Pipeline.

### §15.4 `log_level`

Default `Info`. Not exposed via `config.toml`; set programmatically.
The temp and bench config presets both lower this to `Critical` to
keep test output clean.

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

Hot-index sizes (§9.6) are derived from `transaction_count_per_segment`
(§15.5) and `max_accounts` (§15.1). There are no separate knobs;
changing the active window changes both the hot indexes and the dedup
window in lockstep.

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
`wal.bin`, the writer is appending records to it, no `SegmentSealed`
record has been written; **Closed** — it has been renamed to
`wal_NNNNNN.bin` and a `SegmentSealed` record has been written and
fsynced, but the seal marker and CRC sidecar are not yet present;
**Sealed** — the per-segment seal procedure (§11.3) has computed the
file CRC, written the `.crc` sidecar, and written the empty `.seal`
marker. Cold-tier index lookups (§22) require the Sealed state;
recovery (§12.2) re-seals any Closed-but-not-Sealed segment it finds.

### §17.4 Rotation is rename-then-open

When the WAL Writer crosses the rotation threshold (§7.6), it appends
the `SegmentSealed` record to `wal.bin`, performs a synchronous
`fdatasync`, then renames `wal.bin` to `wal_NNNNNN.bin` and opens a
fresh `wal.bin` for the next segment. The rename is atomic on every
supported filesystem. A reader that opened `wal.bin` before the rename
retains its handle to the now-renamed inode; a reader that opens after
the rename gets the new file. This is the foundation of the inode-based
rotation detection used by replication (§23.3).

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

### §18.3 `TxMetadata` (`entry_type = 0`)

| Bytes | Field | Purpose |
|---|---|---|
| `[0]` | `entry_type = 0` (u8) | discriminant |
| `[1]` | `entry_count` (u8) | number of `TxEntry` records that follow |
| `[2]` | `link_count` (u8) | number of `TxLink` records after the entries |
| `[3]` | `fail_reason` (u8) | success = 0; see §1.12 |
| `[4..8]` | `crc32c` (u32) | covers this record (with this field zeroed) plus all entries plus all links |
| `[8..16]` | `tx_id` (u64) |  |
| `[16..24]` | `timestamp` (u64) |  |
| `[24..32]` | `user_ref` (u64) | dedup key (§5) |
| `[32..40]` | `tag` (8 bytes) | execution-engine identification (§6.14) |

The `crc32c` field's position inside its own coverage is what forces
the zero-while-computing convention: any reader that wants to verify
the CRC must zero those four bytes in a working copy before
recomputing. The `u8` widths of `entry_count` and `link_count` are
hard caps of 255 entries and 255 links per transaction (§1.8).

### §18.4 `TxEntry` (`entry_type = 1`)

| Bytes | Field |
|---|---|
| `[0]` | `entry_type = 1` (u8) |
| `[1]` | `kind` (u8) — `Credit = 0` or `Debit = 1` |
| `[2..8]` | `_pad0` (6 bytes) |
| `[8..16]` | `tx_id` (u64) |
| `[16..24]` | `account_id` (u64) |
| `[24..32]` | `amount` (u64) |
| `[32..40]` | `computed_balance` (i64, signed) |

`computed_balance` is signed because the WASM path can drive a balance
negative (§4.9); the built-in operations cannot. The `tx_id` field is
duplicated on every entry so a reader that joins on a tx boundary
(account-history walks, §10.3) does not need to look back at the
owning `TxMetadata`.

### §18.5 `SegmentHeader` (`entry_type = 2`)

| Bytes | Field |
|---|---|
| `[0]` | `entry_type = 2` (u8) |
| `[1]` | `version` (u8) — `WAL_VERSION = 1` |
| `[2..4]` | `_pad0` (2 bytes) |
| `[4..8]` | `magic` (u32) — `WAL_MAGIC = 0x524F4441` (`"RODA"` in ASCII) |
| `[8..12]` | `segment_id` (u32) — must equal the suffix in the file name |
| `[12..40]` | `_pad1`, `_pad2` (28 bytes) |

`SegmentHeader` is the first record of every segment file. The `magic`
field is the cheapest sanity check on any read: a wrong magic means
the file is not a roda WAL. The `version` byte exists for forward
evolution; a reader that does not know how to interpret a version
refuses the file rather than risk silent miscompute.

### §18.6 `SegmentSealed` (`entry_type = 3`)

| Bytes | Field |
|---|---|
| `[0]` | `entry_type = 3` (u8) |
| `[1..4]` | `_pad0` (3 bytes) |
| `[4..8]` | `segment_id` (u32) |
| `[8..16]` | `last_tx_id` (u64) — last committed `tx_id` in this segment |
| `[16..24]` | `record_count` (u64) — total records including this one and the header |
| `[24..40]` | `_pad1` (16 bytes) |

`SegmentSealed` is written only by rotation (§7.6) and is followed by
a synchronous `fdatasync`. Its presence at the tail is the definitive
signal that a segment file is complete; cold-boot recovery uses
`record_count` and `last_tx_id` as cross-checks against the bytes it
walked.

### §18.7 `TxLink` (`entry_type = 4`)

| Bytes | Field |
|---|---|
| `[0]` | `entry_type = 4` (u8) |
| `[1]` | `link_kind` (u8) — `Duplicate = 0`, `Reversal = 1` |
| `[2..8]` | `_pad` (6 bytes) |
| `[8..16]` | `tx_id` (u64) — the transaction that *owns* this link |
| `[16..24]` | `to_tx_id` (u64) — the transaction this link points at |
| `[24..40]` | `_pad2` (16 bytes) |

A link record always follows the `TxMetadata` of its owning
transaction and is included in the metadata's CRC32C coverage (§18.3).
A torn write that lands somewhere inside a multi-record transaction is
detected by the metadata CRC failing — the reader does not need a
per-record CRC to spot the corruption.

### §18.8 `FunctionRegistered` (`entry_type = 5`)

| Bytes | Field |
|---|---|
| `[0]` | `entry_type = 5` (u8) |
| `[1]` | `_pad0` (1 byte) |
| `[2..4]` | `version` (u16) — per-name monotonic, `1..=65535` |
| `[4..8]` | `crc32c` (u32) — CRC32C of the WASM binary; `0` ⇒ unregister |
| `[8..40]` | `name` (32 bytes) — ASCII snake_case, null-padded |

`FunctionRegistered` carries no `tx_id` and is not part of any
transaction's CRC envelope (§1.11). The `crc32c == 0` convention
preserves the audit trail for unregistration without introducing a
separate record kind. The 32-byte `name` width is a hard limit at
registration time (§6.15). [ADR-0001, ADR-0014]

---

## §19 Sidecar files

### §19.1 The CRC sidecar

Every Sealed segment has a paired `wal_NNNNNN.crc` file of exactly
16 bytes:

| Bytes | Field |
|---|---|
| `[0..4]` | `crc32c` (u32) — CRC32C of the entire segment binary |
| `[4..12]` | `file_size` (u64) — size in bytes of the segment file at seal time |
| `[12..16]` | `magic` (u32) — `WAL_MAGIC = 0x524F4441` |

The sidecar is written by the per-segment seal procedure (§11.3,
§25.5) after the segment file itself has been closed. The `file_size`
field catches truncation that does not change the CRC of the surviving
prefix; the `magic` field disambiguates a `.crc` against any unrelated
16-byte file that might end up in the directory.

### §19.2 The `.seal` marker

Alongside the `.crc` sidecar, Seal writes an empty `wal_NNNNNN.seal`
file. Presence of the marker is what distinguishes a Sealed segment
from a Closed-but-not-yet-sealed one: a crash between `SegmentSealed`
writing and the seal procedure leaves a Closed segment that recovery
(§12.2) re-seals on the next startup. The marker file carries no
content; only its existence matters. An empty marker file is cheaper
than a flag inside the sidecar because its presence can be detected by
a stat call alone.

### §19.3 The `wal.stop` clean-shutdown marker

`wal.stop` is the symmetric marker for the active segment: present iff
the last `Ledger::shutdown` call completed cleanly (§17.5). It is the
one file in the directory whose *absence* is informative. Recovery
uses its absence to decide whether the active segment needs
tail-trimming (§23.4) versus full-trust load.

### §19.4 Missing sidecars are tolerated

A Sealed segment that is missing its `.crc` sidecar is loaded with a
warning, not a failure. The rationale: the sidecar is a redundant
check; the segment's own per-record CRCs (§18.3) and `SegmentSealed`
cross-check (§18.6) are still authoritative. Missing sidecars are most
commonly produced by a crash after the segment was renamed but before
Seal ran, or by external tools that copied a segment without copying
its sidecar. Neither case warrants a refusal to start. [ADR-0006]

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

### §20.2 Balance snapshot header

Each `snapshot_NNNNNN.bin` begins with a 36-byte header followed by
an LZ4-compressed body:

| Bytes | Field |
|---|---|
| `[0..4]` | `magic` (u32) — `SNAPSHOT_MAGIC = 0x534E4150` (`"SNAP"`) |
| `[4]` | `version` (u8) — `SNAPSHOT_VERSION = 1` |
| `[5..9]` | `segment_id` (u32) |
| `[9..17]` | `last_tx_id` (u64) — the highest `tx_id` reflected in the snapshot |
| `[17..25]` | `account_count` (u64) — number of `(account_id, balance)` pairs in the body |
| `[25]` | `compressed` (u8) — `1` = LZ4, `0` reserved |
| `[26..32]` | `_pad` (6 bytes) |
| `[32..36]` | `data_crc32c` (u32) — CRC32C of the *uncompressed* body |

Two CRCs cover the file: this `data_crc32c` validates the records
themselves independent of the compression layer; the sidecar CRC
(§20.5) validates the written file as a whole.

### §20.3 Balance snapshot body

The body is LZ4-compressed bytes prefixed by a 4-byte little-endian
uncompressed-size word. Decompressed, the body is a flat array of
`account_count` records, each 16 bytes:
`[account_id:8 LE][balance:8 LE]`, sorted by `account_id`. Sorting is
for determinism — the same set of balances always produces the same
bytes — which makes the file reproducible across restarts and amenable
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
`FunctionRegistered` record (§18.8) and `N` is the per-name monotonic
version starting at `1`. The directory is the secondary signal of the
registry's history; the WAL is authoritative (§1.11).

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
`FunctionRegistered` record with `crc32c == 0` (§18.8). The 0-byte
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

### §22.2 Transaction index format

`wal_index_NNNNNN.bin` is a flat sorted array prefixed by a count:

```
[record_count: u64 LE]
[ tx_id: u64 LE | byte_offset: u64 LE ] × record_count
```

Records are in WAL order — i.e., naturally sorted by `tx_id`. The
`byte_offset` field points to the start of the transaction's
`TxMetadata` record inside the segment file. A cold-tier
`GetTransaction` binary-searches the index on `tx_id`, then issues a
positional read at the resulting offset to fetch the metadata plus
its dependent entries and links.

### §22.3 Account index format

`account_index_NNNNNN.bin` is the same shape but sorted differently:

```
[record_count: u64 LE]
[ account_id: u64 LE | tx_id: u64 LE ] × record_count
```

Records are sorted lexicographically by `(account_id, tx_id)`. A
cold-tier `GetAccountHistory` partition-points on `account_id` to
find the range, then iterates forward in `tx_id` order until either
the lower bound is hit or the limit is reached. Iteration order is
ascending in `tx_id`; the caller reverses if it wants newest-first.

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

On startup, recovery (§12) inspects the active segment for a torn
tail. The procedure: walk the segment record by record, validating
each metadata's CRC against its dependent entries and links; on the
first metadata whose CRC fails or that runs off the end of the file,
truncate the file to the byte position just before that metadata.
This is the *one* tolerated case of corruption (§12.7); a torn middle
is fatal. The torn-tail trim is performed via `set_len` followed by an
`fsync_data`, so the post-recovery file matches what is on disk.

### §23.5 Runtime truncation classifies segments by watermark

Divergence-driven runtime truncation (§33) classifies each segment by
its range of `tx_id`s relative to the recovery watermark. A segment
whose last `tx_id` is at or below the watermark is kept untouched. A
segment whose first `tx_id` is strictly above the watermark is removed
entirely — `wal_NNNNNN.bin`, the `.crc` and `.seal` sidecars, the
transaction and account indexes, the balance snapshot and its sidecar,
the function snapshot and its sidecar — every artifact for that
segment id. A segment that straddles the watermark is byte-truncated
at the first `TxMetadata` whose `tx_id` exceeds the watermark; the
surviving prefix is treated as Closed-but-not-yet-Sealed and resealed
on the next start.

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

### §24.2 40-byte records, magic per file

Both records are exactly 40 bytes — the same width as a WAL record
(§18.1) — so the same offline tooling can inspect them. A
`TermRecord` is `(term: u64, start_tx_id: u64, magic: u32 = "TERM",
crc32c: u32)` in its first 24 bytes, with the remaining 16 bytes
reserved zero-pad. A `VoteRecord` is `(term: u64, voted_for: u64,
magic: u32 = "VOTE", crc32c: u32)` in the same first 24 bytes, also
with 16 bytes of pad. The CRC covers bytes `[0..20]` only; it does
not include the `crc32c` field itself.

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
using the zero-copy serialization (§18.2) and recomputes the CRC32C
of every `TxMetadata` from its dependent entries and links. By
default it validates structural invariants: the first record is
`SegmentHeader`, the last is `SegmentSealed`, segment ids match, and
the `record_count` in `SegmentSealed` equals the actual record count.
`--no_validate` disables the structural checks for cases where a
deliberately partial or malformed segment is needed (typically for
testing recovery).

### §25.4 `verify`

`roda_ctl verify DIR [--segment ID | --range FROM..TO]` runs an
integrity audit over a data directory or a subset of its segments.
Per segment it checks: the file-level CRC against the `.crc` sidecar;
the magic byte and version of the `SegmentHeader`; the per-transaction
CRC; the cumulative `computed_balance` chain (no unexplained jumps);
declared `entry_count` and `link_count` match the actual record
counts. Across segments it checks that sealed segments form a
contiguous `tx_id` chain with no gaps and no overlaps. The output is
a structured report; a non-zero exit code signals at least one
failure.

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
  used by the leader to ship bytes to followers.
- An `on_commit` hook fired when the local commit index advances,
  used by the leader to feed its own slot of the quorum tracker
  (§30.4).
- `start_with_recovery_until(watermark)` — an alternative to `start()`
  invoked only by the runtime divergence path (§33).

Nothing else changes inside the Ledger. A standalone Ledger ignores
all four. [ADR-0015, ADR-0016 §10]

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
receiving `AppendEntries`). The role is encoded as a `u8` inside a
single atomic; reads use `Acquire`, writes use `Release`. The atomic
is shared by every gRPC handler so the writability check on every
submit RPC is one cache-line read.

### §27.2 The supervisor is the sole writer

The role supervisor is the only entity that mutates the role atomic.
Handlers, replication tasks, and ledger threads all read the atomic
freely but never write it. The single-writer rule means a transitioning
role cannot leave handlers observing a half-applied state: the
supervisor publishes the new role only after the old role's tasks
have been torn down and the new role's tasks have been brought up.

### §27.3 Drop-and-rebuild on every transition

Role-specific state — peer replication tasks, the Quorum tracker,
the gRPC handler's posture — lives inside a `LeaderHandles` or
`FollowerHandles` struct. A transition drops the outgoing struct
(which joins or aborts every owned task) before constructing the new
one. There is no "modify role in place" path; a node that goes
Leader → Follower → Leader has fully re-instantiated its peer tasks,
fresh quorum slots, and a freshly-bound writable handler each time.
The discipline is what eliminates stale-state bugs during failover.

### §27.4 What survives a transition

Three pieces of state are deliberately carried across a role
boundary: the `Arc<Ledger>` (so the node's data does not have to be
recovered on every transition), the `Arc<Term>` (so the durable term
log keeps a single owner), and the `Arc<Vote>` (same reason for vote
durability). Every other piece of state is owned by a `Handles`
struct and dies with it. The one exception to even the Ledger
surviving a transition is divergence (§33), in which case the Arc is
dropped and rebuilt via `start_with_recovery_until`.

### §27.5 `Initializing` is the post-boot and post-teardown state

`Initializing` is the role a node is in immediately after the
supervisor starts (before the first election timer fires) and
immediately after any `Handles` is dropped (before the next role's
bring-up). The Node gRPC server is already running so the node can
participate in elections and receive `AppendEntries`, but neither the
writable client handler nor any peer-replication task is up. The
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
election_timer_max_ms]` (default 150–300 ms). The deadline is
re-randomised on every reset. Reset triggers: any valid
`AppendEntries` from a current leader (empty heartbeat included), and
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
matches `Quorum`'s majority calculation (§30.1) but is computed
independently — the election's vote count is not the running quorum.

### §29.4 `RequestVote` grant rule

`RequestVote` grants iff *all three* hold (Raft §5.4.1):

1. `request.term ≥ current_term`.
2. `voted_for` is `None`, or `voted_for == request.candidate_id`
   (within this term).
3. The candidate's log is at least as up-to-date as ours:
   `(request.last_term, request.last_tx_id) ≥ (our_last_term,
   our_last_tx_id)` lexicographically.

A granted vote fsyncs `vote.log` *before* the RPC reply is sent
(§28.2). The third clause is what enforces leader completeness: a
candidate whose log lacks a quorum-committed transaction cannot win.

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

Any RPC in any direction (request or response, `RequestVote` or
`AppendEntries` or even a `Ping`) that carries a term higher than the
node's current term triggers an immediate step-down: `Term::observe`
durably records the new term, `voted_for` is cleared, and the node
transitions to `Initializing`. A node in `Leader` role drops its
`LeaderHandles` (which drains all peer tasks); a node in `Follower`
or `Candidate` drops the equivalent. The step-down is the universal
correctness anchor — no Raft node ever continues operating in an old
term once it has seen a higher one. [ADR-0016 §5, §6, §13]

---

## §30 Quorum tracker

### §30.1 One slot per node

`Quorum` is an array of `AtomicU64`s, one slot per node in the
cluster. By convention slot 0 is the leader itself; slots `1..` are
the peers in configuration order. The majority size —
`(node_count / 2) + 1` — is computed at construction and never
changes. A separate atomic caches the current majority-committed
index for lock-free reads.

### §30.2 `advance` publishes via `fetch_max`

`advance(slot, index)` writes the slot's atomic with `Release`,
snapshots all slots with `Relaxed`, sorts the snapshot descending,
takes the `(majority - 1)`-th element (the highest index acknowledged
by a majority), and publishes it via `fetch_max(Release)` on the
cached majority atomic. The `fetch_max` ensures the published majority
never regresses, even when concurrent `advance` calls from different
slots interleave. This is the lock-free property that lets every peer
task call `advance` without coordination.

### §30.3 Lock-free reads via `get`

`Quorum::get()` is one `Acquire` load of the cached majority atomic.
There is no allocation, no fast-path lock, no per-call computation.
Client wait paths (§34.1) call `get()` repeatedly while polling for a
target index; the cost per poll is one cache-line load.

### §30.4 The leader's slot is fed by the on-commit hook

On `Leader` bring-up, the leader registers an `on_commit` callback
with the Ledger. The callback fires every time the local commit index
advances and calls `advance(self_slot, ledger.last_commit_id())`.
Without this callback the leader's own progress would be invisible to
the quorum calculation and the cached majority would be stuck at the
slowest peer. The hook is the reason the leader counts toward its own
quorum.

### §30.5 `reset_peers` zeros peer slots on bring-up

When a leader is freshly elected, the prior leader's match-index
state may still be reflected in the slots; carrying it forward could
let a stale peer slot inflate the new leader's perceived majority.
`reset_peers(leader_slot)` is called as part of `Leader` bring-up and
zeros every slot except the leader's own. The next `advance` call
from each peer task republishes from a clean baseline.
[ADR-0015 decisions 3, 5; ADR-0016 §3]

---

## §31 Leader replication tasks

### §31.1 One task per peer

A `Leader` spawns one `tokio` task per peer (`PeerReplication`). The
leader owns these tasks directly — there is no per-peer-supervisor
indirection — and they all share the leader's `Arc<Quorum>`,
`Arc<AtomicBool> running` shutdown flag, and a transition channel for
out-of-band signals. Spawning is part of `Leader::run_role_tasks`;
joining is part of `LeaderHandles::drop`.

### §31.2 Each task's private state

Each peer task owns: a `WalTailer` cursor seeded from the leader's
ledger (§23.1); a tonic Node-service client connected to the peer;
the peer's slot index in `Quorum`; and two watermarks — `from_tx_id`
(Raft's `nextIndex`, the next `tx_id` to ship) and `peer_last_tx`
(Raft's `matchIndex` snapshot, the highest `tx_id` the peer
acknowledged). Nothing else is shared between peer tasks.

### §31.3 The replication loop

Each iteration: call `tailer.tail(from_tx_id, &mut buf)`; if it
returns zero bytes, send an empty `AppendEntries` heartbeat carrying
only `leader_commit_tx_id` and sleep for `replication_poll_ms`; if it
returns bytes, call the ship-until-accepted path which retries on
transport failures and observes higher-term replies. On a successful
non-empty reply the task advances `from_tx_id` past the shipped
batch's last `tx_id`, updates `peer_last_tx`, and calls
`quorum.advance(slot, peer_last_tx)`.

### §31.4 Lagged single-phase replication

A successful `AppendEntries` returns the follower's *current*
fsynced `last_commit_id` — *not* a fresh fsync of the shipped batch.
The follower queues the bytes and replies immediately; its WAL stage
fsyncs on its own schedule (§7.7) and the next RPC's reply reflects
the result. Replies are therefore one batch stale relative to the
shipped data, which is harmless for quorum tracking because the gap
closes on the next reply. The model avoids blocking the network RPC
on disk latency. [ADR-0015]

### §31.5 Idle heartbeats close the staleness gap

Without idle heartbeats, the leader's last knowledge of a peer's
commit progress would be the reply to the previous shipment — itself
one batch stale. After the writer goes idle, the peer's true commit
watermark would never be observed. Sending an empty `AppendEntries`
every `replication_poll_ms` fixes this: the reply carries the peer's
now-fsynced `last_commit_id`, which advances the peer's quorum slot.
Transport errors on heartbeats are intentionally swallowed — the
next interval retries; a heartbeat is purely observational, not a
durability event.

### §31.6 Step-down on higher-term reply

A peer reply whose `term` is higher than the leader's current term
triggers an immediate step-down: the peer task posts a step-down
transition to the supervisor and returns. The supervisor drains all
peer tasks (drops `LeaderHandles`), observes the higher term, clears
`voted_for`, and re-enters `Initializing`. The write side is also
drained: any in-flight client submit returns the error generated by
the dropping handler. [ADR-0015, ADR-0016 §3]

---

## §32 Per-peer replication and consistency

### §32.1 Raft naming map

The Roda cluster module uses Raft's terminology where possible:
`from_tx_id` is `nextIndex`; `peer_last_tx` and the `Quorum` peer
slot together correspond to `matchIndex`. The `tx_id` stream is the
Raft log; the term boundary records in `term.log` (§28) supply the
term for any historical `tx_id`. This mapping is documented so a
reader who knows Raft can navigate the implementation without
re-deriving the correspondence.

### §32.2 `prev_tx_id` and `prev_term` consistency check

Every `AppendEntries` request carries `prev_tx_id` (the `tx_id`
immediately before the batch) and `prev_term` (the term of that
`tx_id`). The follower validates: it has `prev_tx_id` durably on
disk, and the term covering `prev_tx_id` matches `prev_term`. If the
follower's log is shorter, the reply carries `REJECT_PREV_MISMATCH`
with the follower's own `last_commit_id`. If the follower's log has
the `tx_id` but with a different term, the reply also carries
`REJECT_PREV_MISMATCH` and divergence handling kicks in (§33).

### §32.3 The first RPC

For the first `AppendEntries` against an empty follower (or a
fully-replicated follower starting a fresh batch from `tx_id = 0`),
both `prev_tx_id` and `prev_term` are zero. The follower treats
`(0, 0)` as "no precondition" and accepts. Subsequent RPCs use the
actual preceding `tx_id` and its covering term.

### §32.4 Reject reasons are explicit

`AppendEntriesResponse` carries an enum reject reason rather than a
bare bool. The four currently emitted reasons are: `TERM_STALE`
(request's term is below the follower's current term),
`PREV_MISMATCH` (consistency check failed — see §33), `CRC_FAILED`
(the bytes did not parse), `WAL_APPEND_FAILED` (the follower's WAL
stage rejected the queued bytes). The leader uses the reject reason
to decide whether to step down (`TERM_STALE`), trigger a divergence
path on the follower side (`PREV_MISMATCH`), or log and continue.

### §32.5 Batch sizing is exact

WAL records are exactly 40 bytes (§18.1), so the leader's per-RPC
byte cap is always a clean multiple of 40 — there is no padding, no
partial record at the end of a batch. The configured
`append_entries_max_bytes` is rounded down to the nearest multiple of
40 at startup.

### §32.6 gRPC message-size override

Tonic's default decoding/encoding limit of 4 MiB would otherwise
reject batches sized at exactly `append_entries_max_bytes`. The Node
server and client both raise `max_decoding_message_size` and
`max_encoding_message_size` to `append_entries_max_bytes × 2 + 4 KiB`
to cover both the WAL byte payload and protobuf framing overhead with
margin. [ADR-0015, ADR-0016 §8]

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

### §33.2 Divergence detection on the follower

When `AppendEntries` arrives with a `prev_tx_id` that the follower
has on disk but with a `prev_term` that disagrees with the follower's
own term covering that `tx_id`, the follower has diverged from the
new leader's history. The follower stashes `leader_commit_tx_id` from
the request as the *recovery watermark* and replies
`REJECT_PREV_MISMATCH`. The supervisor's divergence watcher reads the
stashed watermark and triggers the reseed.

### §33.3 Reseed sequence

The supervisor: drops the current `FollowerHandles` (so all tasks
holding the `Arc<Ledger>` are gone); calls a `LedgerSlot::replace`
with a freshly-built `Ledger::start_with_recovery_until(watermark)`,
which gives the slot a new `Arc<Ledger>` and returns the old one for
asynchronous drop on a background task; brings up a fresh
`FollowerHandles` over the new Ledger. The next `AppendEntries` from
the leader either resumes cleanly (`prev_tx_id ≤ watermark`) or
triggers a normal catch-up backfill from the leader.

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
restarting the gRPC servers. The mechanism is `LedgerSlot`: an atomic
swap of `Arc<Ledger>`. Every handler reads the current Arc on every
call (`slot.ledger()`) and never retains it across operations. The
swap is lock-free; the old Arc's strong count drops to zero on a
background task that joins the old Ledger's pipeline threads
synchronously, which never blocks a gRPC request because gRPC handlers
have already released their reference. The supervisor is the sole
writer of the slot. [ADR-0016 §9, §10]

---

## §34 Cluster commit and the `cluster_commit` wait level

### §34.1 A fifth wait level

Stage 1 introduces four wait levels (§14.1). The cluster adds a
fifth: `cluster_commit` blocks the submitter until
`quorum.get() ≥ tx_id`. The handler driving the wait is the same
shared wait strategy (§13.2) used for the in-process levels; the only
difference is which atomic the predicate reads.

### §34.2 The leader feeds its own slot

The leader's slot in `Quorum` is advanced by the on-commit hook
(§30.4); each peer task advances its peer's slot on every successful
`AppendEntries`. Both contributions are required for the majority to
move forward — without the leader's own contribution the cached
majority would be stuck at the slowest peer's progress.

### §34.3 Local commit is independent of quorum

The leader's *own* `commit_index` (§2.5) advances from its local WAL
stage as soon as the local `fdatasync` returns, independent of any
peer's progress. Only the client's wait choice determines the
guarantee: a client that picks `wal` sees the local commit; a client
that picks `cluster_commit` waits for quorum. The two indexes coexist
in the same pipeline; nothing about cluster mode slows down the local
commit path. [ADR-0015 decision 5]

### §34.4 Followers do not durably track cluster commit

`leader_commit_tx_id` arrives on every `AppendEntries` (including
heartbeats) but the follower does not persist it. Its only consumer
on the follower side is divergence handling (§33.2), where it is
captured as the recovery watermark. A follower's view of "cluster
commit" is not authoritative — only the leader's `Quorum::get()` is.
Reads from a follower that need cluster-commit semantics are out of
scope for this implementation.

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

### §35.2 `AppendEntries`

Request fields: `leader_id`, `term`, `prev_tx_id`, `prev_term`,
`from_tx_id`, `to_tx_id`, `wal_bytes`, `leader_commit_tx_id`.
Response fields: `term`, `success` (bool), `last_tx_id` (the
follower's *current* fsynced commit id, see §31.4), `reject_reason`.
An empty `wal_bytes` is a valid heartbeat: the follower runs the
consistency check and returns its commit id without queueing any
records.

### §35.3 `RequestVote`

Request fields: `term`, `candidate_id`, `last_tx_id`, `last_term`.
Response fields: `term`, `vote_granted`. The handler implements the
grant rule of §29.4 verbatim and persists the vote via `Vote::vote`
before replying. A grant that requires a term observation also
durably bumps the term via `Vote::observe_term`.

### §35.4 `Ping`

Request fields: `from_node_id`, `nonce`. Response fields: `node_id`,
`term`, `last_tx_id`, `role`, `nonce`. The handler reads only atomics
— no WAL, no vote log — and bypasses the per-node mutex. Its purpose
is operational: health checks, RTT measurement, and leader discovery
without the cost of the full Raft handler stack.

### §35.5 `InstallSnapshot`

Request and response fields are modelled after Raft's
`InstallSnapshot` and exist in the proto to lock in the wire format.
The handler is scaffolded but not fully implemented; followers that
fall too far behind a leader's retention window will need this RPC to
catch up via a snapshot rather than via WAL backfill. The protocol is
in place; the handler currently returns `UNIMPLEMENTED`.

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
cache and never touch the snapshot FIFO (§10.6); the rest enqueue
queries on the Snapshotter FIFO (§8.4). Reads are served on every
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

The role supervisor builds the long-lived shared resources: `Quorum`
(sized to `peers + 1`), the cooperative `running: Arc<AtomicBool>`,
and the transition channel. It then spawns the client-facing gRPC
server, the peer-facing gRPC server, the divergence watcher, and the
role driver loop. The two gRPC servers are bound for the entire
lifetime of the process; they never restart on a role transition.

### §38.3 Initial role

A single-node cluster (`peers.len() == 1` and the only entry is
`self`) is brought up with `initial_role = Leader` so the node is
immediately writable without waiting for an election timer. A
multi-node cluster starts in `Initializing` and waits for the first
election timer to expire before becoming a candidate.

### §38.4 The role-driver loop

The driver awaits the appropriate signal per role: an election-timer
expiry while in `Initializing` or `Follower`; the candidate round's
outcome while in `Candidate`; a transition-channel message while in
`Leader`. Each transition drops the previous role's `Handles` (which
joins or aborts every owned task) before constructing the next role's
`Handles`. The driver itself owns no role-specific state; it is purely
the coordinator that decides which role to bring up next.

### §38.5 Shutdown ordering

Shutdown flips `running` to `false`, sends a `Shutdown` transition
message (which wakes the role driver if it is parked), and aborts
every spawned task in order — the role driver first, the divergence
watcher, the peer-facing server, the client-facing server. Peer
replication tasks observe `running` at the top of their loop and exit
cleanly; the gRPC servers run their tonic shutdown protocol before
the abort takes effect.

---

## §39 Standalone, single-node, and testing modes

### §39.1 Standalone (no `[cluster]` config block)

When the configuration has no `[cluster]` section, `ClusterNode` does
not invoke the supervisor at all. It constructs a single client-facing
`Server` task with a hardcoded `Role::Leader` posture, no `RoleFlag`,
no Node gRPC server, no `Term`, no `Vote`, no `Quorum`, no peer tasks.
The deployment is identical in observable behaviour to a single-node
cluster but pays none of the supervisor's coordination cost. This is
the configuration `roda` ships with by default.

### §39.2 Single-node cluster (`peers.len() == 1`)

A cluster whose configuration lists exactly one peer (itself) runs
the full supervisor infrastructure: a `RoleFlag`, a `Term`, a `Vote`,
a single-slot `Quorum`. The node boots as `Leader` (§38.3) and the
replication loop has zero peers to fan out to; every `on_commit` hook
firing immediately advances the cluster watermark since there is no
one to wait for. This mode exercises the cluster code path against a
trivial cluster shape and is the one used by most cluster integration
tests.

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
