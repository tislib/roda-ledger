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
