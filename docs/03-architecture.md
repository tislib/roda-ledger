# Architecture

This document explains the design of roda-ledger — what each part does, why it exists, and the reasoning behind key tradeoffs. 

[//]: # (Implementation details, invariants, and decisions are in [Internal]&#40;./internal.md&#41;.)

If you haven't already, read [Concepts](./01-concepts.md) first — it defines the terminology and guarantees this document builds on.

---

## Design philosophy

roda-ledger is built around two problems that pull in opposite directions: **correctness** and **throughput**.

Correctness requires strict ordering and a single source of truth — concurrent writers create races, conflicts, and partial states that are catastrophic in a financial system. Throughput requires parallelism — sequential processing leaves hardware idle.

The solution is a **staged pipeline**. Each stage does one job on its own thread, communicating with adjacent stages through lock-free queues. There is no shared mutable state between stages.

The key insight: **parallelism around execution, not within it.** The Transactor executes one transaction at a time — this is the source of all correctness guarantees. But while the Transactor executes transaction N, the WAL is persisting N-1, and the Snapshotter is making N-2 visible to readers. All stages run concurrently without ever coordinating.

---

## Pipeline overview

<img src="./resources/pipeline.png" style="width: 100%;" class="wide-image" />

A transaction moves through four stages, each adding a guarantee:

| Stage | Guarantee | Status |
|---|---|---|
| Sequencer | Monotonic ID, permanent global order | `PENDING` |
| Transactor | Executed, serializable, result known | `COMPUTED` |
| WAL | Durable on disk, crash-safe | `COMMITTED` |
| Snapshotter | Balances and indexes visible | `ON_SNAPSHOT` |

The caller chooses which stage to wait for — see [API](./02-api.md) for wait level details.

---

## Transaction logical view

<img src="./resources/logical-view.png" style="width: 100%;" class="less-wide-image" />

Every transaction occupies a fixed position in a globally ordered sequence. Three pipeline indexes divide that sequence into zones at any given moment:

- **`compute_index`** — the furthest transaction the Transactor has executed. Everything to its right is accepted, in memory, serializable. Not yet durable.
- **`commit_index`** — the furthest transaction flushed to disk. Everything to its right is durable and crash-safe. The **Commit Gap** between `compute_index` and `commit_index` represents transactions that would be lost on a crash — bounded by the WAL's batch flush cycle (~100s µs).
- **`snapshot_index`** — the furthest transaction visible to `get_balance`. Everything to its right is readable and linearizable. The **Balance Gap** between `commit_index` and `snapshot_index` is typically nanoseconds.

Two invariants hold at all times:

1. All transactions are monotonically ordered — no gaps, no reordering.
2. If transaction N is committed, all transactions before N are also committed. The indexes only move forward.

These invariants are what make per-call wait levels meaningful. When you call `submit_wait(wal)`, you are waiting for `commit_index` to pass your transaction ID — and you know with certainty that everything before it is also durable.

---

## The Sequencer

The Sequencer is the entry point. It assigns a unique, monotonically increasing ID to every transaction and fixes its position in the global order. This decision is permanent — no later stage can change it.

The Sequencer has no dedicated thread. It runs on the caller's thread at submit time — nothing more than an atomic increment and a queue push.

---

## The Transactor

The Transactor is a single-threaded, deterministic execution engine. It processes one transaction at a time, in strict sequence order.

**Why single-threaded?** A single writer eliminates races and conflicts entirely. Correctness is guaranteed by construction, not by coordination.

<img src="./resources/transactor-flow.png" style="width: 100%;"/>

For each transaction the Transactor: checks deduplication by `user_ref`, executes the operation (producing credit/debit entries and updating the balance cache live), verifies invariants (zero-sum), computes a CRC over the full transaction, then advances its processed index and sends to the WAL queue. Failed transactions — duplicates, execution errors, invariant violations — follow the same path to the WAL for audit purposes.

The balance cache always reflects the latest state. This is why the write path is always linearizable — the Transactor never makes a decision based on stale data.

For built-in operations (`Deposit`, `Withdrawal`, `Transfer`) the Transactor runs native Rust code. For an `Operation::Function`, the Transactor delegates to the embedded **WASM Runtime** described below. Either path produces the same `TxEntry` records, is bounded by the same zero-sum check, and uses the same rollback machinery — there is no second code path for durability or recovery.

---

## The WASM Runtime — programmable execution inside the Transactor

The WASM Runtime is not a separate pipeline stage. It is a component **inside the Transactor** that handles `Operation::Function` the same way native code handles `Operation::Transfer`. This placement is deliberate: a function execution must inherit every guarantee the Transactor already provides — single-writer ordering, zero-sum enforcement, atomic rollback, deduplication — without any new coordination.

<img src="./resources/transactor-flow.png" style="width: 100%;"/>

### Where it sits in the pipeline

```
Sequencer → Transactor ─┬─ native execution path  ─┐
                        │                          ├─→ TxEntry stream → WAL → Snapshotter
                        └─ WasmRuntime.invoke()   ─┘
```

A `Function` operation flows through Sequencer → WAL → Snapshotter exactly like any other operation. The only difference is *how* the Transactor produces its entries: by calling into a sandboxed WebAssembly handler that issues `credit` / `debit` host calls, instead of running a hard-coded Rust branch.

### Components

- **`WasmRuntime`** — one per ledger, wrapping a single shared `wasmtime::Engine` and `Linker`. Lives behind an `Arc<WasmRuntime>` and is cheap to clone.
- **Per-Transactor caller cache** — a `HashMap<name, CachedHandler>` owned by the Transactor thread. No locks on the hot path; the cache stores the `update_seq` of the registry it was verified against and revalidates lazily when a registration changes.
- **Shared registry** — a small lock-protected map of `name → (version, crc32c, compiled module)`. Only touched on registration, unregistration, and cache miss / revalidation — never on every transaction.
- **Host-side execution context** — a per-Transactor `TransactorState` that captures the host calls a function emits during a single `execute()` invocation. Credits and debits accumulate here, not in WASM-visible globals, so no state leaks across calls.

### Atomic execution flow

For an `Operation::Function { name, params, user_ref }` the Transactor:

1. Performs the standard `user_ref` deduplication check.
2. Resolves `name` against the caller cache, falling back to the shared registry on miss; verifies `update_seq` to detect a concurrent registration.
3. Opens an isolated host-side execution context — credits and debits emitted from now on are recorded against this transaction only.
4. Calls `TypedFunc::call(execute, params)` on the cached handler. The function may issue any number of `credit` / `debit` / `get_balance` host calls; each is captured into the execution context.
5. On any trap, host error, or non-zero return code: discards every captured credit/debit, marks the transaction failed with the appropriate status (the user-defined `u8` for `128..=255`, a standard reason otherwise), and writes a `TxMetadata` record so auditors can see exactly which function and which return code rejected the transaction.
6. On a `0` return: verifies `sum(credits) == sum(debits)`. Failure here yields `ZERO_SUM_VIOLATION` and the same rollback. Success commits the captured entries to the live balance cache and emits them as ordinary `TxEntry` records.
7. Tags the resulting `TxMetadata` with `b"fnw\n" ++ crc32c[0..4]` so every committed transaction permanently identifies the exact binary that produced it.

Stages 5–7 are the atomicity boundary. Nothing reaches the WAL queue until the function has either fully succeeded *and* balanced, or been fully rolled back. The WAL stage and the Snapshotter that follow it are unchanged — they cannot tell a function-produced entry stream from a native one.

### Why this fits the pipeline

- **Single-writer correctness.** A function executes on the Transactor thread, in strict sequence, with the same in-memory balance cache native operations use. Two functions cannot race; a function cannot race with a native operation.
- **No new persistence path.** Function-produced entries are normal `TxEntry` records. WAL segmentation, sealing, snapshotting, and replay treat them identically to entries from a `Transfer`.
- **No new failure mode.** A WASM trap, an unbalanced credit/debit set, or a domain-specific reject all use the existing transaction-rollback machinery.
- **Determinism for replication.** The host API is intentionally narrow: no clocks, no randomness, no I/O, no atomics, no threads. The leader executes a function; any future Raft follower can apply the resulting WAL entries directly without re-running the WASM code.

### Registration as a first-class WAL event

Registering or unregistering a function is itself a durable transaction in the pipeline:

1. The submitted binary is validated (signature, size, exports) before any disk write.
2. The binary is written atomically (`tmp + rename`) to `data/functions/{name}_v{N}.wasm`. Unregister writes a 0-byte file under the next version — the audit trail is preserved.
3. A `FunctionRegistered` WAL record (`name`, `version`, `crc32c`) is committed to the active segment.
4. The handler is compiled and installed in the live `WasmRuntime`.
5. Only after all four steps does `RegisterFunction` return.

Periodic snapshots emit a paired `function_snapshot_{N}.bin` next to the balance `snapshot_{N}.bin`, on the same `snapshot_frequency` trigger. Recovery loads the function snapshot first, replays `FunctionRegistered` records from the WAL after it (`crc32c != 0` → load, `crc32c == 0` → unload), and only then resumes processing transactions. A missing or corrupted binary aborts startup — the runtime never serves traffic against a registry that diverges from the WAL.

### Performance shape

- The hot path adds one `HashMap::get`, one `TypedFunc::call`, and one host-import crossing per `credit` / `debit`.
- Compiled modules are cached per `(name, crc)`; instantiation cost is paid once per registration, not per call.
- Cache invalidation is per-name — registering `foo` does not evict the cached entry for `bar`.
- At the Transactor level, WASM execution adds ~200 ns per operation over the native path. At the pipeline level, this is invisible: the WAL `fdatasync` cost dominates and end-to-end TPS for `Function` and `Deposit` are within a percent of each other.

Detailed ABI, host API, validation rules, and recovery semantics live in [WASM Runtime](./wasm-runtime.md) and [ADR-014](./adr/0014-wasm-function-registry.md).

---

## The WAL

The WAL stage makes transactions durable. A transaction executed by the Transactor exists only in memory — the WAL writes it to disk before it can be considered safe.

<img src="./resources/wal-flow.png" style="width: 100%;" class="less-wide-image" />

The WAL runs as **two concurrent threads** communicating through shared atomics:

- **WAL Writer** — drains the input queue into a buffer, writes to the active segment file, advances `last_written_tx_id`, rotates segments when full, and pushes committed entries to the Snapshotter queue.
- **WAL Committer** — runs independently, calls `fdatasync` whenever `last_written_tx_id > last_committed_tx_id`, then advances `last_committed_tx_id`.

The Writer never blocks waiting for `fdatasync` — it continues writing while the Committer syncs. Entries only flow to the Snapshotter after the Committer confirms durability. This decoupling is why the WAL sustains high write throughput despite the inherent latency of `fdatasync` (~100s µs, disk-bound).

The WAL is **segmented** — divided into files based on transaction count (`transaction_count_per_segment`). When the transaction count in the active segment reaches the configured limit, the Writer rotates to a new one. Segment files are dynamically sized on disk — a segment with many complex (multi-entry) transactions will be larger than one with simple deposits — but the transaction count per segment is always fixed and predictable. Sealed segments are complete, consistent units used for recovery.

---

## Segment lifecycle

A segment moves through three states:

| State | File on disk | Description |
|---|---|---|
| `ACTIVE` | `wal.bin` | Currently being written. One at a time. |
| `CLOSED` | `segment_NNN.wal` | WAL Writer rotated away. Immutable, awaiting Seal. |
| `SEALED` | `segment_NNN.wal` + `.crc` + `.seal` | Verified, safe for recovery. May have snapshot. |

**ACTIVE → CLOSED:** transaction count threshold hit → WAL Writer writes `SegmentSealed` entry, `fdatasync`, renames `wal.bin` to `segment_NNN.wal`, opens new `wal.bin`.

**CLOSED → SEALED:** Seal process picks it up on a timer, computes CRC32, writes `.crc` and `.seal` sidecar files.

### Segment anatomy

<img src="./resources/segment-anatomy.png" style="width: 60%;" />

Every WAL record is exactly **40 bytes** — fixed size, no variable-length scanning. This makes recovery fast and deterministic.

### Seal process

The Seal process runs on a configurable timer. For each unsealed segment it: loads the WAL data, seals it (CRC + sidecar files), builds on-disk tx and account indexes, updates its local balance buffer, and conditionally writes a snapshot file.

<img src="./resources/seal-flow.png" style="width: 100%;" />

Snapshots are not written on every seal — only every `snapshot_frequency` segments (default: 2). The snapshot captures all non-zero balances at that point and is the base for recovery.

---

## Active Window, Idempotency, and Hot Indexes

The **active window** is the central sizing concept in roda-ledger. It is defined as the last N transactions where N = `transaction_count_per_segment`. Three subsystems are derived from this single value:

### Segment rotation

WAL segment rotation triggers when the transaction count in the active segment reaches `transaction_count_per_segment`. This makes the active window predictable — operators always know exactly how many transactions fit in the active segment, regardless of transaction complexity or entry counts.

### Idempotency (deduplication)

The Transactor maintains a flip-flop deduplication cache — two HashMaps (`active` and `previous`) mapping `user_ref → tx_id`. When `tx_id` crosses a segment boundary, the maps flip: `active` becomes `previous`, and a new `active` begins. This gives an effective deduplication window of N to 2N transactions.

The flip is driven by transaction ID, not wall-clock time. Under a burst of 1M transactions per second the window covers the same number of transactions as under 100 transactions per second. Idempotency guarantees are deterministic.

Deduplication is always on — there is no config toggle. `user_ref = 0` skips the check on a per-transaction basis, but the cache is always active.

### Hot indexes

The Snapshotter maintains two circular indexes for serving `GetTransaction` and `GetAccountHistory` queries on recent data, hot index is keeping last N transactions where N is active window size.

---

## The Snapshotter

The Snapshotter applies committed entries to the readable state — the balance cache, transaction index, and account history index.

**Why a separate stage?** The Transactor's balance cache is write-side truth, private and optimized for writes. The Snapshotter maintains read-side truth. Separating them means readers never block writers.

The Snapshotter processes a single input queue carrying two message types:

<img src="./resources/snapshotter-flow.png" style="width: 100%;" />

- **WalEntry** — updates the appropriate index or balance cache based on entry type. Once a full transaction is processed, `last_processed_tx_id` advances and `get_balance` reflects it.
- **QueryRequest** — executes inline against current state (`GetTransaction`, `GetAccountHistory`) and calls the response callback.

The Snapshotter does no computation — balances are pre-computed by the Transactor and stored in entries. The gap between `COMMITTED` and `ON_SNAPSHOT` is typically nanoseconds.

---

## Inter-stage communication

Stages communicate exclusively through **SPSC lock-free queues** — one producer, one consumer, no locks, no CAS contention. Each stage owns its data completely.

**Backpressure** propagates naturally: WAL pressure fills the Transactor-to-WAL queue, stalling the Transactor, which fills the Sequencer-to-Transactor queue, which eventually stalls `submit()`. No explicit flow control needed.

The wait strategy under backpressure is controlled by `pipeline_mode`: `low_latency` (spin forever), `balanced` (spin → yield → park), `low_cpu` (park quickly).

---

## Recovery

On startup, roda-ledger restores state automatically before accepting transactions:

1. Find the latest snapshot — gives all balances at a known transaction ID
2. Load those balances
3. Replay WAL segments that postdate the snapshot
4. Resume from the exact point where the ledger left off

Because every committed transaction is in the WAL, and the WAL is always replayed on recovery, **a committed transaction is never lost**. Recovery time is bounded by snapshot frequency — more frequent snapshots mean less WAL to replay.

---

## Design boundaries

**Single node.** All pipeline stages run on one machine. Raft-based multi-node replication is planned — the segmented, append-only WAL is a natural fit for log replication.

**Pre-allocated account space.** Balances are stored in a structure sized to `max_accounts`. O(1) reads and writes always, but memory is committed at startup — a capacity planning decision.

**Disk-bound durability throughput.** `fdatasync` latency determines `COMMITTED` throughput. On NVMe this is hundreds of µs per batch. `COMPUTED` throughput (in-memory only) reaches millions of transactions per second.

**Single-writer ceiling.** The Transactor is the upper bound on execution throughput. In practice the WAL is almost always the bottleneck — but on fast enough storage, the single-threaded Transactor becomes the limit.
