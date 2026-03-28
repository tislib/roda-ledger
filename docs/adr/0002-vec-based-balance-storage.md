# ADR-002: Vec-based Balance Storage

**Status:** Accepted  
**Date:** 2026-03-27  
**Author:** Taleh Ibrahimli

---

## Context

During the entries-based refactor (ADR-001), balance storage for the pipeline stages needed a new design. Several concurrent map structures were evaluated:

**SkipMap (crossbeam)** was the original implementation. Benchmarks showed:

```
Accounts    SkipMap TPS
───────────────────────
1K          ~1.4M
1M          ~960K
10M         ~755K
50M         ~460K
```

**evmap** was evaluated as a replacement — theoretically O(1) vs SkipMap's O(log n). Benchmarks showed evmap wins at small scale but loses at large scale:

```
Accounts    evmap TPS    SkipMap TPS    Winner
──────────────────────────────────────────────
1K          4.21M        ~1.4M          evmap
1M          1.20M        ~960K          evmap
10M         748K         ~755K          SkipMap
50M         627K         ~460K          SkipMap
```

evmap is theoretically O(1) but degrades at large account counts due to `refresh()` synchronization cost, reader contention, and maintaining two full internal copies of the map. SkipMap is O(log n) but wins at scale due to predictable memory access and no synchronization overhead.

Both are the wrong data structure. The fundamental insight is that account IDs are sequential by design — this enables direct array indexing with zero overhead.

---

## Decision

Replace all balance storage structures with flat `Vec`-based arrays using account ID as a direct index.

### Core insight

Account IDs are sequential and guaranteed contiguous — you cannot have `account_id=78` without also having `account_id=77`. This constraint enables direct array indexing:

```
balance = vec[account_id]   // single CPU instruction, O(1), no hashing, no traversal
```

### Structures

**Transactor — plain Vec:**
```rust
balances: Vec<u64>
// read:  balances[account_id as usize]
// write: balances[account_id as usize] = new_balance
```

Single contiguous allocation. CPU prefetcher works perfectly. No hashing, no pointer chasing, no tree traversal. Single writer — no synchronization needed.

**Snapshotter — Vec of atomics:**
```rust
balances: Vec<AtomicU64>
// read:  balances[account_id as usize].load(Ordering::Acquire)
// write: balances[account_id as usize].store(value, Ordering::Release)
```

Same contiguous layout. External balance reads are a single array index plus atomic load. No map lookup, no synchronization beyond the atomic operation itself.

### Capacity management

The ledger accepts `max_accounts` at configuration time:

```rust
pub struct LedgerConfig {
    pub max_accounts: usize,
}
```

Both Vecs are pre-allocated at startup — zero reallocation during operation, zero rehashing stalls. If `account_id >= max_accounts`, the operation is rejected with `FailReason::AccountLimitExceeded` before reaching the Transactor.

**Resize at runtime:**
```rust
fn resize(&mut self, new_size: usize) {
    self.balances.resize(new_size, 0u64);  // new slots zero-initialized
    // existing balances untouched
}
```

Resize increases memory allocation only — existing account data is never affected.

**Initialization note:** `Vec<AtomicU64>` cannot use `vec![]` macro since `AtomicU64` is not `Copy`. Use:
```rust
(0..max_accounts).map(|_| AtomicU64::new(0)).collect::<Vec<_>>()
```

### Snapshot isolation

The Snapshotter owns its `Vec<AtomicU64>` exclusively as single writer. Consistent point-in-time snapshots are achieved via a checkpoint mechanism:

```rust
struct Balance {
    snapshot:      AtomicU64,   // always current, external reads
    checkpoint:    AtomicU64,   // frozen at checkpoint time, disk writes
    checkpoint_id: AtomicU8,    // which checkpoint this balance belongs to
}

// Global state
checkpoint_requested: AtomicBool  // SnapshotStorer requests
checkpoint_mode:      AtomicBool  // SnapshotRunner controls
current_checkpoint:   AtomicU8    // SnapshotRunner increments
checkpoint_tx_id:     AtomicU64   // set at freeze moment
checkpoint_wal_id:    AtomicU64   // set at freeze moment
```

**Checkpoint sequence:**

```
1. SnapshotStorer sets checkpoint_requested = true

2. SnapshotRunner catches request at transaction boundary
   (pending_entries == 0, clean state)
   → stores checkpoint_tx_id, checkpoint_wal_id
   → increments current_checkpoint (N → N+1)
   → sets checkpoint_mode = ON
   → clears checkpoint_requested = false

3. While checkpoint_mode = ON:
   SnapshotRunner updates balance.snapshot normally
   SnapshotRunner does NOT update balance.checkpoint
   New entries written with checkpoint_id = N+1

4. SnapshotStorer iterates Vec — reads balance.checkpoint
   All values consistent at checkpoint_tx_id (still at N)
   Writes to snapshot.bin

5. SnapshotStorer sets checkpoint_mode = OFF

6. SnapshotStorer iterates Vec again
   For each balance where checkpoint_id == N+1:
     CAS(balance.checkpoint, old_frozen_value, balance.snapshot.load())
     CAS ensures no accidental overwrite if snapshot updated mid-iteration

7. Done — all balances caught up, ready for next checkpoint
```

With `Vec`, steps 4 and 6 are sequential scans of a contiguous array — maximally cache-friendly, extremely fast even at 100M accounts.

### Memory comparison

```
Structure              10M accounts    50M accounts    100M accounts
───────────────────────────────────────────────────────────────────
HashMap<u64,u64>       ~480MB          ~2.4GB          ~4.8GB
evmap (2 copies)       ~960MB          ~4.8GB          ~9.6GB
SkipMap                ~600MB          ~3.0GB          ~6.0GB
Vec<u64>               80MB            400MB           800MB
Vec<AtomicU64>         80MB            400MB           800MB
Both Vecs combined     160MB           800MB           1.6GB
```

6x less memory than evmap. Zero fragmentation. Zero allocator overhead.

---

## Consequences

### Positive

- O(1) balance read and write — single array index, no hashing, no traversal
- CPU prefetcher works perfectly — contiguous memory, sequential access pattern
- 6x memory reduction vs evmap at all scales
- Zero rehashing stalls — single pre-allocation at startup
- Performance flat across all account counts — no crossover degradation point
- Snapshot iteration is a sequential array scan — fast and consistent at any scale
- Simpler code — `vec[id]` replaces all map lookup logic

### Negative

- Sequential account ID constraint — IDs must be assigned monotonically, gaps not allowed
- `max_accounts` must be configured upfront — operations beyond limit are rejected
- Memory reserved upfront for full capacity — at 100M accounts: 1.6GB at startup regardless of actual account count
- Resize requires reallocation of backing array

### Neutral

- Account ID assignment is the ledger's responsibility — external systems cannot use arbitrary IDs
- Sparse workloads are not supported — by design, not a limitation

---

## Alternatives considered

**evmap**
Evaluated and rejected as primary store — degrades at 10M+ accounts despite theoretical O(1) advantage. Benchmarks confirmed 15-27% TPS regression vs SkipMap at large scale due to refresh synchronization overhead and two-copy memory layout.

**HashMap<u64, u64>**
Rejected — hash computation overhead, non-contiguous memory, 6x memory overhead vs Vec.

**SkipMap (crossbeam)**
Rejected — O(log n) traversal, pointer chasing, designed for MPMC which single-writer pipeline does not need.

**DashMap**
Rejected — sharded RwLock contention, MPMC overhead not needed for single-writer pipeline.

## References

- ADR-001 — entries-based execution model, pipeline stage ownership
- CPU cache line optimization — Intel optimization reference manual
- Mechanical sympathy — contiguous memory layout for maximum hardware utilization