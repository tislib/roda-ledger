# ADR-013: Transaction-Count-Based WAL Segments and Deterministic Deduplication Window

**Status:** Proposed  
**Date:** 2026-04-14  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-006 — replaces size-based segment rotation with transaction-count-based rotation
- ADR-009 — replaces time-based deduplication window with transaction-count-based window; removes `dedup_enabled` and `dedup_window_ms` config

---

## Context

The current WAL segment rotation is based on file size (`wal_segment_size_mb`). This
makes segments predictable in disk usage but unpredictable in transaction count —
a segment may contain anywhere from thousands to millions of transactions depending
on entry counts per transaction. This unpredictability cascades into two problems:

**1. Idempotency window is time-based and configurable — fragile.**
The deduplication flip-flop cache (ADR-009) uses `dedup_window_ms` (wall-clock time)
to decide when to flip. Wall-clock windows are:
- Non-deterministic under load spikes (a burst may push past the window before
  clients can retry)
- Disconnected from the WAL segment lifecycle — the dedup window and the segment
  boundary are independent, making recovery reasoning harder
- Configurable (`dedup_enabled`, `dedup_window_ms`) — operators can misconfigure
  or disable deduplication, breaking idempotency guarantees

**2. Hot index sizing is guesswork.**
`index_circle1_size` and `index_circle2_size` are hardcoded powers of two
(`1 << 20`, `1 << 21`) with no relation to actual segment capacity. Under- or
over-sizing wastes memory or causes hash collisions.

**3. Segment size in MB gives no guarantee about transaction capacity.**
Operators reason about "how many transactions fit in the active segment" but the
answer depends on average `entry_count` per transaction — unknowable at config time.

---

## Decision

### 1. Transaction-count-based segment rotation

Replace `wal_segment_size_mb` with `transaction_count_per_segment` in `StorageConfig`:

```rust
pub struct StorageConfig {
    pub data_dir: String,
    pub temporary: bool,
    pub transaction_count_per_segment: u64,  // replaces wal_segment_size_mb
    pub snapshot_frequency: u32,
}
```

Default: `10_000_000` (10 million transactions per segment).

Segment files become dynamically sized on disk — a segment with many complex
transactions will be larger than one with simple deposits. The trade-off is
accepted: what matters is that the **active window** (number of transactions in
the active segment) is known and fixed.

Segment rotation triggers when `tx_id` crosses the next segment boundary:
```
if tx_id % transaction_count_per_segment == 0 → seal current, open new segment
```

### 2. Deduplication window = active window (always on, not configurable)

The deduplication flip-flop cache window is no longer time-based. Instead:

- **Active window** = last N transactions where N = `transaction_count_per_segment`
- The dedup cache covers **at least** the active window (one full segment worth)
- The flip-flop mechanism remains: `active` + `previous` HashMaps give coverage
  of N to 2N transactions
- Flip occurs when `tx_id` crosses the segment boundary (same trigger as rotation)

**Removed config fields:**
```rust
// REMOVED from LedgerConfig:
pub dedup_enabled: bool,
pub dedup_window_ms: u64,
```

Deduplication is **always on**. There is no configuration to disable it. `user_ref = 0`
still skips the check (no idempotency key provided), preserving the opt-out for
individual transactions.

**Rationale:** Deduplication is a correctness feature, not a performance toggle.
Allowing it to be disabled creates a class of bugs that only appears in
misconfigured deployments. The overhead for `user_ref = 0` transactions is zero
(the check is skipped), so there is no performance argument for disabling it globally.

### 3. DedupCache changes

```rust
pub struct DedupCache {
    active: FxHashMap<u64, u64>,     // user_ref -> tx_id
    previous: FxHashMap<u64, u64>,   // user_ref -> tx_id
    window_size: u64,                // = transaction_count_per_segment
    window_start_tx_id: u64,         // tx_id when active window started
}
```

**Flip logic — checked on every transaction:**
```
if tx_id - window_start_tx_id >= window_size:
    swap(active, previous)
    active.clear()
    window_start_tx_id = tx_id
```

No wall-clock dependency. Flip is deterministic and tied to transaction sequence.

**Recovery:** Rebuild from WAL replay. For each committed transaction with
`user_ref != 0` and `tx_id` within the last `2 * window_size` transactions,
insert into `active` or `previous` based on `tx_id` position relative to the
current window boundary.

### 4. Index circle sizes derived from active window

```rust
// REMOVED from LedgerConfig (no longer hardcoded):
pub index_circle1_size: usize,
pub index_circle2_size: usize,
```

Instead, derive from `transaction_count_per_segment`:

```rust
impl LedgerConfig {
    pub fn index_circle1_size(&self) -> usize {
        self.storage.transaction_count_per_segment as usize
    }

    pub fn index_circle2_size(&self) -> usize {
        self.storage.transaction_count_per_segment as usize * 2
    }
}
```

Hot index sizes match the active window exactly. Circle 1 covers the active
segment, circle 2 covers active + previous (matching the dedup window).

---

## Config Changes

### StorageConfig

| Before | After |
|--------|-------|
| `wal_segment_size_mb: u64` (default: 64) | `transaction_count_per_segment: u64` (default: 10_000_000) |

### LedgerConfig

| Removed | Reason |
|---------|--------|
| `dedup_enabled: bool` | Dedup is always on |
| `dedup_window_ms: u64` | Window is transaction-count-based, derived from `transaction_count_per_segment` |
| `index_circle1_size: usize` | Derived from `transaction_count_per_segment` |
| `index_circle2_size: usize` | Derived from `transaction_count_per_segment * 2` |

### config.toml

```toml
[storage]
data_dir = "data/"
transaction_count_per_segment = 10000000  # 10M transactions per segment
snapshot_frequency = 4
```

Removed:
```toml
# REMOVED:
# wal_segment_size_mb = 64
# dedup_enabled = true
# dedup_window_ms = 10000
```

---

## Consequences

### Positive

- **Predictable active window** — operators know exactly how many transactions
  fit in the active segment; idempotency window is deterministic
- **Deduplication always on** — eliminates misconfiguration risk; correctness
  guarantee is unconditional
- **No wall-clock dependency** — dedup flip is tied to transaction sequence, not
  system time; deterministic under any load pattern
- **Index sizes match reality** — hot index capacity is derived from segment
  capacity, not hardcoded guesses
- **Single knob** — `transaction_count_per_segment` controls segment rotation,
  dedup window, and index sizing; fewer config fields to reason about
- **Recovery is simpler** — dedup rebuild uses `tx_id` ranges, not timestamp
  comparisons against wall clock

### Negative

- **Variable segment file sizes** — disk usage per segment depends on average
  transaction complexity; operators cannot predict exact disk usage per segment
- **Breaking config change** — `wal_segment_size_mb`, `dedup_enabled`,
  `dedup_window_ms` removed; existing config files must be updated
- **WAL format unchanged but rotation logic changes** — existing WAL files remain
  readable, but segment boundaries will differ after migration
- **Cannot disable dedup** — workloads that never use `user_ref` pay no runtime
  cost (check skipped for `user_ref = 0`) but the DedupCache struct is always
  allocated

### Neutral

- Flip-flop HashMap mechanism unchanged — same data structure, different trigger
- `user_ref = 0` behavior unchanged — still skips dedup check
- WAL record format unchanged — no migration needed for existing WAL files
- `TxLink { kind: Duplicate }` behavior unchanged

---

## Alternatives Considered

**Keep time-based dedup with configurable window**
Rejected — time-based windows are non-deterministic and configurable, creating
a class of correctness bugs. Transaction-count-based windows are deterministic
and tied to the segment lifecycle.

**Make dedup window a separate config from segment size**
Rejected — the dedup window should always match the active segment to ensure
idempotency covers at least the active window. Two independent knobs invite
misconfiguration with no benefit.

**Use a fixed-size circular buffer instead of flip-flop**
Rejected — same reasoning as ADR-009: hash collisions cause false positives.
Flip-flop HashMap gives exact match with bounded memory.

**Cap segment file size as a secondary limit**
Considered but deferred — if segment files grow too large due to high-entry
transactions, a secondary size limit could be added later. For now, the
transaction count is the sole rotation trigger.

---

## References

- ADR-006 — WAL segment lifecycle, storage layout (amended by this ADR)
- ADR-009 — TxLink, deduplication, flip-flop HashMap (amended by this ADR)
- `src/config.rs` — StorageConfig, LedgerConfig
- `src/dedup.rs` — DedupCache
- `src/transactor.rs` — segment rotation, dedup integration
