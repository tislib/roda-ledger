# ADR-021: WAL is the Sole Ring Releaser; Snapshot Tails the Durable WAL

**Status:** Accepted  
**Date:** 2026-06-07  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-019 (Transaction Ring) — moves the ring releaser from the snapshot stage to the WAL and changes the reclamation invariant; the ring becomes a single-reader (SPSC) channel.
- ADR-006 — the snapshot stage no longer reads the in-memory ring; it indexes by tailing the durable WAL.

---

## Context

ADR-019 made the **snapshot** stage the sole ring releaser, advancing the reclamation
point only up to the durability watermark (`write ≥ persisted ≥ durable ≥ indexed =
reclaimed`). Two readers — WAL and snapshot — shared the ring, and the snapshot freed
slots once it had indexed them.

That couples ring capacity to the snapshot's indexing pace. From the writer's view the
occupied ring is `compute − snapshot = (compute − commit) + (commit − snapshot)`, so a
lagging snapshot consumes ring slots and **back-pressures the transactor through the
ring**. Load tests made this visible: the per-stage backlog `commit − snapshot` was
bursty and, combined with the WAL gap, pushed ring occupancy toward its capacity, capping
write throughput on the snapshot's indexing speed even though indexing is not on the
durability path.

The snapshot does not actually need the *in-memory* ring: it needs the ordered, committed
record stream, which the WAL already persists. A `WalTailer` that streams the durable WAL
already exists (it is how cluster followers replicate).

---

## Decision

Make the ring a single-reader **SPSC** channel and move indexing off it:

1. **The WAL is the sole ring reader and releaser.** It frees each slot once it has
   *written* the batch to the segment (`write_pending_entries`), before `fdatasync`. The
   transactor is the sole writer; the WAL is the sole reader; no other stage touches the
   ring. Placing release after the **write** (not the in-memory copy) makes the disk
   writer the flow-control point: a stalled/slow writer holds the ring and back-pressures
   the transactor, while `fdatasync`/commit is deliberately *not* a backpressure point —
   the page cache and OS write-back absorb the write-vs-sync gap.

2. **The snapshot tails the durable WAL.** Instead of `ring.walk_entries`, the snapshot
   stage streams committed records via a new `WalTailer::tail_entries(handler)` — the
   durable-WAL twin of `walk_entries`. The tailer owns its read position and re-delivers a
   transaction group whole if the handler defers it. The snapshot's handler is otherwise
   unchanged: it buffers a transaction's followers until the closing `TxMetadata`, applies
   index + balances together, and still gates visibility on `commit_index` (a group with
   `tx_id > commit_index` is held until committed — "invisible until durable").

Recovery is untouched: it remains the sole owner of the recovery phase, rebuilds the index
into the same `Snapshot.indexer`, and sets `snapshot_index`; the runtime tailer resumes at
`snapshot_index + 1` on that same index — no shared code, no gap, no double-apply.

### The reclamation invariant changes

Reclamation is now gated on the **disk write**, not durability:

```
write ≥ release (= written, pre-fsync) ≥ commit (= fdatasync'd) ≥ snapshot (= indexed)
```

`release` may now *lead* `commit` and `snapshot`. This is safe: `append_pending_entry`
byte-copies the record out of the ring slot before the releaser advances, so the writer
can only ever overwrite a slot it already owns. Releasing a slot is pure process-memory
recycling — it makes no transaction "committed". Durability is unchanged: `commit_index`
still advances only after `fdatasync`, and the snapshot still exposes only committed
transactions to queries. Crash-before-fsync is handled by recovery exactly as before.

The single-reader rule is what makes release-on-ingest sound: with the snapshot off the
ring there is no second reader that could fall below `release`. Both halves therefore land
together.

---

## Consequences

### Positive
- **Snapshot can no longer back-pressure writes.** Ring occupancy drops to `compute −
  ingest` (≤ `compute − commit`), bounded by the WAL's memory-copy rate, not the
  snapshot's indexing rate. The transactor is decoupled from indexing. A 20 s load test
  (Apple M2 Max, `bench()` config) rose from ~7.3M to ~8.35M TPS, with the ring no longer
  pinned near capacity.
- **Simpler transport.** The ring is a clean SPSC channel (one writer, one
  reader/releaser); release-on-ingest frees slots promptly.
- **Index path converges with recovery and replication** — all three consume the WAL.

### Negative
- **`fdatasync` is no longer a flow-control point.** A stalled fsync no longer directly
  caps submission through the ring (release precedes commit); the page cache and OS
  dirty-page write-back are what eventually bound a sustained fsync stall. Backpressure
  under a slow/stalled *disk writer* is preserved, since release follows the write — see
  `backpressure_test.rs` (write-path only; the fsync-backpressure tests were removed).
- **The snapshot re-reads the WAL** (page cache) instead of memory, adding read bandwidth
  off the critical path.
- **Unbounded snapshot lag under sustained max write load.** ADR-019's design self-throttled
  `commit − snapshot` via ring back-pressure; decoupled, it can grow (the load test showed
  the snapshot ~7–9M transactions behind a ~8.3M TPS commit stream while keeping up at
  ~95% of that rate). Queries are correspondingly staler under load, and the lag must stay
  within the indexer's circular window. The snapshot tailer (`pread` + parse + index) is
  now the slowest stage and the next optimization target if query freshness matters.

### Neutral
- The reclamation party moves from the snapshot to the WAL; `WalTailer` gains
  `tail_entries` but its existing API (used by cluster replication) is unchanged.
- With the snapshot off the ring, `TxRing` was collapsed to a true **SPSC** pair:
  `TxRing::new()` now returns `(TxRingWriter, TxRingReader)` — the reader both reads and
  releases — and the ring lives behind an `Arc` held by those two handles (dropped when
  both drop), no longer owned by the `Pipeline`. The old multi-reader surface
  (`walk_entries`/`get`/`write_index` on `TxRing`, and the separate `TxRingReleaser`) is
  gone; the WAL reads via `TxRingReader::walk` and frees via `release_to`.

---

## References
- ADR-019 — Transaction Ring (amended: releaser + reclamation invariant)
- ADR-006 — WAL, Snapshot, Seal Durability (amended: snapshot index source)
- ADR-015 — Cluster Mode (the `WalTailer` reused here)
- ADR-020 — Trailer Metadata (the commit-record layout `tail_entries` groups by)
