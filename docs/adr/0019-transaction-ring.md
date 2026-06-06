# ADR-018: Transaction Ring — Single-Writer Inter-Stage Transport

**Status:** Accepted  
**Date:** 2026-06-05  
**Author:** Taleh Ibrahimli  

**Amends:**
- ADR-006 — replaces the queue-based hand-off between the transactor, WAL, and snapshot stages, and ties ring-slot reclamation to the durability watermark.
- ADR-011 — the WAL no longer *forwards* committed entries to the snapshot stage; both stages read the shared ring independently.

---

## Context

Before this change the pipeline moved WAL records between stages with two bounded
queues:

- **transactor → WAL** — the transactor staged a transaction's records in a
  temporary buffer, then enqueued them; the WAL stage dequeued and persisted them.
- **WAL → snapshot** — after a record became durable, the WAL stage re-enqueued it
  onto a second queue that the snapshot stage consumed.

This shape had three structural costs:

1. **Copies and staging.** Every record was copied at least twice (into the
   transactor→WAL queue, then into the WAL→snapshot queue), and the transactor had
   to build each transaction in a separate staging buffer before it could enqueue —
   an allocation and a copy per transaction.
2. **Coupling.** The snapshot stage could only observe records the WAL stage chose
   to forward. The two consumers were chained rather than independent, so the
   snapshot's view was a function of the WAL's forwarding cadence.
3. **No single reclamation invariant.** Durability (WAL `fdatasync`) and
   consumption (snapshot indexing) were tracked in different places, with the
   queues acting as the only backpressure signal.

The transactor produces a strictly ordered stream of records, and the WAL and
snapshot stages each need to read *every* record, at their own pace, for different
purposes (persistence vs. indexing). That is a single-producer, multiple-reader
problem — a shared ring fits it far better than a chain of copying queues.

---

## Decision

Introduce a single shared, lock-free **transaction ring** as the one transport
between the transactor and the read stages, replacing both queues. The transactor
also uses the ring directly as its per-transaction scratch space, eliminating the
staging buffer.

### Roles around the ring

```
              writes (sole producer)            copy-out reads
  transactor ───────────────►  TX RING  ──────────────────────────►  WAL (persist + fsync)
                                  ▲   │  ──────────────────────────►  snapshot (index)
                                  │   │
                  reclaims slots  │   └── release frontier (slot reuse)
                  (sole releaser) └────────────────────────────────  snapshot
```

- **One producer** — the transactor is the only writer. It appends records into
  ring slots and publishes them.
- **Many copy-out readers** — the WAL and snapshot stages each read the published
  stream independently, each at its own position. They copy records out of slots
  rather than borrowing them, because a slot may be reused once it is released.
- **One releaser** — the snapshot stage is the sole party that advances the
  reclamation point, and it does so **only up to the durability watermark**.

### Absolute addressing

Positions in the ring are absolute, monotonically increasing indices starting at
zero (mapped to physical slots only on access), rather than wrapping counters.
A write returns the absolute index it landed at; the transactor uses that index to
operate on the records of the in-flight transaction, and each reader tracks its own
absolute cursor. Absolute indices make "where is X" and "how far has each party
progressed" unambiguous across wraps.

### The transactor builds transactions in place

Instead of staging a transaction in a side buffer, the transactor writes its
metadata and sub-items directly into the ring's *uncommitted* region — the slots it
owns exclusively and that no reader can see yet. Within that region it:

- validates the transaction (e.g. the zero-sum invariant) by walking its own
  uncommitted records,
- back-fills the metadata once the sub-item count and checksum are known, and
- on success, **publishes the whole transaction atomically** by advancing the write
  frontier in a single step.

On failure it **discards the uncommitted tail**, so a rejected or partial
transaction is never visible to any reader. Readers therefore only ever observe
complete, validated transactions — publish-on-commit visibility.

### Durability-gated reclamation (the core invariant)

A slot's lifecycle is:

1. the transactor writes a record into the slot (uncommitted, private to the writer);
2. the transactor commits the transaction → the slot becomes visible to readers;
3. the WAL reads it, persists it, and `fdatasync`s → the durability watermark advances;
4. the snapshot reads it (only once it is at or below the durability watermark) and
   indexes it;
5. the snapshot, as releaser, advances the reclamation point past the slot → the
   transactor may reuse it.

Because the releaser only advances to the durability watermark, the frontiers are
always ordered `write ≥ persisted ≥ durable ≥ indexed = reclaimed`. A slot is reused
**only after the record it held is durable on disk *and* has been consumed by every
reader.** This makes "never overwrite a record that isn't yet durable" a single,
structural property of the transport rather than something each stage must enforce.

### Caller-driven, non-blocking backpressure

The producer never sleeps inside the ring. It reserves a window of free slots, fills
it, and reclaims more space as the releaser frees it. When no space is available the
transactor's own retry/wait-strategy loop decides how to back off, and it can abort
cleanly on shutdown. Backpressure is thus expressed at the call site, not hidden
inside the transport — consistent with the pipeline's non-blocking design.

### Consequences for the read stages

- The WAL stage reads the ring and persists; it no longer owns an input queue and no
  longer forwards anything to the snapshot.
- The snapshot stage reads the ring, indexes for query serving, and drives
  reclamation; its remaining queue carries only *query requests*, not records.
- The intermediate message types that wrapped records for the two queues are removed.

---

## Consequences

### Positive

- **Fewer copies, no per-transaction staging.** Records are written once into the
  ring; each reader copies out only what it consumes. The transactor's separate
  staging buffer is gone.
- **In-place transaction assembly.** Validation, checksumming, and rollback all
  operate on the ring's uncommitted region — the ring *is* the transactor's scratch
  space for the in-flight transaction.
- **Independent readers.** The WAL and snapshot consume the same published stream at
  their own pace; the snapshot no longer depends on the WAL's forwarding cadence.
- **One reclamation invariant.** Slot reuse is gated on durability *and* consumption
  in a single place, so the "don't overwrite undurable data" guarantee is structural.
- **Backpressure stays caller-driven** and the producer never blocks inside the
  transport.

### Negative

- **Concentrated unsafe code.** A lock-free ring with hand-managed memory ordering
  and a strict window discipline is inherently `unsafe`; correctness rests on those
  invariants and warrants careful, isolated review.
- **A transaction must fit in the ring.** Because a transaction is published
  atomically, the ring capacity must be at least the largest possible single
  transaction; otherwise the writer cannot make progress.
- **Readers cannot hold references across reclamation.** Consumers must copy records
  out; they cannot retain borrows into ring slots past the release boundary.
- **Durability-coupled backpressure.** If durability stalls, reclamation stalls, and
  the producer eventually blocks once the ring fills. This is the same coupling any
  bounded transport has, now explicitly tied to the durability watermark.

### Neutral

- Replaces the transactor→WAL and WAL→snapshot queues and their wrapper message
  types; the snapshot's query queue is unchanged in purpose.
- The reclamation party is fixed to the snapshot stage by design — it is the last
  reader and the one that observes the durability watermark.

---

## References

- ADR-001 — Entries-based execution model (the record types the ring carries)
- ADR-006 — WAL, Snapshot, and Seal Durability (durability lifecycle; amended)
- ADR-011 — WAL Write/Commit Separation (WAL→snapshot forwarding; amended)
- ADR-005 — Adaptive Pipeline Execution Mode (pipeline staging and backpressure context)
- ADR-010 — Sync Submit (submit-path backpressure semantics)
