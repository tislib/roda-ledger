# ADR-015: Cluster Mode

**Status:** Proposed  
**Date:** 2026-04-20

---

## Context

Ledger is a single-node engine. This ADR introduces a `Cluster` layer that
provides replication and high availability on top of Ledger **without modifying
Ledger's core logic**.

The guiding constraint: Ledger remains a correct, standalone, single-node
component. Cluster is a separate module that wraps Ledger and owns all
distributed-systems concerns.

---

## Minimal Ledger Surface (the contract)

Three additions to Ledger. Nothing else changes.

```rust
/// Append pre-validated WAL entries produced externally (follower path).
pub fn append_wal_entries(&mut self, entries: Vec<WalEntry>) -> Result<()>;

/// Stream WAL bytes from a given tx_id into caller-supplied buffer.
/// Returns bytes written. Enables leader-to-follower WAL shipping.
pub fn tail_wal_binary(
    &self,
    from_tx_id: u64,
    buffer: &mut [u8],
) -> u32;
```

`append_wal_entries` accepts a `Vec<WalEntry>` directly, bypassing the
Transactor. This mirrors how the Transactor already passes buffered entries
into the WAL — follower replication uses the same path. It may also improve the
internal Transactor→WAL communication, but that is out of scope here.

`tail_wal_binary` allows the Cluster layer to read raw WAL bytes from a
checkpoint, without coupling Cluster to Ledger's internal WAL types.

---

## Architecture

```
┌──────────────────────────────────────────┐
│  Cluster                                  │
│                                          │
│  ┌──────────┐   Write RPC   ┌──────────┐ │
│  │  Leader  │ ──────────────▶ Follower  │ │
│  │          │   Commit RPC  │          │ │
│  │  Ledger  │ ──────────────▶  Ledger  │ │
│  └──────────┘               └──────────┘ │
└──────────────────────────────────────────┘
```

On the **leader**:

- Client submits a transaction.
- Leader runs it through the Transactor (existing path, unchanged).
- Leader fans out WAL bytes to followers via **Write RPC** (no fsync wait).
- Leader and followers fsync in parallel.
- Once quorum has acked fsync, leader advances commit.
- Leader sends **Commit RPC** to followers with the committed `tx_id`.

On a **follower**:

- Receives Write RPC bytes; calls `append_wal_entries`.
- Fsyncs in its own batched cycle (separate from leader).
- Acks fsync to leader.
- On Commit RPC: advances its commit cursor.
- The Transactor does **not** run on followers. Followers are read-only.

---

## Two-Phase Replication

Replication is split into Write and Commit phases so both can proceed in
parallel, removing fdatasync from the critical path.

```
Leader          Follower A      Follower B
  │                │                │
  │── Write ──────▶│                │
  │── Write ────────────────────────▶│
  │  (own fsync)   │  (own fsync)   │  (own fsync)
  │◀── Ack ────────│                │
  │◀── Ack ─────────────────────────│
  │                │                │
  │── Commit ─────▶│                │
  │── Commit ───────────────────────▶│
```

The leader does not wait for follower fsyncs before issuing Write RPCs.
It waits for quorum fsync acks before issuing Commit RPCs. This means:

- Network transit and disk write on all nodes overlap.
- Per-transaction fsync cost is amortized over the batch, same as
  TigerBeetle's ~8,000 transfers per prepare.

---

## What Cluster Owns

| Concern                        | Owner   |
|-------------------------------|---------|
| Transaction execution          | Ledger  |
| WAL write / fsync              | Ledger  |
| WAL binary streaming           | Ledger (`tail_wal_binary`) |
| Entry application on follower  | Ledger (`append_wal_entries`) |
| Leader election                | Cluster |
| Replication RPCs               | Cluster |
| Quorum tracking                | Cluster |
| Historical WAL backfill        | Cluster |
| Follower catch-up              | Cluster |
| Node membership                | Cluster |

---

## Historical WAL Backfill

When a follower joins or rejoins after downtime, it needs the full WAL
history (not just post-snapshot state) to serve `GetTransaction` queries.

This is handled by a **background backfill channel** separate from Raft
consensus:

1. Leader exposes sealed WAL segments via `FetchSegment(segment_id)` RPC.
2. Follower requests missing segments in order.
3. Leader streams bytes using `tail_wal_binary`.
4. Follower writes them as sealed segments directly.
5. The follower is Raft-live before backfill completes; backfill runs in
   the background without blocking client traffic.

---

## Alternatives Rejected

**Raft log as a separate log alongside WAL** — requires two fsyncs per
commit (one for Raft log, one for WAL). Ruled out; halves throughput.

**Raft log entries reference WAL byte ranges** — followers have no WAL
yet; they must receive bytes, not references. Pre-consensus WAL writes
break the "committed implies durable" guarantee.

**Transactor runs on followers** — violates single-writer invariant.
Followers apply pre-validated `WalEntry` values, not raw client requests.

**Truncation / log rollback on conflict** — requires rolling back applied
balances. Ruled out. The pipeline's determinism and `Recover` path depend
on the WAL being append-only. Leader election handling (ADR-016) will use
term-fenced writes instead of truncation.

---

## Out of Scope

- Leader election and term management → ADR-016
- Dynamic membership changes
- Read-from-follower semantics

---

## References

- ADR-006 — WAL, snapshot, seal durability
- ADR-010 — Wait levels (`WaitLevel::Committed`)
- ADR-011 — WAL write/commit separation
- ADR-015 — Block-based WAL replication (steady-state design)
- Ongaro & Ousterhout 2014 — Raft
- Liskov & Cowling 2012 — Viewstamped Replication Revisited