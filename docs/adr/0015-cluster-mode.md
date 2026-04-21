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

Two additions to Ledger. Nothing else changes.

```rust
/// Append pre-validated WAL entries produced externally (follower path).
///
/// Uses `&self` — internally only pushes onto the WAL input queue
/// (lock-free), so no mutable borrow is required. The follower's own
/// WAL stage is responsible for writing and fsyncing.
pub fn append_wal_entries(&self, entries: Vec<WalEntry>) -> Result<()>;

/// Build a stateful raw-WAL byte tailer bound to this ledger's storage.
/// The tailer holds one open `File` handle + byte position across calls
/// so successive reads pay only for the new bytes.
pub fn wal_tailer(&self) -> WalTailer;
```

```rust
impl WalTailer {
    /// Stream WAL bytes starting at `from_tx_id` into the caller-supplied
    /// buffer. Returns bytes written (always a multiple of 40). The cursor
    /// resumes across calls on a monotonically non-decreasing `from_tx_id`;
    /// a strictly smaller value re-seeks.
    pub fn tail(&mut self, from_tx_id: u64, buffer: &mut [u8]) -> u32;
    pub fn reset(&mut self);
}
```

`append_wal_entries` accepts a `Vec<WalEntry>` directly, bypassing the
Transactor. The WAL input queue was generalised to carry either a
`WalInput::Single(WalEntry)` (the Transactor's path) or a
`WalInput::Multi(Vec<WalEntry>)` (the follower's path); both variants
share the same single consumer inside the WAL stage.

`WalTailer` lets the Cluster layer read raw WAL bytes from a checkpoint
without coupling Cluster to Ledger's internal WAL types. It lives in the
storage layer (`storage::wal_tail`) and uses positional reads (`pread`)
against the segment files directly — it does **not** load `Segment`
objects, avoiding the full-file read + clone that an earlier prototype
suffered from.

Rotation is detected by inode comparison: the tailer stashes the inode of
`wal.bin` at open time and, on EOF with a mismatched on-disk inode,
treats its file as a now-sealed `wal_{id:06}.bin` and advances to the
next segment (sealed if present, else the new active).

---

## Architecture

```
┌──────────────────────────────────────────┐
│  Cluster                                 │
│                                          │
│  ┌──────────┐   AppendEntries  ┌───────┐ │
│  │  Leader  │ ─────────────▶   │ Peer  │ │
│  │          │ ◀─ last_tx_id ── │ (F)   │ │
│  │  Ledger  │                  │Ledger │ │
│  └──────────┘                  └───────┘ │
└──────────────────────────────────────────┘
```

On the **leader**:

- Client submits a transaction.
- Leader runs it through the Transactor (existing path, unchanged).
- Leader's WAL stage writes + fsyncs locally (unchanged).
- A per-peer replication task tails the leader's WAL via `WalTailer`,
  ships bytes via `AppendEntries` RPC, and advances its own
  `from_tx_id` watermark on every accepted response.

On a **follower**:

- Receives `AppendEntries` bytes; decodes them with
  `decode_records(&[u8]) -> Vec<WalEntry>`, calls `append_wal_entries`.
- Handler returns immediately with the follower's current
  `last_commit_id`. Fsync is **not** awaited in the RPC path; the
  follower's WAL stage fsyncs on its own schedule.
- The Transactor does **not** run on followers. Followers are read-only:
  their client-facing Ledger gRPC handler runs in `read_only` mode,
  rejecting every `submit_*` / `register_function` RPC with
  `FAILED_PRECONDITION`.

---

## Lagged Replication (replaces Two-Phase)

Replication is **single-phase and lagged**: every `AppendEntries` call
writes the shipped bytes to the follower's WAL input queue and returns
immediately with the follower's **current** `last_commit_id` — fsync
completes asynchronously in the follower's own WAL stage.

```
Leader                    Follower
  │── AppendEntries ─────▶│  append_wal_entries (queued + written)
  │                        │  (fsync happens in background)
  │◀── Ack(last_tx_id) ────│  returns follower.last_commit_id NOW
  │     (lagged;           │
  │      prev fsync point) │
  │── AppendEntries ─────▶│
  │◀── Ack(last_tx_id) ────│  may now reflect previous batch's fsync
  │     (moves forward)    │
```

Rationale:

- **No fsync on the critical RPC path** — the write RPC is never blocked
  by disk latency. The leader ships the next chunk as fast as its tailer
  produces bytes.
- **Follower commit watermark is observable, not authoritative** — the
  `last_tx_id` returned by each RPC reflects whatever the follower's WAL
  stage has fsynced by the moment the handler runs. The leader learns
  the latest value on every subsequent call, which is sufficient for
  quorum tracking (see below) and for `load_cluster` observability
  (`repl-lag`, `min_lag`, `max_lag`).
- **This is close to etcd's model** — writes durable-by-fsync on a
  background cadence, with the observable commit index lagging the
  applied index by a bounded amount.

Discarded alternatives:

- **Two-phase (Write + separate Commit RPC)** — the original ADR draft
  called for a second RPC to advance the commit watermark after quorum
  fsync acks. Dropped: it doubles RPC count, serialises commits behind
  the slowest follower's fsync latency, and does not improve durability
  over the lagged model once quorum tracking (next section) is in place.
- **Blocking on follower fsync inside `AppendEntries`** — trivially
  correct but halves write throughput when any follower's disk is slow.

---

## Quorum Tracking (deferred, scaffolded)

Leader commit advancement gated by quorum fsync acks is **not yet
implemented**. The scaffolding is in place:

- Every `AppendEntriesResponse` already carries the follower's
  `last_tx_id`.
- `PeerReplication` tracks each peer's `peer_last_tx` independently.
- The `PeerManager` layer is the natural place to aggregate per-peer
  watermarks and emit a cluster-wide commit index.

Until implemented, the leader's own `last_commit_id` advances
independently from its own WAL stage, as in single-node mode. The
Cluster layer is purely a fan-out replicator.

---

## What Cluster Owns

| Concern                        | Owner   |
|--------------------------------|---------|
| Transaction execution          | Ledger  |
| WAL write / fsync              | Ledger  |
| WAL binary streaming           | Ledger (`WalTailer`) |
| Entry application on follower  | Ledger (`append_wal_entries`) |
| Read-only mode on followers    | Cluster (`LedgerHandler::new_read_only`) |
| Leader election                | Cluster → ADR-016 |
| Replication RPCs               | Cluster |
| Per-peer tailer cursor         | Cluster (`PeerReplication`) |
| Peer supervision               | Cluster (`PeerManager`) |
| Quorum tracking                | Cluster (scaffolded, not implemented) |
| Historical WAL backfill        | Cluster (`FetchSegment`, planned) |
| Follower catch-up              | Cluster |
| Node membership (static)       | Cluster (`ClusterConfig::peers`) |

---

## Task Topology

The Cluster layer uses a two-layer supervisor topology:

- **`PeerManager`** — one supervisor task per leader. This is the only
  top-level `tokio::spawn` in the replication subsystem. It owns the
  `running: Arc<AtomicBool>` shutdown flag and spawns one child per peer.
- **`PeerReplication`** — one child async task per follower. Owns its
  own `WalTailer` cursor, gRPC client, `from_tx_id`, and `peer_last_tx`
  state. Retries transport / logical rejects in place; reconnects on
  transport failure.

A single `tokio::spawn` from the caller's perspective; fan-out is
internal.

---

## Node gRPC Service

The Cluster listens on a second gRPC port (separate from the client-facing
Ledger service) and speaks the `Node` service (`proto/node.proto`).

Implemented:

- `AppendEntries(AppendEntriesRequest) -> AppendEntriesResponse` — ships
  raw WAL bytes + metadata from leader to follower. Response carries
  `term`, `success`, `last_tx_id` (follower's current commit watermark),
  `reject_reason`.
- `Ping(PingRequest) -> PingResponse` — health + role probe. Returns
  `node_id`, `term`, `last_tx_id`, `role`, echoed `nonce`. Not in the
  original ADR; added for operational visibility.

Scaffolded (proto messages exist; handlers return `UNIMPLEMENTED`):

- `RequestVote` — deferred to ADR-016.
- `InstallSnapshot` — deferred to ADR-016.

`AppendEntriesRequest` carries `term`, `prev_tx_id`, `prev_term` fields
for future term-fenced writes; the current follower does not enforce
them (ADR-016 work).

### gRPC message sizing

The Node server and client both raise `max_decoding_message_size` and
`max_encoding_message_size` to `append_entries_max_bytes * 2 + 4 KiB` to
cover protobuf framing overhead. Tonic's default 4 MiB limit would
otherwise reject chunks sized at exactly `append_entries_max_bytes`.

---

## Historical WAL Backfill

Planned (not yet implemented). When a follower joins or rejoins after
extended downtime, it needs the full WAL history (not just post-snapshot
state) to serve `GetTransaction` queries.

Design:

1. Leader exposes sealed WAL segments via `FetchSegment(segment_id)` RPC.
2. Follower requests missing segments in order.
3. Leader streams bytes using its `WalTailer` (or a dedicated reader).
4. Follower writes them as sealed segments directly.
5. The follower is live for `AppendEntries` before backfill completes;
   backfill runs on a separate background channel so it never blocks
   the hot replication path.

The RPC is not yet defined in `proto/node.proto`. Without it, followers
that fall too far behind the leader's retention window will need a full
snapshot install (`InstallSnapshot`, ADR-016).

---

## Static Configuration

Cluster membership is static per process. Each node reads a
`cluster.toml` deserialised into `ClusterConfig`:

```toml
mode        = "leader"     # or "follower"
node_id     = 1
term        = 1            # static; term bumps → ADR-016
replication_poll_ms      = 5    # idle-poll when tailer returns 0
append_entries_max_bytes = 4194304

[[peers]]
id        = 2
node_addr = "http://127.0.0.1:50062"

[server]       # client-facing Ledger gRPC service
host = "0.0.0.0"
port = 50051

[node]         # peer-facing Node gRPC service
host = "0.0.0.0"
port = 50061

[ledger]
# ... standard LedgerConfig ...
```

Dynamic membership changes are out of scope (see §Out of Scope).

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

**Two-phase replication (Write + Commit RPC)** — doubles RPC count and
serialises commit advancement behind the slowest follower's fsync.
Replaced by the lagged single-phase model above; the leader's commit
watermark advances from its own WAL stage, and quorum observation uses
the `last_tx_id` returned on every `AppendEntries` response.

**Blocking `AppendEntries` on follower fsync** — halves write throughput
whenever a follower's disk is slow. Rejected for the same reason the
local WAL stage decouples write and fsync (ADR-011).

---

## Out of Scope

- Leader election and term management → ADR-016
- Term-fenced writes (proto fields scaffolded, not enforced) → ADR-016
- Dynamic membership changes
- Read-from-follower semantics (the follower's read RPCs are exposed
  today purely as a side-effect of running the Ledger gRPC service in
  read-only mode; formal semantics → ADR-016)
- Quorum-gated leader commit (scaffolded, not implemented)
- `FetchSegment` RPC and historical backfill (designed, not implemented)

---

## References

- ADR-006 — WAL, snapshot, seal durability
- ADR-010 — Wait levels (`WaitLevel::Committed`)
- ADR-011 — WAL write/commit separation
- Ongaro & Ousterhout 2014 — Raft
- Liskov & Cowling 2012 — Viewstamped Replication Revisited
- etcd — lagged-commit replication model
