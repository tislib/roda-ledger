# ADR-015: Cluster Mode

**Status:** Proposed
**Date:** 2026-04-20
**Last-Updated:** 2026-04-22

## 2026-04-22 Decisions

These decisions formalise changes made since the original draft. They
update the ADR's shape; implementation lives in the code.

1. **The Cluster layer owns all replication and all gRPC.**
   A "single node" is a cluster with zero peers. There is no separate
   server-only mode, no separate server binary, and no feature flag for
   gRPC that is distinct from cluster.

2. **One entry point, one configuration shape.**
   The same configuration object describes both single-node and
   multi-node deployments; the only difference is the peer list. There
   is no single-node-specific config type.

3. **The leader counts itself toward the quorum.**
   Previously the quorum tracker only observed peer acks, so a leader's
   own commit progress was invisible to majority calculations. The
   leader is now a first-class participant in quorum (Raft-style:
   majority of `peers + 1`). This is a correctness fix, not a new
   feature.

4. **Ledger exposes a commit-advance hook.**
   The Ledger now offers a single, narrow extension point: a callback
   fired when its commit index advances. The Cluster layer uses it to
   feed the leader's own commit progress into the quorum tracker. The
   hook is part of the minimal Ledger surface — it does not expand
   Ledger's responsibilities, only its observability.

5. **Cluster-commit is exposed as a client wait level.**
   Clients can submit a transaction and block until it has reached
   quorum across the cluster, not just the local leader. This is an
   opt-in durability guarantee on the submit path — callers who don't
   need it continue to use the local commit or snapshot wait levels.
   The leader's own commit watermark still advances from its own WAL
   stage; only the client's wait choice determines whether "committed"
   means locally or cluster-wide.

6. **Naming follows role, not transport.**
   Within the cluster module, types are named by their role (leader,
   follower, handler, server, config) rather than by the transport
   (gRPC) that implements them. Transport is an internal detail.

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

1. A follower-side write path that accepts a pre-validated batch and
   hands it to the WAL stage, bypassing the Transactor.
2. A stateful raw-WAL byte tailer for the leader side.
3. A one-shot hook that fires when the commit index advances, so the
   Cluster layer can observe the leader's own commit progress without
   Ledger having to know anything about clusters.

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
  `from_tx_id` watermark on every accepted response. The same task
  also emits **idle heartbeats** (empty `AppendEntries`) when the
  tailer has no new bytes, so the peer's `last_tx_id` keeps flowing
  back into `Quorum` after write traffic stops.

On a **follower**:

- Receives `AppendEntries` bytes; decodes them with
  `decode_records(&[u8]) -> Vec<WalEntry>`, calls `append_wal_entries`.
- Handler returns immediately with the follower's **current** (live)
  `last_commit_id`, read from the ledger on every RPC path (empty
  heartbeat, non-empty success, reject). Fsync is **not** awaited in
  the RPC path; the follower's WAL stage fsyncs on its own schedule.
- The Transactor does **not** run on followers. Followers are read-only:
  their client-facing Ledger gRPC handler runs in `read_only` mode,
  rejecting every `submit_*` / `register_function` RPC with
  `FAILED_PRECONDITION`.

---

## Role Separation: `Leader` / `Follower`

The Cluster layer is organised around two role-specific bring-up types:

```
Cluster::new(config) ──► embeds Ledger, Arc<Ledger>
        │
Cluster::run() ──────────► dispatch on config.mode
        │
        ├── ClusterMode::Leader   → Leader { config, ledger }.run()
        │                            → LeaderHandles
        └── ClusterMode::Follower → Follower { config, ledger }.run()
                                     → FollowerHandles
```

- **`Cluster`** is a thin dispatcher. It constructs and starts the
  embedded `Ledger`, then calls `Leader::run` or `Follower::run` based on
  `config.mode`. No role-specific logic lives in `Cluster` itself.
- **`Leader`** (in `src/cluster/leader.rs`) owns the full leader-side
  bring-up: writable client-facing gRPC server, peer-facing Node server
  advertising `NodeRole::Leader`, a shared `Arc<Quorum>` and
  cooperative `Arc<AtomicBool>` shutdown flag, and one
  `tokio::spawn`ed `PeerReplication` task per configured peer. There is
  no separate supervisor layer — the leader is the supervisor.
- **`Follower`** (in `src/cluster/follower.rs`) owns the follower-side
  bring-up: read-only client-facing gRPC server, peer-facing Node
  server advertising `NodeRole::Follower`. No replication tasks run
  here — incoming `AppendEntries` are applied via the `NodeHandler`
  directly.

`ClusterHandles` is the unified return type. It is a tagged enum
`{ Leader(LeaderHandles), Follower(FollowerHandles) }` with
accessors (`quorum()`, `running()`, `as_leader()`, `as_follower()`,
`client_handle()`, `node_handle()`, `abort()`) so generic callers
(tests, `load_cluster`, the `roda-cluster` binary) don't need to branch
on role.

Rationale for the split: each role has a disjoint set of long-lived
resources (a leader has peer tasks and a `Quorum`; a follower has
neither) and a different gRPC handler posture (writable vs read-only).
Putting the bring-up in one function produced pervasive branching on
`config.mode`; the separate types make the role contract explicit and
let each file change independently.

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
  quorum tracking (next section) and for `load_cluster` observability
  (`maj_lag`, `min_lag`, `max_lag`).
- **This is close to etcd's model** — writes durable-by-fsync on a
  background cadence, with the observable commit index lagging the
  applied index by a bounded amount.

### Idle Heartbeats

Because the follower's returned `last_tx_id` is always **one batch
stale** (queued-but-not-yet-fsynced at the instant of the reply), the
leader needs to re-poll after the last write to see the follower's
final commit. A per-peer heartbeat closes that gap: when
`tailer.tail(..)` returns 0 bytes, the peer task sends an empty
`AppendEntries` (zero `wal_bytes`) every `replication_poll_ms` and
updates `Quorum` from the response. Transport errors on heartbeats
are swallowed — the next cycle retries; heartbeats are purely
observational. Without them, `Quorum::get()` could remain pinned at
the previous batch's stale watermark indefinitely after write traffic
stops.

Discarded alternatives:

- **Two-phase (Write + separate Commit RPC)** — the original ADR draft
  called for a second RPC to advance the commit watermark after quorum
  fsync acks. Dropped: it doubles RPC count, serialises commits behind
  the slowest follower's fsync latency, and does not improve durability
  over the lagged model once quorum tracking (next section) is in place.
- **Blocking on follower fsync inside `AppendEntries`** — trivially
  correct but halves write throughput when any follower's disk is slow.

---

## Quorum

The leader aggregates per-peer watermarks into a cluster-wide
majority-committed index through `Quorum` (`src/cluster/quorum.rs`):

```rust
pub struct Quorum {
    match_index:    Vec<AtomicU64>,  // one slot per peer, indexed by peer_id
    majority_index: AtomicU64,       // cached majority-committed index
    majority:       usize,           // (peer_count / 2) + 1
}
```

**API.**

- `Quorum::new(peer_count) -> Self` — creates one `AtomicU64` per peer
  (initialised to 0) and pre-computes the quorum size.
- `advance(&self, peer_id: u32, index: u64)` — writes
  `match_index[peer_id]` with `Release`, snapshots every peer atomic
  with `Relaxed`, sorts descending, and publishes the
  `majority - 1`-th value via `majority_index.fetch_max(Release)`.
  `fetch_max` ensures the cached value never regresses even under
  concurrent `advance` calls from sibling peer tasks.
- `get(&self) -> u64` — single `Acquire` load of `majority_index`.
  Fully lock-free.
- `peer(&self, peer_id: u32) -> u64` — observability accessor for a
  single follower's last published index.

**Integration.** The leader is a first-class participant in the
quorum tracker. It occupies one slot and advances it from its own
commit-index hook; every peer occupies one additional slot and
advances it on each successful `AppendEntries` (including idle
heartbeats). Majority is computed over `peers + 1` nodes (Raft-style),
not over peers alone. External observers read the cluster-wide
watermark through the handle the leader exposes.

**Invariants.**

- Each node (leader or peer) has exactly one slot. The leader's slot
  reflects its local commit progress; each peer's slot reflects the
  watermark returned on its most recent RPC.
- The published majority is monotonically non-decreasing. A regression
  on any one slot (restart, stale response) never lowers the cached
  value.
- Lock-free. No allocation after construction beyond a small per-call
  snapshot.

**Uses today.** The majority watermark serves two purposes:

- **Client wait level.** `submit_and_wait` exposes a cluster-commit
  wait option that blocks the caller until the transaction's id is at
  or below the current quorum watermark. Clients that need
  cluster-level durability opt in here; clients that only need local
  commit do not pay for it.
- **Observability.** Tests, the cluster load generator, and monitoring
  tooling read the same watermark to report replication lag.

The leader still advances its own commit watermark from its local WAL
stage, independent of quorum. Making that advancement itself
quorum-gated (so even non-waiting clients implicitly get cluster-level
durability) is the remaining ADR-016 work.

---

## What Cluster Owns

| Concern                        | Owner   |
|--------------------------------|---------|
| Transaction execution          | Ledger  |
| WAL write / fsync              | Ledger  |
| WAL binary streaming           | Ledger (`WalTailer`) |
| Entry application on follower  | Ledger (`append_wal_entries`) |
| Role dispatch                  | Cluster (`node.rs`) |
| Leader-side bring-up           | Cluster (`Leader`) |
| Follower-side bring-up         | Cluster (`Follower`) |
| Read-only mode on followers    | Cluster (`LedgerHandler::new_read_only`) |
| Leader election                | Cluster → ADR-016 |
| Replication RPCs               | Cluster |
| Per-peer tailer cursor         | Cluster (`PeerReplication`) |
| Per-peer supervision           | Cluster (`Leader`, one `tokio::spawn` per peer) |
| Idle heartbeats                | Cluster (`PeerReplication::send_heartbeat`) |
| Quorum tracking                | Cluster (`Quorum`) |
| Historical WAL backfill        | Cluster (`FetchSegment`, planned) |
| Follower catch-up              | Cluster |
| Node membership (static)       | Cluster (`ClusterConfig::peers`) |

---

## Task Topology

The Cluster layer is organised around role-specific owners; no separate
supervisor layer exists.

- **`Leader`** — constructs `Arc<Quorum>` and the cooperative
  `Arc<AtomicBool>` shutdown flag, then spawns the client-facing
  gRPC server task, the Node gRPC server task, and one
  `PeerReplication` task per configured peer. Each peer task is its
  own `tokio::spawn`. `LeaderHandles` carries every
  `JoinHandle` plus the shared `Quorum` and `running` flag.
- **`Follower`** — spawns the read-only client-facing gRPC server
  task and the Node gRPC server task. No peer tasks.
- **`PeerReplication`** — one per configured peer. Owns its own
  `WalTailer` cursor, gRPC client, `from_tx_id`, `peer_last_tx`,
  `peer_index`, and a shared `Arc<Quorum>` clone. Retries transport
  and logical rejects in place; reconnects on transport failure;
  emits idle heartbeats on empty tails.

A previous draft used a dedicated `PeerManager` supervisor that
wrapped the per-peer tasks in a single `tokio::spawn`. It was
eliminated in favour of the leader owning peer tasks directly —
`Leader` already holds the state (`Quorum`, `running`, peer list) the
supervisor was forwarding, and the extra indirection added no
observable behaviour.

---

## Node gRPC Service

The Cluster listens on a second gRPC port (separate from the client-facing
Ledger service) and speaks the `Node` service (`proto/node.proto`).

Implemented:

- `AppendEntries(AppendEntriesRequest) -> AppendEntriesResponse` — ships
  raw WAL bytes + metadata from leader to follower. Request permits
  empty `wal_bytes` as an idle heartbeat. Response carries `term`,
  `success`, `last_tx_id` (follower's **live** `last_commit_id`, read
  from the ledger on every RPC path), `reject_reason`.
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
replication_poll_ms      = 5    # idle-poll / heartbeat cadence when tailer returns 0
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

**Dedicated `PeerManager` supervisor** — an earlier draft wrapped the
per-peer tasks in a single `tokio::spawn`ed supervisor. Dropped in
favour of `Leader` spawning peers directly; the supervisor was a
pass-through that duplicated state already owned by `Leader`.

---

## Out of Scope

- Leader election and term management → ADR-016
- Term-fenced writes (proto fields scaffolded, not enforced) → ADR-016
- Dynamic membership changes
- Read-from-follower semantics (the follower's read RPCs are exposed
  today purely as a side-effect of running the Ledger gRPC service in
  read-only mode; formal semantics → ADR-016)
- Quorum-gated *leader commit*. Clients can already wait on
  cluster-commit via the submit wait level, but the leader's own
  advertised commit watermark still tracks its local WAL, not quorum.
  Making the leader's watermark itself quorum-gated is deferred.
- `FetchSegment` RPC and historical backfill (designed, not implemented)

---

## References

- ADR-006 — WAL, snapshot, seal durability
- ADR-010 — Wait levels (`WaitLevel::Committed`)
- ADR-011 — WAL write/commit separation
- Ongaro & Ousterhout 2014 — Raft
- Liskov & Cowling 2012 — Viewstamped Replication Revisited
- etcd — lagged-commit replication model
