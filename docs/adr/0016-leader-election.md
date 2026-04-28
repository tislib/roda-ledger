# ADR-0016: Leader Election and Log Divergence

**Status:** Proposed  
**Date:** 2026-04-24  
**Author:** Taleh Ibrahimli

---

## Context

ADR-015 introduced the Cluster layer with static leader/follower roles,
lagged single-phase replication, and a `Quorum` tracker. Leader
election, term-fenced writes, and handling of log divergence between a
deposed leader and a new leader were explicitly deferred to this ADR.

This ADR covers:

- Replacing the hard-coded `ClusterMode::{Leader, Follower}` with a
  runtime role state machine driven by Raft-style elections.
- Adding the persistent `voted_for` state required by Raft §5.4.1.
- Implementing `RequestVote` and enforcing `term` / `prev_tx_id` /
  `prev_term` on `AppendEntries`.
- Defining Roda's mechanism for resolving log divergence without
  violating the "live WAL is append-only" invariant.
- Making the cluster configuration symmetric across nodes so the same
  config file can be deployed everywhere.

Initialization/recovery-mode runtime shape (how the node behaves between
role transitions and during boot reseed) and the full `InstallSnapshot`
/ historical-backfill mechanism are called out here but fully specified
in a follow-up document alongside the implementation.

## Decisions

### 1. Configuration: identity lives under `[cluster.node]`, membership is symmetric

The `Mode` field is removed from the cluster config. Role is a runtime
property, not a deployment property. Cluster-related sections are
grouped under a single `[cluster]` namespace so the client-facing
`[server]` section owns only the external gRPC surface; identity and
peer-facing transport live with the cluster.

```toml
[server]                           # client-facing Ledger gRPC
host    = "0.0.0.0"
port    = 50051

[cluster.node]                     # this node's identity + peer-facing gRPC
node_id = 1
host    = "0.0.0.0"
port    = 50061

[[cluster.peers]]                  # full membership (including self)
peer_id = 1
host    = "http://127.0.0.1:50061"

[[cluster.peers]]
peer_id = 2
host    = "http://127.0.0.1:50062"

[[cluster.peers]]
peer_id = 3
host    = "http://127.0.0.1:50063"

[ledger]
# ... standard LedgerConfig ...
```

The `cluster.peers` list **includes every node in the cluster,
including this one**. Each node's config file is therefore identical
except for `cluster.node.*`. At startup the node filters `cluster.peers`
by `peer_id != cluster.node.node_id` to derive its "others" set. If
`cluster.node.node_id` does not appear in `cluster.peers`, startup
fails — the node is not part of its own cluster view.

Rationale: identical peer lists eliminate a class of deployment bugs
where nodes disagree about membership. Grouping identity with the
peer-facing transport under `[cluster]` keeps `[server]` focused on
the external API surface.

### 2. Runtime role state machine

Replaces the one-shot `match config.mode` in `node.rs` with a
long-running loop that transitions between roles and tears down /
rebuilds the per-role state on each transition.

```
┌──────────────┐  election timeout   ┌───────────┐   majority votes   ┌────────┐
│ Initializing │ ──────────────────▶ │ Candidate │ ─────────────────▶ │ Leader │
│ / Follower   │                     │           │                    │        │
└──────────────┘ ◀───── step-down ── └───────────┘ ◀───── higher term ┘────────┘
       ▲                                                                   │
       └───────────────── higher term / lost leadership ───────────────────┘
```

Transitions always go through a full teardown: the outgoing role's
`Handles` struct is dropped, which joins or aborts every owned task
(peer replication, gRPC servers, election timer), before the new
role's bring-up runs. No mutable state is carried across role
boundaries except the `Arc<Ledger>`, the `Arc<Term>`, and the
`Arc<Vote>` (see §4 and §5).

**Ledger teardown on history truncation.** The `Arc<Ledger>` is
normally carried across role transitions. There is exactly one
exception: when an incoming `AppendEntries` reveals log divergence
(§9), the Ledger must also be dropped and re-created via
`start_with_recovery_until(watermark)` before the new role is
brought up. This teardown **only happens during the runtime role
state machine** — never at cold boot (cold boot always calls plain
`start()`). In other words, history truncation is a runtime-only
operation triggered by divergence detection, not a boot-time option.
The transition in this case is:

```
Follower (detects divergence)
   │  drop role Handles
   ▼
Initializing (tasks down, Ledger still alive)
   │  drop Arc<Ledger>
   │  Ledger::start_with_recovery_until(watermark)
   ▼
Initializing (fresh Ledger, history truncated to watermark)
   │
   ▼  next AppendEntries resumes normally
Follower
```

`Initializing` is the state on cold boot and after any teardown. The
Node gRPC server is already running so the node can participate in
elections and receive `AppendEntries`, but neither the writable
client-facing handler nor any peer-replication tasks are up yet. The
exact shape of this state (what handlers do while in it, how it
differs from `Follower`) is specified in the implementation follow-up.

### 3. Drop-and-rebuild on role transition

`LeaderHandles` and `FollowerHandles` own **all** role-specific
long-lived resources — peer tasks, the writable/read-only client
handler, the Node gRPC server binding, the `Arc<Quorum>` (leader
only), and the cooperative `running: Arc<AtomicBool>` shutdown flag.
Dropping a handle is sufficient to fully tear the role down. A second
leader bring-up after step-down therefore cannot observe stale peer
connections, stale quorum slots, or a still-writable client handler.

### 4. Persistent `voted_for` via `vote.log`

Raft requires `(current_term, voted_for)` to be durable before a vote
response is returned. This state does not belong in `term.log`:
`term.log` is on the `GetStatus` hot path and grows by one record per
term transition, whereas vote state is cold and grows by one record
per vote grant.

- `storage::VoteStorage` owns the sibling `vote.log` file in the data
  dir: `open` / `append(VoteRecord)` / `scan` / fdatasync semantics.
- `cluster::Vote` owns the in-memory view (`current_term`,
  `voted_for`) and the API: `vote(term, candidate_id) -> bool`,
  `observe_term(term)`.

`VoteRecord { term, voted_for }` — one record per grant. On reopen,
the last record supplies the in-memory view; absence of the file
means "never voted."

### 5. Election

Standard Raft election with pre-vote:

- Randomised election timeout (default 150–300 ms, configurable).
  Reset on any valid `AppendEntries` (including heartbeats) or on
  granting a vote.
- On timeout: transition to Candidate, bump term (durably via
  `Term::new_term` — see §10 below), vote for self (durably via
  `Vote::vote`), fan out `RequestVote` to every peer.
- **Pre-vote round** (etcd-style) precedes the real vote: a
  disconnected node must win a pre-vote before bumping its term, to
  avoid forcing the rest of the cluster to step down on every
  re-connection.
- Majority is computed over `peers.len() + 1` (the candidate
  counts itself). Reuses the same `(n / 2) + 1` arithmetic `Quorum`
  already uses; the election's vote count is not `Quorum` itself but
  uses the same sizing.
- On majority: transition to Leader, emit an initial no-op
  `AppendEntries` to establish authority and let term advancement
  propagate.
- On majority-denied, higher term observed, or deadline: transition
  back to Initializing with a new randomised deadline.

### 6. `RequestVote` handler

Implements Raft §5.4.1 verbatim. Grants the vote iff **all** of:

1. `request.term >= current_term`.
2. `voted_for` is `None` or equals `request.candidate_id` (within
   this term).
3. Candidate's log is at least as up-to-date as ours:
   `(request.last_term, request.last_tx_id) >= (our_last_term,
   our_last_tx_id)` using lexicographic comparison.

`our_last_term` comes from `Term::last_record()`, `our_last_tx_id`
from `ledger.last_commit_id()`.

A granted vote fsyncs `vote.log` **before** the response is returned.
A term observed above `current_term` in any incoming RPC (request or
response, `RequestVote` or `AppendEntries`) causes a step-down: current
role drops, term is durably updated via `Term::observe`, `voted_for`
is cleared, and the node returns to Initializing.

### 7. Serialised Node-service handlers

All `Node` service handlers (`AppendEntries`, `RequestVote`,
`InstallSnapshot`) share a single per-node `tokio::sync::Mutex`
acquired at the top of each handler. Raft safety requires these to
appear atomic with respect to `(current_term, voted_for, log)`; tonic
dispatches RPCs in parallel by default, so this lock is mandatory.

`Ping` is read-only and does not take the lock.

### 8. Term-fenced `AppendEntries`

The follower handler begins enforcing the `term`, `prev_tx_id`, and
`prev_term` fields that were scaffolded in ADR-015:

| Condition | Action |
|---|---|
| `request.term < current_term` | Reject with `REJECT_TERM_STALE`, return our `current_term`. |
| `request.term > current_term` | Observe term (durable), clear `voted_for`, continue. |
| `prev_tx_id > last_local_tx_id` | Reject with `REJECT_PREV_MISMATCH` — we have a gap. |
| `prev_tx_id` in our log, `prev_term` mismatches our term at `prev_tx_id` | **Divergence** (see §9). |
| all matched | Queue bytes, return `last_commit_id`. |

### 9. Log divergence handled via recovery-mode reseed

This ADR **amends ADR-015's "Truncation / log rollback on conflict —
Ruled out"** alternative. Truncation is allowed, but only through a
Ledger restart — the live WAL remains append-only *while running*.

When divergence is detected on `AppendEntries`:

1. Follower returns `REJECT_PREV_MISMATCH`.
2. Role state machine (§2) tears down the current `FollowerHandles`,
   transitions to Initializing, and **drops the `Arc<Ledger>`** —
   all tasks owning the Ledger are gone by this point, so the Arc's
   strong count reaches zero and the Ledger is destructed.
3. Role state machine re-creates the Ledger via a new API,
   `Ledger::start_with_recovery_until(watermark)`, passing
   `leader_commit_tx_id` from the rejecting request as `watermark`.
4. The existing Recover path replays WAL + snapshot **up to
   `watermark`**, rebuilds balances from scratch to that point, and
   physically truncates WAL content above `watermark`.
5. A fresh `FollowerHandles` is brought up over the new Ledger; the
   next `AppendEntries` either resumes cleanly (`prev_tx_id >=
   watermark`) or starts a normal catch-up backfill.

This is the **only** path in which a Ledger is dropped and
re-created at runtime. Cold boot always uses plain `Ledger::start()`
— there is no configuration or command-line flag for "boot with
truncation." `start_with_recovery_until` is strictly a runtime
response to divergence, invoked by the role state machine.

Because balances are derived state of the WAL, rewinding the WAL
rewinds balances as a side-effect of Recover — no undo log, no staged
balance layer, no in-place rollback of applied entries. The
append-only invariant holds for every *running* Ledger; truncation is
a boot-time operation only.

A deposed leader that becomes a follower uses the **same reseed
path** if its uncommitted tail diverges from the new leader's log.
Any snapshot whose covered-up-to-tx is greater than `watermark` is
skipped during `start_with_recovery_until`; recovery falls back to an
earlier snapshot or genesis + WAL replay.

If no local snapshot covers a sufficiently early point (unusual but
possible under aggressive retention), the follower cannot complete
recovery locally and must request `InstallSnapshot` from the leader.
This crossover into snapshot-based catch-up is deferred to the
recovery-phase follow-up.

Follower's own `last_commit_id` remains what it was under ADR-015:
its WAL-fsync'd state, reported to the leader on every
`AppendEntries` response. The follower does **not** track a
cluster-commit watermark durably — `leader_commit_tx_id` is consumed
only as the reseed target above.

### 10. New Ledger surface: `start_with_recovery_until`

```rust
/// Alternative entry point to `start()` used by followers that must
/// rebuild state at or below a given watermark after detecting log
/// divergence. Replays WAL + snapshot up to and including `watermark`
/// and physically truncates any WAL content above it. Snapshots whose
/// covered-up-to-tx exceeds `watermark` are skipped in favour of an
/// earlier snapshot (or genesis).
pub fn start_with_recovery_until(watermark: u64) -> Result<Ledger>;
```

This is the only new item on the minimal Ledger surface defined by
ADR-015. It does not change the normal `start()` path.

### 11. Term bump semantics for Candidate

`Term::new_term` currently takes `start_tx_id` meaning "first tx of
this term." A candidate bumping its term has no appended tx yet. Two
options:

- Allow `start_tx_id` to equal the candidate's `last_commit_id + 1`
  at the moment of the bump — this is the tx the candidate *would*
  write first if it wins. If it loses, the record is still correct
  (no tx was ever written under that term).
- Defer writing the term record until the winning candidate emits
  its first `AppendEntries`, keeping `Term::new_term` semantics
  unchanged.

Chosen: the former. Simpler, and `Term` already records one record
per term regardless of activity.

### 12. Leader-hint redirection

`AppendEntriesResponse` and `PingResponse` gain (or reuse) a
`leader_hint: u64` field carrying the node's best-known leader id.
Clients hitting a non-leader's client-facing API receive
`FAILED_PRECONDITION` with the leader hint in response metadata
instead of a bare error, and can redirect without a separate lookup
RPC.

### 13. Pre-vote

As noted in §5, pre-vote is enabled by default to avoid unnecessary
term inflation when a partitioned node reconnects. A candidate first
issues a `RequestVote` with a "pre-vote" flag (or a separate
`RequestPreVote` RPC — to be decided at proto time); peers grant the
pre-vote by the same up-to-date-log check but **do not** persist
`voted_for` and **do not** bump their term. Only if pre-vote wins
does the candidate do the real term bump and real vote fan-out.

## Amendments to ADR-015

- **§Alternatives Rejected — Truncation:** "Ruled out" is amended to
  "allowed via boot-time recovery only, see ADR-016 §9."
- **§Out of Scope — Leader election and term management:** now
  in-scope via this ADR.
- **§Out of Scope — Term-fenced writes:** now in-scope via §8.

## Out of Scope (this ADR)

- Detailed specification of Initializing mode behaviour (what each
  handler returns, how it differs from Follower) — deferred to
  implementation follow-up.
- `InstallSnapshot` full implementation — deferred to the recovery
  phase.
- `FetchSegment` historical WAL backfill — unchanged from ADR-015.
- CheckQuorum leader-step-down-on-majority-loss — follow-up; easy to
  add on top of this ADR.
- Dynamic membership changes (joint consensus / single-server
  additions) — still out of scope.
- Read-from-follower formal semantics — still out of scope.

## References

- ADR-015 — Cluster Mode
- ADR-006 — WAL, snapshot, seal durability
- ADR-011 — WAL write/commit separation
- Ongaro & Ousterhout 2014 — *In Search of an Understandable
  Consensus Algorithm* (Raft)
- Ongaro 2014 PhD thesis — pre-vote, CheckQuorum
- etcd — pre-vote default, lagged-commit replication model
