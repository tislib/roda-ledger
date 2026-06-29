# ADR-027: Reactive Index Propagation — One Ledger Index Hook Drives Replication and Waiters

**Status:** Proposed
**Date:** 2026-06-29
**Author:** Taleh Ibrahimli

**Amends:**
- ADR-015 (Cluster Mode) — re-reverses §Decision item 4 ("the leader's commit progress
  feeds quorum **by polling, not by callback**") back to a callback/reactive mechanism, and
  generalizes the parked §"2026-04-22 Decisions" item 4 ("Ledger exposes a commit-advance
  hook") from a commit-only callback to **all observable progress indexes**.

---

## Context

The cluster layer observes the ledger's progress indexes by **polling**, and every waiter
in the system **spins or sleeps**. Both are cyclic, and the cycle period — not the work —
sets the latency floor.

**Replication is cyclic.** The per-peer leader sender (`replication_driver.rs:566` —
`run_peer_sender`) tails the durable WAL and, when the tailer returns no new bytes, parks
on `sleep(idle_sleep)` where `idle_sleep = replication_poll_ms` (`replication_driver.rs:580,
:623`), default **5 ms** (`config.rs:161`). On every loop tick it also calls
`self_advance()` (`replication_driver.rs:712`), which reads `ledger.last_snapshot_id()`
(`consensus/state.rs:253`) to feed the leader's own slot into `Quorum::advance`
(`raft/src/quorum.rs`). Followers ack by re-reading `last_snapshot_id()` after each apply
(`replication_driver.rs:265, :297`). So a transaction that commits 1 µs after a sender's
idle tick waits up to ~5 ms before it is replicated, and quorum advance is gated on the
same poll. Under steady load the tailer rarely idles, but at low/bursty rates the 5 ms tail
dominates cluster-commit latency.

**Waiters spin.** The client `*_and_wait` RPC path
(`cluster/src/waiter/wait_for_transaction_level.rs:85`) polls `last_commit_id()` /
`last_snapshot_id()` every `100 µs` until the level is reached. The ledger's own
`wait_for_transaction_*` (`ledger.rs:488, :626`) backs off through `WaitStrategy::retry`,
whose slow tier is `thread::sleep(1ms)` (`wait_strategy.rs:60`) — a hard 1 ms floor below
which a blocked caller cannot observe progress, even when the index moved nanoseconds later.

This was a deliberate prior choice: ADR-015 §"2026-04-22" item 4 added a commit-advance
callback, then ADR-015 §Decision item 4 **superseded it with polling** ("same effect,
different mechanism"). The callback type still exists as dead code — `CommitHandler`
(`pipeline.rs:23`), re-exported but never registered or fired. The motivation to avoid a
callback was the fear that firing a consumer inline on a hot pipeline thread would stall the
ledger.

That fear is now measured, not assumed. We added a single index hook fired inline on the
publishing stage thread and load-tested it on the 12-core Debian dev box (`bench()` config,
5 runs each):

- A naïve variant — firing on **every** index including the per-submit sequencer increment,
  and building a 4-field snapshot per fire — cost **+76 ns/submit** (submit P50 46 ns → 122 ns,
  ~2.6×). Throughput was unaffected (≈4.5M TPS) because the producer is not the bottleneck.
- The refined variant — `Fn(PipelineIndexKind, u64)`, fired only on **compute/commit/snapshot**
  (never sequencer), passing the value already in hand — was **indistinguishable from
  baseline**: submit P50 ≈54 ns vs ≈46 ns (within run-to-run noise), throughput ≈4.5M TPS,
  and end-to-end probe latency unchanged.

Conclusion: an inline hook is free **iff** it (a) skips the per-submit sequencer path and
(b) does only non-blocking work. That gives us the budget to make index propagation
reactive everywhere.

---

## Decision

### D1 — The ledger exposes one non-blocking index hook (the mechanism)

The ledger gains a single registrable hook, replacing the dead `CommitHandler`:

```rust
pub enum PipelineIndexKind { Compute, Commit, Snapshot }
pub type IndexHook = Arc<dyn Fn(PipelineIndexKind, u64) + Send + Sync + 'static>;
fn set_index_hook(&self, hook: IndexHook);   // on Pipeline, surfaced on Ledger
```

Invariants this establishes:

1. **One hook, registered once, before `start()`.** Stored in a `OnceLock`; first
   registration wins. Read lock-free on the hot path — a single `Acquire` load + branch when
   none is registered (the standalone-ledger default pays nothing).
2. **Fired inline on the publishing stage thread**, immediately after the index store, for
   `Compute` (transactor), `Commit` (WAL committer), and `Snapshot` (snapshot stage). The
   **sequencer index is never hooked** — waiters never block on it, so firing per-submit
   would be pure hot-path cost (the +76 ns above).
3. **The hook must be non-blocking.** It runs *on* the stage thread, so it may not block,
   `.await`, lock a contended mutex, allocate, or do I/O. Permitted work is the
   atomic-store class: mirror the value into an atomic and poke a notifier. A blocking hook
   stalls the pipeline stage that fired it — this invariant is the whole reason the design
   is safe. The hook is observability, not a place to do work.

This is the narrow extension point ADR-015 originally intended, generalized from commit-only
to all three observable indexes and proven free.

### D2 — The cluster registers the hook and drives a notifier (reactive replication, goal 1)

`ClusterNode` registers exactly one hook (wiring point: `node.rs`, after the `Ledger` is
built and before `Consensus` is constructed). The hook does only two non-blocking things per
fire: store the value into a per-kind atomic mirror, and wake a **notifier** the consensus
tasks own. The notifier is the sync→async bridge — the ledger fires from a std thread; the
notifier wake (`tokio::sync::Notify::notify_waiters`, or a `watch`) is sync-callable and
non-blocking, with no `.await` and no lock on the fast path.

The per-peer sender (`run_peer_sender`) stops polling: instead of an idle-poll sleep on an
idle tailer, it `await`s the `Commit` watch and re-tails the instant it advances. The
leader's self-quorum feed (`self_advance` → `Quorum::advance`) is driven by `Snapshot`
advances rather than re-read every tick. The old 5 ms `replication_poll_ms` idle tail is
**removed entirely**; replication latency collapses to wake + send. The only remaining
periodic wake is a true **Raft heartbeat**, fired every `RaftConfig.heartbeat_interval`
(50 ms by default — derived from and validated against the election timeout,
`heartbeat_interval * 2 < election_timer.min_ms`; **not** separately configurable). A data
send re-arms the heartbeat timer, so heartbeats only flow when genuinely idle — matching the
house rule "event-driven freshness over periodic tickers."

### D3 — Reactive waits replace polling (goal 2)

The same notifier replaces the client waiter's `100 µs` poll
(`waiter/wait_for_transaction_level.rs`): the `*_and_wait` future parks on the notifier and
re-checks the atomic mirror on each wake, with the existing 2 s timeout as a deadline, not a
poll period. One notifier is the single source of all index movement, so it serves both
replication (D2) and client waits from the same signal.

**Lost-wakeup discipline (required):** a waiter registers its interest *before* the final
predicate check — `notify_waiters()` keeps no permit, so a concurrent advance must not slip
between the check and the await (`tokio::Notify::notified` + `enable()`). It then re-reads
the atomic mirror.

**No close path — teardown is owned by the node, not the notifier.** A reactive waiter does
*not* need the notifier to wake it on shutdown. Every `wait_reach` is awaited under the node
`CancellationToken` (`ClusterNode` owns it and cancels it in its `Drop`, `node.rs`) — via a
`select!` on `cancelled()`, or implicitly under tonic's `serve_with_shutdown`. On shutdown
the owning task's future is dropped from the outside, which unregisters the parked waiter.
So the notifier carries no `close()`/shutdown flag. This is deliberate: the things that
would need waking are exactly the `Arc<Waiter>` holders, so a `Drop for Waiter` could never
fire while any waiter is parked (refcount paradox) — shutdown signalling must come from a
singularly-owned token, never the shared notifier. The single invariant for every call site:
**never bare-await `wait_reach` — always under the node cancellation (or tonic graceful
shutdown).**

**Out of scope (noted for a future ADR):** the ledger-internal `wait_for_transaction_*`
1 ms-sleep floor. The hook can't drive a *cluster* notifier from inside the standalone
ledger, and the ledger crate is non-tokio. Making those waits reactive needs a ledger-side
sync notifier (futex / `event-listener`) wired at the same publish sites — a separate change
that preserves the ledger's no-tokio constraint. This ADR deliberately keeps "one hook,
cluster-owned notifier" and does not expand the ledger's runtime surface.

---

## Consequences

### Positive
- **Replication latency loses the 5 ms tail.** At low/bursty rates a commit is replicated on
  the next wake (microseconds) instead of up to the old 5 ms idle poll. Quorum advance is
  reactive, not poll-gated.
- **Far fewer tokio cycles.** Idle senders and blocked client waits stop busy-polling
  (5 ms sender ticks, 100 µs waiter spins). Threads sleep until there is real work, reducing
  CPU and scheduler pressure — the stated goal.
- **The hook is measurably free** (D1 experiment): no submit-latency or throughput
  regression, so reactivity costs nothing on the write path.
- **Resurrects a narrow, intended extension point.** `CommitHandler` dead code is removed;
  the ledger surface gains observability without new responsibilities.

### Measured (D3 client waiter)

A/B on `cluster_load_latency` (single-node, 12-core Debian, `bench()` config, 3 runs each),
isolating *only* the waiter — HEAD's 100 µs poll vs the reactive `IndexWatch` — with the
index hook firing in both arms so its cost cancels out:

| Wait level | A — 100 µs poll (p50 / p99) | B — reactive (p50 / p99) |
|---|---|---|
| **Compute** (in-memory) | **1.1 ms / 2.1 ms** | **~7–9 µs / ~10 µs** |
| **Commit** (fsync) | ~6.4 ms / ~10 ms | ~2.7–6.2 ms / ~9 ms |

Compute is **~150× faster** reactive — and A's p50 is *flat at 1.1 ms across every load level*,
which is the tell: it measures the runtime timer tick, not the work. `tokio::time::sleep(100 µs)`
rounds **up** to the ~1 ms timer-wheel granularity, so the poll loop paid a full millisecond per
iteration while the transaction was ready in microseconds. Commit is fsync-bound (milliseconds),
so the ~1 ms poll tick is lost in disk variance and A ≈ B there. Net: reactivity is a large win
when the wait target is fast, neutral when it is inherently slow, and never a regression.

### Negative
- **A blocking or slow hook stalls the pipeline.** The non-blocking invariant (D1.3) is a
  load-bearing contract enforced only by review and documentation, not the type system. A
  future careless consumer that locks or allocates in the hook would reintroduce the exact
  stall ADR-015 feared. The mitigation is the atomic-store-then-poke discipline.
- **Single-hook limit.** Only one consumer can register. If a second subsystem ever needs
  index events, it must multiplex behind the cluster's hook (or a later ADR widens the
  mechanism). Chosen deliberately for simplicity over a subscriber list on the hot path.
- **Lost-wakeup surface.** Reactive waiting is correct only with the register-before-check
  discipline and cancellation-owned teardown (D3); a polling loop is harder to get subtly
  wrong. This shifts a class of bugs from "slow" to "hung," so the discipline is mandatory
  and tested.

### Neutral
- `replication_poll_ms` is **removed** from the config (cluster `ClusterSection`, the control
  `ClusterConfig` proto field — reserved #5 — and the UI). The replication cadence is no
  longer a knob: the only periodic wake is the Raft heartbeat, sourced from
  `RaftConfig.heartbeat_interval`.
- The ledger-internal `wait_for_*` waiters keep their `WaitStrategy` spin/sleep until the
  follow-up ADR; standalone-ledger behavior is unchanged by this ADR.
- The notifier lives in the cluster (tokio); the ledger stays non-tokio. The hook is the
  only coupling, and it is a plain `Fn` — the `raft` crate's purity (no tokio/ledger deps)
  is untouched.

---

## References
- ADR-015 — Cluster Mode (amended: §Decision-4 polling → reactive; §2026-04-22-4 hook generalized)
- ADR-016 — Leader Election (the `Quorum` / `seal_watermark` path this feeds)
- ADR-017 — roda-raft (`cluster_commit_index` / `match_index`, the two commit indexes)
- ADR-011 — WAL Write/Commit Separation (the `commit_index` this reacts to)
- ADR-006 — WAL, Snapshot, Seal Durability (the `snapshot_index` this reacts to)
- ADR-019 / ADR-021 — Transaction Ring / WAL sole releaser (the pipeline stages that fire the hook)
