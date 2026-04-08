# ADR-005: PressureManager — Adaptive Pipeline Execution Mode

**Status:** Postponed (due to existing problems)  
**Date:** 2026-03-29  
**Author:** Taleh Ibrahimli  

---

## Context

Every pipeline stage — Transactor, WAL Storer, Snapshotter — runs in a dedicated thread and
polls its inbound `ArrayQueue` in a tight loop. When the queue is empty the thread calls
`yield_now()` and immediately polls again. This is a busy-wait loop.

The result: all pipeline threads consume 100% CPU at all times, regardless of whether there
is any work to do. On a machine running other workloads, or during idle periods between
transaction bursts, this wastes CPU resources with no benefit.

The problem is structural — there is no signal between stages about whether work exists.
Each stage makes its own decision independently, and the only decision available is "yield
and try again immediately."

---

## Decision

Introduce a shared `PressureManager` that observes work and empty signals from all pipeline
stages and owns the decision of how each thread executes: **spin**, **yield**, or **park**.

### Responsibility split

Each stage reports to `PressureManager` on every loop iteration. The stage has no awareness
of its execution mode — it only reports what it found (work or empty). `PressureManager`
owns the state machine and decides what the calling thread does next.

This keeps stage code simple and ensures execution mode decisions are made in one place.

### Two pressure signals, opposite directions

The pipeline has two observable pressure signals:

```
BackPressure:  Sequencer → Transactor → WAL → Snapshotter
               Sequencer spinning to push → downstream is slow, pipeline is full

Pressure drop: Snapshotter → WAL → Transactor → Sequencer
               Transactor inbound empty → upstream has no work, pipeline is dry
```

Both signals are captured implicitly by `on_work()` and `on_empty()` calls from each stage.
No explicit directional signal is needed — the global work/empty streak captures both.

### Global pressure level

Pressure is a single global state shared across all pipeline stages. All stages read and
respond to the same level. This is correct because:

Every transaction passes through every stage exactly once. When there is work, all stages
have work. When there is no work, no stage has work. Pressure is a global property of the
pipeline, not a per-stage property. Per-stage pressure levels would desynchronise transitions
and cause some stages to park while others spin with nothing to process.

### Three execution states

```
Spin   — queue has work, process at full speed, no OS involvement
Yield  — queue borderline, give up timeslice but stay hot
Park   — queue dry, thread sleeps until unparked
```

State transitions:

```
              work crosses threshold
  ┌──────────────────────────────────────────────────────┐
  │                                                      ▼
Idle                Low  ◄──────────────────────────── High
  ▲                  │      empty crosses HIGH_TO_LOW    │
  │                  │                                   │
  └──────────────────┘                                   │
   empty crosses LOW_TO_IDLE              (stays High while work present)
```

New work always transitions directly to `High` regardless of current level — no ramp-up.
The Sequencer calls `on_work()` before pushing the first transaction into the queue, so
threads are unparked and spinning before the Transactor sees the first item. Zero wakeup
lag on the hot path.

### Sequencer is the entry point

The Sequencer calls `on_work()` on every `submit()` before pushing to the queue. This
ensures the pipeline is awake before work arrives, not after. The Sequencer is the natural
owner of this signal — it is the only stage that receives external input.

### WaitStrategy — user-facing configuration

Threshold values are internal to `PressureManager`. Users do not configure raw numbers.
Instead they choose a `WaitStrategy` that expresses intent:

```
LowLatency   Threads spin continuously. Minimum latency, maximum CPU.
             Suitable for dedicated hardware where roda-ledger owns all cores.

Balanced     Adaptive spin → yield → park. Balances latency and CPU.
             Default. Suitable for most deployments.

LowCpu       Parks quickly when idle. Minimum CPU, slightly higher wakeup latency
             on burst start. Suitable for shared hardware or background ledgers.
```

The user answers "what matters more — latency or CPU?" The thresholds behind each mode are
owned by `PressureManager` and are not exposed.

Approximate threshold behaviour per mode:

```
Mode          Spin duration    Yield duration    Park
────────────────────────────────────────────────────────────────
LowLatency    indefinite       never             never
Balanced      short            moderate          after sustained idle
LowCpu        minimal          short             quickly on idle
```

`LowLatency` preserves current behaviour exactly — no regression for users who want maximum
throughput on dedicated hardware.

`WaitStrategy` is set in `LedgerConfig`. Default is `Balanced`.

### Thread registration

Each stage registers its thread handle with `PressureManager` at startup. `PressureManager`
holds these handles to issue `unpark()` calls when transitioning out of `Idle`. All stage
threads must be registered before the first transaction is submitted.

### Asymmetric transition speed

Transitions to higher pressure (more work) are fast. Transitions to lower pressure (less
work) are slow. This prevents thrashing on bursty workloads where a short idle gap between
bursts would otherwise cause unnecessary park/unpark cycles.

The specific thresholds per mode are determined during implementation and validated against
stress test scenarios before being finalised.

---

## Behaviour at low throughput (e.g. 10 req/s)

At 10 req/s the inter-arrival gap is ~100ms. The pipeline will naturally cycle:

```
Request arrives → Sequencer on_work() → unpark all → High
Process transaction → queue empty → empty streak climbs → Low → Idle → park
~100ms later → next request → repeat
```

Park/unpark wakeup latency is 1–10µs on Linux. At 100ms inter-arrival gaps this is
negligible. `LowLatency` mode avoids park entirely for deployments where even 10µs
wakeup latency is unacceptable. `LowCpu` mode parks aggressively, spending the 99.99%
idle time at ~0% CPU. `Balanced` falls between the two.

---

## Consequences

### Positive

- Idle CPU drops from ~100% to ~0% in `Balanced` and `LowCpu` modes
- Wakeup latency under sustained load is zero — threads never park while `High`
- Single shared struct, single atomic level — minimal coordination overhead
- Each stage is simplified — no spin/yield/park logic per stage
- User-facing configuration is three named modes, no internal knowledge required
- `LowLatency` preserves current behaviour — no regression for existing deployments
- Sequencer-driven wakeup — new work wakes all threads before entering the queue

### Negative

- Thread registration at startup adds a small initialisation dependency — all stage threads
  must be spawned and registered before the first `submit()` call
- Shared atomics written by all stages on every loop iteration may cause false sharing at
  very high throughput — to be verified with benchmarks, mitigated with cache line padding
  if needed
- `thread::park()` wakeup latency is OS-dependent (~1–10µs on Linux) — only relevant at
  `Idle → High` transitions, which only occur after genuine idle periods
- Threshold values behind each `WaitStrategy` require tuning and stress test validation
  before being finalised

### Neutral

- Under sustained load, all modes except `LowLatency` behave identically to `LowLatency`
  — threads stay in `High` and spin continuously
- Existing benchmarks (`wallet_bench`, `grpc_bench`) are unaffected — they run under
  sustained load where `PressureManager` stays at `High`

---

## Alternatives Considered

**Per-stage adaptive spinning**  
Each stage manages its own spin/yield/park threshold independently. Rejected — pressure is
a global pipeline property. Per-stage thresholds desynchronise transitions and produce
stages spinning with nothing to process while other stages are parked.

**Blocking crossbeam channels**  
Replace `ArrayQueue` with blocking `crossbeam::channel`. Each stage blocks on `recv()`.
Rejected — changes the queue type across all stages, loses the bounded backpressure
guarantee of `ArrayQueue`, and moves blocking decisions into the channel rather than a
single owned place.

**`thread::sleep` with fixed interval**  
Sleep a fixed duration (e.g. 1ms) when the queue is empty. Rejected — adds fixed minimum
wakeup latency on every idle period. Unacceptable for a latency-sensitive financial ledger.

**Condvar**  
`Mutex` + `Condvar` for blocking. Rejected — requires acquiring a mutex on every wakeup.
`park`/`unpark` is the correct primitive when a known set of threads is the target and no
shared state needs to be guarded during the wakeup.

**Exposing raw thresholds in config**  
Allow users to configure `high_to_low`, `low_to_idle` directly. Rejected — these values
are meaningless without understanding the pipeline internals. `WaitStrategy` expresses the
same intent in terms users can reason about.

---

## References

- ADR-001 — entries-based execution model, pipeline stage ownership
- ADR-002 — Vec-based balance storage, single-writer Transactor
- `src/transactor.rs` — current busy-wait loop
- `src/wal.rs` — current busy-wait loop
- `src/snapshot.rs` — current busy-wait loop
- `src/sequencer.rs` — entry point, owns submit path
- Linux NAPI — same pattern: spin under load, park when idle, wake on interrupt
- `std::thread::park` / `std::thread::unpark` — chosen primitive for blocking