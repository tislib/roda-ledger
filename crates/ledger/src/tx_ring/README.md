# `tx_ring` — single-writer, multi-reader lock-free ring buffer

`TxRing` is the inter-stage transport for the roda-ledger pipeline. One writer fills a
fixed, power-of-two array of `WalEntry` slots; many readers consume the same slots **in
place** at their own pace; and a single *release mover* advances a consume index that
bounds the writer so it can never overwrite data that hasn't been released yet.

It behaves like an **SPSC** queue for flow control — one producer, one consume-index mover
— but allows an arbitrary number of **passive readers** between them.

```
            release_index            write_index           release_index + capacity
                 │                         │                          │
   ── overwritable ──┤■■■■■■■■■■■■■■■■■■■■■■│── writer may fill ──────┤
                     └ readable window ────┘
                     [release_index, write_index)   ← every reader's cursor lives here
```

Invariant chain (always true): `release_index ≤ write_index ≤ release_index + capacity`.

The element type is fixed to [`WalEntry`](../../../storage/src/entities.rs) — a
`#[derive(Copy)]` 40-byte POD enum — so reads copy out by value and never hand out a
reference into a live slot.

---

## Components

Construction hands out exactly three things, so the single-writer / single-mover rules are
enforced *by the type system*, not by convention:

```rust
let (ring, writer, releaser) = TxRing::new(capacity);
//   Arc<TxRing>     TxRingWriter   TxRingReleaser
```

| Handle | Count | Role |
|---|---|---|
| `Arc<TxRing>` | many (clone freely) | shared by **all readers**; exposes the read API and cursors |
| `TxRingWriter` | exactly one | the **only** producer; owns an `Arc<TxRing>` and drives `reserve` / `push` / `commit` / `rollback` |
| `TxRingReleaser` | exactly one | the **only** mover of the consume (release) index |

- `TxRingWriter` / `TxRingReleaser` are **not `Clone`** and are yielded once from `new()`.
- Readers are anonymous: each just holds an `Arc<TxRing>` and its own local `usize`
  cursor. There is no `Consumer` type — a reader can sit at any index inside the readable
  window and read any element or sub-range.

---

## Backpressure model — non-blocking, caller-driven

The ring **never blocks or sleeps internally**. Capacity pressure is expressed as a
*clamped grant*:

1. `writer.reserve()` grants *all* currently-free slots — everything the releaser has
   freed: `granted = capacity − (write − release)` — and returns the new `capacity()`.
2. `writer.capacity()` reports how many slots you may still `push` — possibly `0` when the
   ring is full.
3. When you exhaust the grant, call `writer.reserve()` again. It publishes progress
   (`commit`) and re-grants against the (possibly advanced) release index.

The caller decides what to do when `capacity() == 0` — spin, yield, do other work, or come
back later. No `WaitStrategy` lives inside the ring.

The gate lives entirely in `reserve`. `push` stays on the hot path: a bounds assert, one
unsynchronized slot write, and a cursor bump.

---

## Requirements & invariants

**Caller must uphold:**

- **Capacity is a power of two.** Enforced by an assert in `new()` (slot indexing is
  `cursor & (capacity − 1)`).
- **One writer, one releaser.** Guaranteed by construction — don't smuggle extra producers
  in by other means.
- **Readers stay in the window.** A reader's cursor must remain within
  `[release_index(), write_index())`. Reading below `release_index` races a writer
  overwrite; reading at/above `write_index` reads unpublished data. **`get()` enforces this
  at runtime** (see below) — a violation panics instead of returning stale or torn data.
- **The releaser only moves forward, and never past the writer.** `release` / `advance_to`
  are monotonic and must not exceed `write_index()`. Both are checked with `debug_assert!`.
- **The releaser must coordinate all readers.** Because the ring has one mover but many
  readers, the application is responsible for advancing the release index only once *every*
  reader is done with those slots (typically the slowest/last stage drives it).

**Element type:** the ring stores `WalEntry`. It is `Copy`, so `get()` returns a value and
no reference into a slot ever escapes (hence no `Sync` requirement on the element). Slots
are pre-filled with a zeroed placeholder (`WalEntry::Entry(TxEntry::zeroed())`) until the
writer overwrites them; the placeholder is never observed by a caller that respects the
window.

---

## API

### `TxRing` (via `Arc<TxRing>`, used by readers)

| Method | Ordering | Description |
|---|---|---|
| `new(capacity) -> (Arc<Self>, TxRingWriter, TxRingReleaser)` | — | allocate; assert power-of-two |
| `write_index() -> usize` | `Acquire` | exclusive upper bound of the readable window |
| `release_index() -> usize` | `Acquire` | lower bound; slots below may be overwritten |
| `capacity() -> usize` | — | ring size (slot count) |
| `get(idx) -> WalEntry` | read → `Acquire` fence → load | copy of slot `idx`; **panics if `idx` was outside the live window** |

### `TxRingWriter` (the single producer)

| Method | Ordering | Description |
|---|---|---|
| `writer.reserve() -> usize` | `Release` + `Acquire` | commit, then grant all free slots from the head; returns new `capacity()` |
| `writer.capacity() -> usize` | — | remaining slots you may still `push` (`0` = full) |
| `writer.pending() -> usize` | `Relaxed` read of `write` | pushed-but-uncommitted entries, the `[commit, cursor)` window |
| `writer.push(entry: WalEntry)` | — | write next slot; **panics if `capacity() == 0`** |
| `writer.walk(start, end, handler)` | — | visit `[start, end)` (wrapping) lending each `&WalEntry` — no copy |
| `writer.walk_pending(skip, handler)` | — | `walk` the pending window after `skip`: `[commit + skip, cursor)` |
| `writer.patch_pending(skip, f)` | — | back-patch the uncommitted entry at `[commit + skip]` (e.g. a metadata CRC) |
| `writer.commit()` | `Release` | publish the cursor so readers/releaser observe it |
| `writer.rollback(count) -> usize` | — | drop the last `count` uncommitted pushes (clamped); returns count rolled back |
| `drop(writer)` | `Release` | auto-`commit()` of whatever was pushed |

> `walk` is the one place a `&WalEntry` into a slot is handed out, and only to the **writer**:
> its target `[commit, cursor)` is pushed-but-unpublished, so no reader's window (`[release,
> write)`) overlaps it and the borrow cannot race. The reference never escapes the call.

### `TxRingReleaser` (the single consume-index mover)

| Method | Ordering | Description |
|---|---|---|
| `advance_to(index)` | `Release` | advance the release index to an absolute logical index (monotonic; debug-asserted) |
| `released() -> usize` | — | current release index (local) |

---

## Usage

### Producer

```rust
use storage::entities::WalEntry;

let (ring, mut writer, mut releaser) = TxRing::new(1024);

// Push a slice of entries, honoring backpressure with the long-lived writer.
let batch: &[WalEntry] = &entries;
writer.reserve();
let mut i = 0;
while i < batch.len() {
    if writer.capacity() == 0 {
        // Re-grant space the releaser has freed (also commits prior pushes).
        if writer.reserve() == 0 {
            std::hint::spin_loop(); // ring full — let a reader/releaser catch up
            continue;
        }
    }
    writer.push(batch[i]);
    i += 1;
}
// drop(writer) commits the tail
```

### Reader (any number, each with its own cursor)

```rust
let ring = Arc::clone(&ring);
let mut cursor = 0usize;
loop {
    let w = ring.write_index();      // Acquire: see all slots written before this point
    while cursor < w {
        let entry = ring.get(cursor); // copy out; panics if cursor left the window
        handle(entry);
        cursor += 1;
    }
    std::hint::spin_loop();
}
```

### Releaser (single mover — e.g. the last/slowest stage)

```rust
// Once every reader is done up to an absolute consumed position, free those
// slots back to the writer.
releaser.advance_to(global_consumed);
```

---

## Memory ordering & safety

Slots live in `Box<[UnsafeCell<WalEntry>]>`; correctness rests on a release/acquire edge on
each cursor:

- **Publish:** the writer fully writes a slot, then `write.store(Release)` in `flush`. A
  reader's `write_index()` (`Acquire`) that observes the new value is guaranteed to see the
  slot write.
- **Free:** the releaser publishes freed space with `release.store(Release)`; the writer
  observes it via an `Acquire` load in the `reserve` gate before reusing those slots.
- **`unsafe impl Send/Sync for TxRing`** is sound because the writer gates on `release`
  (never overwrites a readable slot) and readers only ever take a *copy*.

### Why `get()` reads before it checks

`get()` reads the slot value **first**, then — after an `Acquire` fence — loads `release`
and `write` and asserts `release ≤ idx < write`:

```rust
let value = unsafe { *slot(idx) };          // read first
fence(Acquire);
let (release, write) = (load(release), load(write));
assert!(release <= idx && idx < write);     // validate after
value
```

Checking *before* reading would be a TOCTOU race: the writer could advance `release` and
overwrite the slot between the check and the read. Reading first makes the check meaningful.
`release` is monotonic, so observing `release ≤ idx` *after* the read proves it was `≤ idx`
*throughout* the read — i.e. the writer had not been cleared to overwrite this slot, so the
value is not torn. If `idx` left the window, `get()` panics instead of returning stale data.

> A genuine concurrent overwrite (a caller that ignores the window) is still a data race;
> the in-`get()` check is a guard that turns most window violations into a panic rather than
> a silent stale/torn read, not a license to read out of range.

> The whole module currently carries `#![allow(dead_code)]`: it is finished, tested
> infrastructure that is **not yet wired into the pipeline**. Remove the allow once a stage
> consumes it.

---

## Tests

`cargo test -p ledger --lib tx_ring` covers grant clamping, full-ring → zero grant,
release-then-reserve, monotonic-release panics, wrap-around, the read-window guards
(`get` at/above `write_index` and below `release_index` both panic), and a multi-threaded
multi-reader broadcast. See [tests.rs](tests.rs).
