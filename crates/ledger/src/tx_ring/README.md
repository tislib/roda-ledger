# `tx_ring` — single-writer, single-reader (SPSC) lock-free ring buffer

`TxRing` is the inter-stage transport between the transactor (producer) and the WAL
(consumer). One writer fills a fixed, power-of-two array of `WalEntry` slots; one reader
consumes them **in place** and advances a release index that bounds the writer so it can
never overwrite data the reader hasn't released yet.

`TxRing::new(capacity)` hands out exactly two handles — a [`TxRingWriter`] and a
[`TxRingReader`] (which both reads *and* releases). The ring itself lives behind an `Arc`
shared by the two handles and is **dropped when both handles are dropped**; it is never
exposed. Pipeline wiring (ADR-021): writer → transactor, reader → WAL; the snapshot stage
no longer touches the ring — it tails the durable WAL (`WalTailer::tail_entries`).

```
            release_index            write_index           release_index + capacity
                 │                         │                          │
   ── overwritable ──┤■■■■■■■■■■■■■■■■■■■■■■│── writer may fill ──────┤
                     └ readable window ────┘
                     [release_index, write_index)   ← the reader's cursor lives here
```

Invariant chain (always true): `release_index ≤ write_index ≤ release_index + capacity`.

The element type is fixed to [`WalEntry`](../../../storage/src/entities.rs) — a
`#[derive(Copy)]` 40-byte POD enum — so reads copy out by value and never hand out a
reference into a live slot.

---

## Components

Construction hands out exactly two handles, so the single-writer / single-reader rules are
enforced *by the type system*, not by convention:

```rust
let (writer, reader) = TxRing::new(capacity);
//   TxRingWriter   TxRingReader
```

| Handle | Count | Role |
|---|---|---|
| `TxRingWriter` | exactly one | the **only** producer; drives `reserve` / `push` / `commit` / `rollback` |
| `TxRingReader` | exactly one | the **only** consumer; reads (`walk`) **and** moves the release index (`release_to`) |

- Both handles hold an `Arc<TxRing>`; the ring is never exposed and drops when both do.
- `TxRingWriter` / `TxRingReader` are **not `Clone`** and are yielded once from `new()`.
- The reader holds its own local release cursor and walks the readable window
  `[released, write_index)`.

---

## Backpressure model — non-blocking, caller-driven

The ring **never blocks or sleeps internally**. Capacity pressure is expressed as a
*clamped grant*:

1. `writer.reserve()` grants *all* currently-free slots — everything the reader has
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
- **One writer, one reader.** Guaranteed by construction — `new()` yields exactly one of each
  and neither is `Clone`; don't smuggle extra producers or consumers in by other means.
- **The reader stays in the window.** The reader's cursor must remain within
  `[release_index(), write_index())`. Reading below `release_index` races a writer overwrite;
  reading at/above `write_index` reads unpublished data. `walk` stops at the `write_index`
  snapshot it takes on entry and `debug_assert!`s `from >= released`.
- **The reader only releases forward, and never past the writer.** `release_to` is monotonic
  and must not exceed `write_index()`; both are checked with `debug_assert!`.
- **The reader gates the writer.** Because the reader is the sole release mover, it advances
  the release index only once it is done with those slots — that is what frees them back to
  the writer.

**Element type:** the ring stores `WalEntry`. It is `Copy`, so `get()` returns a value and
no reference into a slot ever escapes (hence no `Sync` requirement on the element). Slots
are pre-filled with a zeroed placeholder (`WalEntry::Entry(TxEntry::zeroed())`) until the
writer overwrites them; the placeholder is never observed by a caller that respects the
window.

---

## API

### `TxRing` (constructor only; the ring itself is never exposed)

| Method | Ordering | Description |
|---|---|---|
| `TxRing::new(capacity) -> (TxRingWriter, TxRingReader)` | — | allocate (assert power-of-two) and hand out the writer + reader; the `Arc<TxRing>` lives behind them |

### `TxRingWriter` (the single producer)

| Method | Ordering | Description |
|---|---|---|
| `writer.grant() -> usize` | `Acquire` | extend the window to all free slots **without** publishing; returns new `capacity()` |
| `writer.reserve() -> usize` | `Release` + `Acquire` | `commit`, then `grant`; returns new `capacity()` |
| `writer.capacity() -> usize` | — | remaining slots you may still `push` (`0` = full) |
| `writer.cursor() -> usize` | — | the next `ring_index` this writer will write to (the head) |
| `writer.push(entry: WalEntry) -> usize` | — | write at the head, return the `ring_index` written; **panics if `capacity() == 0`** |
| `writer.walk(start, end, handler)` | — | visit `ring_index` range `[start, end)` (wrapping) lending each `&WalEntry` — no copy |
| `writer.commit()` | `Release` | publish the head so the reader observes it |
| `writer.rollback_to(ring_index)` | — | move the head back to `ring_index`, discarding the uncommitted tail |
| `drop(writer)` | `Release` | auto-`commit()` of whatever was pushed |

All positions are absolute `ring_index`es — monotonic `usize`s starting at 0, masked to
physical slots only on access.

> `walk` is the one place a `&WalEntry` into a slot is handed out, and only to the **writer**:
> its target `[commit, cursor)` is pushed-but-unpublished, so no reader's window (`[release,
> write)`) overlaps it and the borrow cannot race. The reference never escapes the call.

### `TxRingReader` (the single consumer — reads *and* releases)

| Method | Ordering | Description |
|---|---|---|
| `reader.walk(from, handler)` | `Acquire` | visit published entries from `from` up to the `write_index` snapshot taken on entry, handing each a **copy** to `handler`; stop early when it returns `false`. `from` must be `>= released` |
| `reader.release_to(index)` | `Release` | advance the release index to an absolute logical index, freeing those slots back to the writer (monotonic, `≤ write_index()`; debug-asserted) |
| `reader.released() -> usize` | — | current release index (local) |
| `reader.write_index() -> usize` | `Acquire` | exclusive upper bound of the readable window |
| `reader.capacity() -> usize` | — | ring size (slot count) |

> `walk` hands `handler` a `WalEntry` **by value** — the reader copies out of the slot and never
> lends a reference into a live one, so the borrow can't race the writer. (`get(idx)`, a random-access
> copy, exists only behind `#[cfg(test)]`.)

---

## Usage

### Producer

```rust
use storage::entities::WalEntry;

let (mut writer, mut reader) = TxRing::new(1024);

// Push a slice of entries, honoring backpressure with the long-lived writer.
let batch: &[WalEntry] = &entries;
writer.reserve();
let mut i = 0;
while i < batch.len() {
    if writer.capacity() == 0 {
        // Re-grant space the reader has freed (also commits prior pushes).
        if writer.reserve() == 0 {
            std::hint::spin_loop(); // ring full — let the reader catch up
            continue;
        }
    }
    writer.push(batch[i]);
    i += 1;
}
// drop(writer) commits the tail
```

### Reader (the single consumer — reads, then releases)

```rust
let mut consumed = 0usize;
loop {
    // walk copies out every published entry from `consumed` to the current
    // write_index; the reader never lends a slot reference.
    reader.walk(consumed, |entry| {
        handle(entry);
        consumed += 1;
        true
    });
    // Done with those slots — free them back to the writer.
    reader.release_to(consumed);
    std::hint::spin_loop();
}
```

---

## Memory ordering & safety

Slots live in `Box<[UnsafeCell<WalEntry>]>`; correctness rests on a release/acquire edge on
each cursor:

- **Publish:** the writer fully writes a slot, then `write.store(Release)` in `commit`. A
  reader's `write_index()` (`Acquire`) that observes the new value is guaranteed to see the
  slot write.
- **Free:** the reader publishes freed space with `release.store(Release)` in `release_to`; the
  writer observes it via an `Acquire` load in the `reserve` gate before reusing those slots.
- **`unsafe impl Send/Sync for TxRing`** is sound because the writer gates on `release`
  (never overwrites a readable slot) and the reader only ever takes a *copy*.

### Why reads need no per-slot window check

`walk` takes the `write_index` (`Acquire`) on entry and only reads `[from, write_index)`. Because
the single reader is also the sole release mover, the writer is gated on a release index that this
reader has already advanced past those slots — so an in-window slot is never overwritten mid-read,
and the read copies out by value with no per-entry fence-and-assert. The caller upholds the window
(`from >= released`), checked by a `debug_assert!`. Reading out of the window is a data race, not a
guarded panic.

> The module still carries `#![allow(dead_code)]` because not every handle method has a caller yet
> (e.g. the test-only `get`); the ring itself **is** wired into the pipeline — the WAL stage holds
> the `TxRingReader` and the transactor holds the `TxRingWriter` (ADR-021).

---

## Tests

`cargo test -p ledger --lib tx_ring` covers grant clamping, full-ring → zero grant,
release-then-reserve, monotonic-release debug-asserts (release backward / past the write
cursor both panic), the power-of-two assert, in-window reads, wrap-around, `push`'s returned
ring index, half-open `walk` ranges, and `rollback_to`. See [tests.rs](tests.rs).
