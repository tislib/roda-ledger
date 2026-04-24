//! Benchmarks for `cluster::Quorum` — the lock-free majority-commit tracker.
//!
//! The hot paths are:
//! - `advance(node_index, idx)` — fired on every successful `AppendEntries`
//!   from each peer task, plus once per commit on the leader itself (via
//!   `Ledger::on_commit`). Cost scales O(N) in cluster size because it
//!   snapshots every slot and sorts.
//! - `get()` — one `Acquire` load, meant to be cheap.
//!
//! Cluster sizes swept: 1 (single-node), 3, 5, 7, 9 — realistic Raft sizes.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::cluster::Quorum;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

const SIZES: &[usize] = &[1, 3, 5, 7, 9];

/// Monotonically advance the leader slot (slot 0). Models the single-node
/// case and the most frequent caller in multi-node: `Ledger::on_commit`
/// firing on each WAL commit.
fn bench_advance_leader_slot(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum/advance_leader_slot");
    group.throughput(Throughput::Elements(1));

    for &n in SIZES {
        let q = Quorum::new(n);
        let mut tx = 0u64;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                tx += 1;
                q.advance(black_box(0), black_box(tx));
            });
        });
    }
    group.finish();
}

/// Round-robin advance across every slot. Exercises the `advance` snapshot
/// + sort across the full slot array — the worst-case per-call cost.
fn bench_advance_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum/advance_round_robin");
    group.throughput(Throughput::Elements(1));

    for &n in SIZES {
        let q = Quorum::new(n);
        let mut tx = 0u64;
        let mut slot = 0u32;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                tx += 1;
                q.advance(black_box(slot), black_box(tx));
                slot = (slot + 1) % n as u32;
            });
        });
    }
    group.finish();
}

/// Pure `get()` hot-path. Represents the observability / future quorum-gated
/// commit read that runs many times per second.
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum/get");
    group.throughput(Throughput::Elements(1));

    for &n in SIZES {
        let q = Quorum::new(n);
        // Seed so `get()` has to acquire a non-zero value.
        for slot in 0..n as u32 {
            q.advance(slot, 1_000 + slot as u64);
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(q.get()));
        });
    }
    group.finish();
}

/// Concurrent advance from multiple threads, one per slot. Each thread owns
/// its slot (mirrors the real topology: one `PeerReplication` task per
/// peer + the leader's `on_commit` closure on slot 0). Measures the cost
/// of the atomic store + the contended `fetch_max` publish.
fn bench_advance_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum/advance_contended");
    group.throughput(Throughput::Elements(1));

    for &n in &[3usize, 5, 7] {
        let iters_per_thread = 10_000u64;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_custom(|iters| {
                // One timing round = `iters` total `advance` calls, split
                // evenly across the n threads.
                let q = Arc::new(Quorum::new(n));
                let stop = Arc::new(AtomicBool::new(false));
                let total_rounds = iters.div_ceil(iters_per_thread);
                let per_thread = total_rounds * iters_per_thread;

                let start = std::time::Instant::now();
                let handles: Vec<_> = (0..n)
                    .map(|slot| {
                        let q = q.clone();
                        let stop = stop.clone();
                        thread::spawn(move || {
                            let slot = slot as u32;
                            let mut tx = 0u64;
                            for _ in 0..per_thread {
                                if stop.load(Ordering::Relaxed) {
                                    break;
                                }
                                tx += 1;
                                q.advance(slot, tx);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                let elapsed = start.elapsed();
                stop.store(true, Ordering::Relaxed);
                // Scale back to one-call time: total work was `per_thread * n`.
                elapsed / (per_thread as u32 * n as u32 / iters as u32).max(1)
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets =
        bench_advance_leader_slot,
        bench_advance_round_robin,
        bench_get,
        bench_advance_contended,
);
criterion_main!(benches);
