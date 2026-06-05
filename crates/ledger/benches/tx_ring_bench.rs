//! Throughput of the lock-free SPSC-gated `TxRing` under two pipeline shapes:
//!
//! * **2-stage** — 1 producer (writer) → 1 consumer that reads every entry and
//!   drives the releaser.
//! * **3-stage** — 1 producer → consumer-1 ("WAL": reads + publishes its
//!   progress) → consumer-2 ("snapshot": reads strictly behind consumer-1 and
//!   drives the releaser). This mirrors the real pipeline, where the releaser
//!   only frees slots the durable consumer has already read.
//!
//! `b.iter` pushes one entry; persistent consumer threads run for the whole
//! measurement, so the reported rate is the pipeline's sustainable throughput
//! (the producer stalls on a full ring until the releaser frees space).

use criterion::{Criterion, Throughput};
use ledger::tx_ring::ring::TxRing;
use ledger::tx_ring::writer::TxRingWriter;
use std::hint::{black_box, spin_loop};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use storage::entities::{EntryKind, TxEntry, WalEntry, WalEntryKind};

const CAPACITY: usize = 1 << 16;
const COMMIT_BATCH: usize = 100;

fn make_entry(id: u64) -> WalEntry {
    WalEntry::Entry(TxEntry {
        entry_type: WalEntryKind::TxEntry as u8,
        kind: EntryKind::Credit,
        _pad0: [0; 6],
        tx_id: id,
        account_id: 1,
        amount: 1,
        computed_balance: 0,
    })
}

/// Push one entry, committing in batches and reclaiming freed slots (blocking
/// on the releaser) when the granted window is exhausted.
fn push_one(writer: &mut TxRingWriter, pending: &mut usize, entry: WalEntry) {
    if writer.capacity() == 0 {
        writer.commit(); // publish so the consumers can free slots
        *pending = 0;
        while writer.grant() == 0 {
            spin_loop(); // wait for the releaser to advance
        }
    }
    writer.push(entry);
    *pending += 1;
    if *pending >= COMMIT_BATCH {
        writer.commit();
        *pending = 0;
    }
}

// ── 2-stage: 1 producer, 1 consumer (reads + releases) ──────────────────────

fn bench_2stage(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_ring/2stage_1p_1c");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let (ring, mut writer, mut releaser) = TxRing::new(CAPACITY);
    writer.grant();
    let running = Arc::new(AtomicBool::new(true));

    let ring_c = ring.clone();
    let run_c = running.clone();
    let consumer = thread::spawn(move || {
        let mut pos = 0usize;
        while run_c.load(Ordering::Relaxed) {
            let mut n = 0usize;
            ring_c.walk_entries(pos, |e| {
                black_box(e);
                n += 1;
                true
            });
            if n == 0 {
                spin_loop();
                continue;
            }
            pos += n;
            releaser.advance_to(pos);
        }
    });

    let mut pending = 0usize;
    let mut id = 0u64;
    group.bench_function("push", |b| {
        b.iter(|| {
            push_one(&mut writer, &mut pending, make_entry(id));
            id = id.wrapping_add(1);
        });
    });

    group.finish();
    running.store(false, Ordering::Relaxed);
    let _ = consumer.join();
}

// ── 3-stage: 1 producer, 1 WAL consumer, 1 snapshot consumer (releaser) ─────

fn bench_3stage(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_ring/3stage_1p_2c");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let (ring, mut writer, mut releaser) = TxRing::new(CAPACITY);
    writer.grant();
    let running = Arc::new(AtomicBool::new(true));
    let wal_progress = Arc::new(AtomicUsize::new(0));

    // Consumer 1 ("WAL"): reads every entry and publishes how far it has read.
    let ring_w = ring.clone();
    let run_w = running.clone();
    let wp_w = wal_progress.clone();
    let wal = thread::spawn(move || {
        let mut pos = 0usize;
        while run_w.load(Ordering::Relaxed) {
            let mut n = 0usize;
            ring_w.walk_entries(pos, |e| {
                black_box(e);
                n += 1;
                true
            });
            if n == 0 {
                spin_loop();
                continue;
            }
            pos += n;
            wp_w.store(pos, Ordering::Release);
        }
    });

    // Consumer 2 ("snapshot"): reads strictly behind WAL, then drives the releaser.
    let ring_s = ring.clone();
    let run_s = running.clone();
    let wp_s = wal_progress.clone();
    let snapshot = thread::spawn(move || {
        let mut pos = 0usize;
        while run_s.load(Ordering::Relaxed) {
            let target = wp_s.load(Ordering::Acquire);
            if target <= pos {
                spin_loop();
                continue;
            }
            let mut idx = pos;
            ring_s.walk_entries(pos, |e| {
                if idx >= target {
                    return false;
                }
                black_box(e);
                idx += 1;
                true
            });
            pos = idx;
            releaser.advance_to(pos);
        }
    });

    let mut pending = 0usize;
    let mut id = 0u64;
    group.bench_function("push", |b| {
        b.iter(|| {
            push_one(&mut writer, &mut pending, make_entry(id));
            id = id.wrapping_add(1);
        });
    });

    group.finish();
    running.store(false, Ordering::Relaxed);
    let _ = wal.join();
    let _ = snapshot.join();
}

// ── Correctness self-checks: every entry flows through in order ─────────────

fn sanity_2stage() {
    const ITEMS: usize = 2_000_000;
    let (ring, mut writer, mut releaser) = TxRing::new(1 << 14);
    writer.grant();

    let ring_c = ring.clone();
    let consumer = thread::spawn(move || {
        let (mut pos, mut expected, mut ok) = (0usize, 0u64, true);
        while pos < ITEMS {
            let mut n = 0usize;
            ring_c.walk_entries(pos, |e| {
                if let WalEntry::Entry(t) = e {
                    ok &= t.tx_id == expected;
                    expected += 1;
                }
                n += 1;
                true
            });
            if n == 0 {
                spin_loop();
                continue;
            }
            pos += n;
            releaser.advance_to(pos);
        }
        (ok, expected)
    });

    let mut pending = 0usize;
    for i in 0..ITEMS {
        push_one(&mut writer, &mut pending, make_entry(i as u64));
    }
    writer.commit();

    let (ok, seen) = consumer.join().unwrap();
    assert!(ok, "2-stage consumer saw an out-of-order or torn entry");
    assert_eq!(seen, ITEMS as u64, "2-stage consumer missed items");
}

fn sanity_3stage() {
    const ITEMS: usize = 2_000_000;
    let (ring, mut writer, mut releaser) = TxRing::new(1 << 14);
    writer.grant();
    let wal_progress = Arc::new(AtomicUsize::new(0));

    let ring_w = ring.clone();
    let wp_w = wal_progress.clone();
    let wal = thread::spawn(move || {
        let mut pos = 0usize;
        while pos < ITEMS {
            let mut n = 0usize;
            ring_w.walk_entries(pos, |_e| {
                n += 1;
                true
            });
            if n == 0 {
                spin_loop();
                continue;
            }
            pos += n;
            wp_w.store(pos, Ordering::Release);
        }
    });

    let ring_s = ring.clone();
    let wp_s = wal_progress.clone();
    let snapshot = thread::spawn(move || {
        let (mut pos, mut expected, mut ok) = (0usize, 0u64, true);
        while pos < ITEMS {
            let target = wp_s.load(Ordering::Acquire);
            if target <= pos {
                spin_loop();
                continue;
            }
            let mut idx = pos;
            ring_s.walk_entries(pos, |e| {
                if idx >= target {
                    return false;
                }
                if let WalEntry::Entry(t) = e {
                    ok &= t.tx_id == expected;
                    expected += 1;
                }
                idx += 1;
                true
            });
            pos = idx;
            releaser.advance_to(pos);
        }
        (ok, expected)
    });

    let mut pending = 0usize;
    for i in 0..ITEMS {
        push_one(&mut writer, &mut pending, make_entry(i as u64));
    }
    writer.commit();

    wal.join().unwrap();
    let (ok, seen) = snapshot.join().unwrap();
    assert!(ok, "3-stage snapshot saw an out-of-order or torn entry");
    assert_eq!(seen, ITEMS as u64, "3-stage snapshot missed items");
}

fn main() {
    sanity_2stage();
    sanity_3stage();
    let mut c = Criterion::default().configure_from_args();
    bench_2stage(&mut c);
    bench_3stage(&mut c);
    c.final_summary();
}
