//! Throughput of the lock-free SPSC `TxRing`: 1 producer (writer) → 1 consumer
//! (reader) that reads every entry and releases its slots.
//!
//! `b.iter` pushes one entry; a persistent consumer thread runs for the whole
//! measurement, so the reported rate is the pipeline's sustainable throughput
//! (the producer stalls on a full ring until the reader frees space).

use criterion::{Criterion, Throughput};
use ledger::tx_ring::ring::TxRing;
use ledger::tx_ring::writer::TxRingWriter;
use std::hint::{black_box, spin_loop};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
        _pad1: [0; 8],
        account_id: id, // doubles as the sequence sentinel the sanity check verifies
        amount: 1,
        computed_balance: 0,
    })
}

/// Push one entry, committing in batches and reclaiming freed slots (blocking
/// on the reader) when the granted window is exhausted.
fn push_one(writer: &mut TxRingWriter, pending: &mut usize, entry: WalEntry) {
    if writer.capacity() == 0 {
        writer.commit(); // publish so the reader can free slots
        *pending = 0;
        while writer.grant() == 0 {
            spin_loop(); // wait for the reader to release
        }
    }
    writer.push(entry);
    *pending += 1;
    if *pending >= COMMIT_BATCH {
        writer.commit();
        *pending = 0;
    }
}

// ── 1 producer, 1 consumer (reads + releases) ───────────────────────────────

fn bench_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_ring/spsc_1p_1c");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let (mut writer, mut reader) = TxRing::new(CAPACITY);
    writer.grant();
    let running = Arc::new(AtomicBool::new(true));

    let run_c = running.clone();
    let consumer = thread::spawn(move || {
        let mut pos = 0usize;
        while run_c.load(Ordering::Relaxed) {
            let mut n = 0usize;
            reader.walk(pos, |e| {
                black_box(e);
                n += 1;
                true
            });
            if n == 0 {
                spin_loop();
                continue;
            }
            pos += n;
            reader.release_to(pos);
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

// ── Correctness self-check: every entry flows through in order ──────────────

fn sanity_spsc() {
    const ITEMS: usize = 2_000_000;
    let (mut writer, mut reader) = TxRing::new(1 << 14);
    writer.grant();

    let consumer = thread::spawn(move || {
        let (mut pos, mut expected, mut ok) = (0usize, 0u64, true);
        while pos < ITEMS {
            let mut n = 0usize;
            reader.walk(pos, |e| {
                if let WalEntry::Entry(t) = e {
                    ok &= t.account_id == expected;
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
            reader.release_to(pos);
        }
        (ok, expected)
    });

    let mut pending = 0usize;
    for i in 0..ITEMS {
        push_one(&mut writer, &mut pending, make_entry(i as u64));
    }
    writer.commit();

    let (ok, seen) = consumer.join().unwrap();
    assert!(ok, "consumer saw an out-of-order or torn entry");
    assert_eq!(seen, ITEMS as u64, "consumer missed items");
}

fn main() {
    sanity_spsc();
    let mut c = Criterion::default().configure_from_args();
    bench_spsc(&mut c);
    c.final_summary();
}
