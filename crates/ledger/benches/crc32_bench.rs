use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rustc_hash::FxHashMap;
use std::hint::black_box;

// CRC32C over a transaction's bytes (same `crc32c` crate the transactor uses):
// one `crc32c` seed followed by a `crc32c_append` per extra chunk. Throughput is
// reported in elements (chunk count) so per-chunk cost is comparable across
// variants. Both variants digest 120 bytes total.
fn crc32_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c");

    // 1 element x 120 bytes: a single contiguous digest.
    let one = [0xA5u8; 120];
    group.throughput(Throughput::Elements(1));
    group.bench_function("1x120B", |b| {
        b.iter(|| black_box(crc32c::crc32c(black_box(&one))));
    });

    // 3 elements x 40 bytes: seed + 2 appends (metadata + sub-items chaining).
    let three = [[0xA5u8; 40]; 3];
    group.throughput(Throughput::Elements(3));
    group.bench_function("3x40B", |b| {
        b.iter(|| {
            let mut digest = crc32c::crc32c(black_box(&three[0]));
            digest = crc32c::crc32c_append(digest, black_box(&three[1]));
            digest = crc32c::crc32c_append(digest, black_box(&three[2]));
            black_box(digest)
        });
    });

    group.finish();
}

/// Rolling CRC window: `last_crc` is the newest digest, `prev_crc` the one
/// before it. Kept in a (fast) `FxHashMap` so it lives on the heap.
struct CrcWindow {
    last_crc: u32,
    prev_crc: u32,
}

// Same digest, but each iteration seeds from `last_crc` and rolls the result
// back into a heap-resident HashMap (last→prev, current→last). The lookup +
// loop-carried `last_crc` dependency keeps the input out of registers and stops
// the CRC being const-folded or pipelined across iterations. FxHashMap keeps
// the lookup cheap so the measurement still reflects CRC cost, not hashing.
fn crc32_rolling_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c_rolling");

    // 1 element x 120 bytes, seeded by the rolling last_crc.
    let one = [0xA5u8; 120];
    let mut win1: FxHashMap<u64, CrcWindow> = FxHashMap::default();
    win1.insert(
        0,
        CrcWindow {
            last_crc: 0,
            prev_crc: 0,
        },
    );
    group.throughput(Throughput::Elements(1));
    group.bench_function("1x120B", |b| {
        b.iter(|| {
            let w = win1.get_mut(&0).unwrap();
            let current = crc32c::crc32c_append(w.last_crc, black_box(&one));
            w.prev_crc = w.last_crc;
            w.last_crc = current;
            black_box((w.last_crc, w.prev_crc))
        });
    });

    // 3 elements x 40 bytes: seed + 2 appends, chained through the window.
    let three = [[0xA5u8; 40]; 3];
    let mut win3: FxHashMap<u64, CrcWindow> = FxHashMap::default();
    win3.insert(
        0,
        CrcWindow {
            last_crc: 0,
            prev_crc: 0,
        },
    );
    group.throughput(Throughput::Elements(3));
    group.bench_function("3x40B", |b| {
        b.iter(|| {
            let w = win3.get_mut(&0).unwrap();
            let mut current = crc32c::crc32c_append(w.last_crc, black_box(&three[0]));
            current = crc32c::crc32c_append(current, black_box(&three[1]));
            current = crc32c::crc32c_append(current, black_box(&three[2]));
            w.prev_crc = w.last_crc;
            w.last_crc = current;
            black_box((w.last_crc, w.prev_crc))
        });
    });

    group.finish();
}

criterion_group!(benches, crc32_bench, crc32_rolling_bench);
criterion_main!(benches);
