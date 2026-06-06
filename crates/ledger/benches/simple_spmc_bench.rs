use criterion::{BenchmarkId, Criterion, Throughput};
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::hint::{black_box, spin_loop};
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

const PAYLOAD_SIZE: usize = 40;
const CAPACITY: usize = 1 << 16;
const PUBLISH_BATCH: usize = 100;
const STAGGER: Duration = Duration::from_millis(10);

#[derive(Clone, Copy)]
struct Payload(#[allow(dead_code)] [u8; PAYLOAD_SIZE]);

const _: () = assert!(size_of::<Payload>() == PAYLOAD_SIZE);

impl Payload {
    fn new(seed: u8) -> Self {
        Payload([seed; PAYLOAD_SIZE])
    }
}

// --- hand-rolled broadcast SPMC (LMAX Disruptor multicast) --------------
// One producer publishes a single `cursor` in batches of 100; every consumer
// reads every item through its own cursor. The producer gates on the slowest
// consumer so it never overwrites a slot that is still unread.

struct Ring<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    cursor: CachePadded<AtomicUsize>, // items published by the producer
    gates: Box<[CachePadded<AtomicUsize>]>, // items consumed, one cursor per consumer
}

// Safe: the producer gates on the consumer cursors so writer and readers never
// touch the same slot at once; T: Copy makes the multi-reader read-out sound.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    fn new(capacity: usize, consumers: usize) -> Arc<Self> {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        let gates = (0..consumers)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        Arc::new(Ring {
            slots,
            mask: capacity - 1,
            cursor: CachePadded::new(AtomicUsize::new(0)),
            gates,
        })
    }
}

struct Producer<T> {
    ring: Arc<Ring<T>>,
    pos: usize,      // next index to write
    pending: usize,  // items written since the last publish
    min_gate: usize, // cached slowest-consumer position
}

impl<T: Copy> Producer<T> {
    fn new(ring: Arc<Ring<T>>) -> Self {
        Producer {
            ring,
            pos: 0,
            pending: 0,
            min_gate: 0,
        }
    }

    // False when the ring is full, i.e. the slowest consumer is a full lap behind.
    fn push(&mut self, value: T) -> bool {
        if self.pos.wrapping_sub(self.min_gate) >= self.ring.slots.len() {
            self.min_gate = self
                .ring
                .gates
                .iter()
                .map(|g| g.load(Ordering::Acquire))
                .min()
                .unwrap();
            if self.pos.wrapping_sub(self.min_gate) >= self.ring.slots.len() {
                return false;
            }
        }
        unsafe { (*self.ring.slots[self.pos & self.ring.mask].get()).write(value) };
        self.pos = self.pos.wrapping_add(1);
        self.pending += 1;
        if self.pending >= PUBLISH_BATCH {
            self.ring.cursor.store(self.pos, Ordering::Release);
            self.pending = 0;
        }
        true
    }

    fn flush(&mut self) {
        self.ring.cursor.store(self.pos, Ordering::Release);
        self.pending = 0;
    }
}

struct Consumer<T> {
    ring: Arc<Ring<T>>,
    id: usize,
    seq: usize, // next index to read
    cached_cursor: usize,
}

impl<T: Copy> Consumer<T> {
    fn new(ring: Arc<Ring<T>>, id: usize) -> Self {
        Consumer {
            ring,
            id,
            seq: 0,
            cached_cursor: 0,
        }
    }

    fn try_read(&mut self) -> Option<T> {
        if self.seq >= self.cached_cursor {
            self.cached_cursor = self.ring.cursor.load(Ordering::Acquire);
            if self.seq >= self.cached_cursor {
                return None;
            }
        }
        let value =
            unsafe { (*self.ring.slots[self.seq & self.ring.mask].get()).assume_init_read() };
        self.seq = self.seq.wrapping_add(1);
        Some(value)
    }

    fn publish_gate(&self) {
        self.ring.gates[self.id].store(self.seq, Ordering::Release);
    }
}

// --- correctness self-check: every consumer sees every item, in order ---

fn broadcast_sanity_check() {
    const ITEMS: u64 = 5_000_000;
    const CONSUMERS: usize = 4;
    let ring = Ring::<u64>::new(1 << 14, CONSUMERS);
    let done = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..CONSUMERS)
        .map(|id| {
            let (ring, done) = (ring.clone(), done.clone());
            thread::spawn(move || {
                let mut cons = Consumer::new(ring, id);
                let mut expected: Option<u64> = None;
                let mut ok = true;
                loop {
                    match cons.try_read() {
                        Some(v) => {
                            if let Some(e) = expected {
                                ok &= v == e;
                            }
                            expected = Some(v.wrapping_add(1));
                        }
                        None => {
                            cons.publish_gate();
                            if done.load(Ordering::Acquire) && cons.try_read().is_none() {
                                break;
                            }
                            spin_loop();
                        }
                    }
                }
                (ok, expected)
            })
        })
        .collect();

    let mut prod = Producer::new(ring);
    for v in 0..ITEMS {
        while !prod.push(v) {
            spin_loop();
        }
    }
    prod.flush();
    done.store(true, Ordering::Release);

    for h in handles {
        let (ok, expected) = h.join().unwrap();
        assert!(ok, "broadcast consumer saw a gap, dup or tear");
        assert_eq!(expected, Some(ITEMS), "broadcast consumer missed items");
    }
}

// --- bench: 1 producer (main) broadcasting to N staggered consumers -----

fn spmc_broadcast(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc_broadcast");
    group.throughput(Throughput::Bytes(PAYLOAD_SIZE as u64));
    group.measurement_time(Duration::from_secs(5));

    for consumers in [1usize, 2, 4, 8] {
        let ring = Ring::<Payload>::new(CAPACITY, consumers);
        let running = Arc::new(AtomicBool::new(true));
        let handles: Vec<_> = (0..consumers)
            .map(|id| {
                let (ring, running) = (ring.clone(), running.clone());
                thread::spawn(move || {
                    thread::sleep(STAGGER * id as u32); // staggered start spreads readers across the ring
                    let mut cons = Consumer::new(ring, id);
                    let mut since = 0usize;
                    while running.load(Ordering::Relaxed) {
                        match cons.try_read() {
                            Some(v) => {
                                black_box(v);
                                since += 1;
                                if since >= PUBLISH_BATCH {
                                    cons.publish_gate();
                                    since = 0;
                                }
                            }
                            None => {
                                cons.publish_gate();
                                since = 0;
                                spin_loop();
                            }
                        }
                    }
                })
            })
            .collect();

        let mut prod = Producer::new(ring);
        let payload = Payload::new(1);
        group.bench_with_input(
            BenchmarkId::from_parameter(consumers),
            &consumers,
            |b, _| {
                b.iter(|| {
                    while !prod.push(payload) {
                        spin_loop();
                    }
                });
            },
        );

        running.store(false, Ordering::Relaxed);
        for h in handles {
            let _ = h.join();
        }
    }

    group.finish();
}

fn main() {
    broadcast_sanity_check();
    let mut c = Criterion::default().configure_from_args();
    spmc_broadcast(&mut c);
    c.final_summary();
}
