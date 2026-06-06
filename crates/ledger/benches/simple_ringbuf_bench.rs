use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

const PAYLOAD_SIZE: usize = 40;
const CAPACITY: usize = 8192;

#[derive(Clone, Copy)]
struct Payload(#[allow(dead_code)] [u8; PAYLOAD_SIZE]);

const _: () = assert!(size_of::<Payload>() == PAYLOAD_SIZE);

impl Payload {
    fn new(seed: u8) -> Self {
        Payload([seed; PAYLOAD_SIZE])
    }
}

// --- minimal lock-free SPSC ring buffer ---------------------------------
// Monotonic head/tail indices; empty when head == tail, full when tail - head
// == capacity. Slots are power-of-two sized so wrap is a mask. head and tail
// sit on separate cache lines to avoid false sharing between the two threads.

struct Ring<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>, // next index to read; only the consumer advances it
    tail: CachePadded<AtomicUsize>, // next index to write; only the producer advances it
}

// Safe because exactly one thread pushes and one pops; the halves never touch the same slot.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let mut head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        while head != tail {
            unsafe { (*self.slots[head & self.mask].get()).assume_init_drop() };
            head = head.wrapping_add(1);
        }
    }
}

struct Producer<T> {
    ring: Arc<Ring<T>>,
}

struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two(),
        "capacity must be a power of two"
    );
    let slots = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let ring = Arc::new(Ring {
        slots,
        mask: capacity - 1,
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
    });
    (Producer { ring: ring.clone() }, Consumer { ring })
}

impl<T> Producer<T> {
    fn push(&self, value: T) -> Result<(), T> {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let head = self.ring.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) == self.ring.slots.len() {
            return Err(value);
        }
        unsafe { (*self.ring.slots[tail & self.ring.mask].get()).write(value) };
        self.ring
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    fn pop(&self) -> Option<T> {
        let head = self.ring.head.load(Ordering::Relaxed);
        let tail = self.ring.tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let value = unsafe { (*self.ring.slots[head & self.ring.mask].get()).assume_init_read() };
        self.ring
            .head
            .store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

// --- bench: same harness as ringbuf_bench / array_queue_bench ------------

fn simple_ringbuf_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_ringbuf_spsc");
    group.throughput(Throughput::Bytes(PAYLOAD_SIZE as u64));
    group.measurement_time(Duration::from_secs(10));

    // Background thread publishes; the bencher thread is the consumer.
    {
        let (prod, cons) = channel::<Payload>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let pub_running = running.clone();
        let publisher = thread::spawn(move || {
            let payload = Payload::new(1);
            while pub_running.load(Ordering::Relaxed) {
                while prod.push(payload).is_err() {
                    if !pub_running.load(Ordering::Relaxed) {
                        return;
                    }
                    spin_loop();
                }
            }
        });

        group.bench_function("bg_publish__main_consume", |b| {
            b.iter(|| {
                loop {
                    if let Some(p) = cons.pop() {
                        break p;
                    }
                    spin_loop();
                }
            });
        });

        running.store(false, Ordering::Relaxed);
        let _ = publisher.join();
    }

    // Opposite direction: the bencher thread publishes; a background thread consumes.
    {
        let (prod, cons) = channel::<Payload>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let sub_running = running.clone();
        let consumer = thread::spawn(move || {
            while sub_running.load(Ordering::Relaxed) {
                while cons.pop().is_some() {}
                spin_loop();
            }
        });

        let payload = Payload::new(2);
        group.bench_function("bg_consume__main_publish", |b| {
            b.iter(|| {
                while prod.push(payload).is_err() {
                    spin_loop();
                }
            });
        });

        running.store(false, Ordering::Relaxed);
        let _ = consumer.join();
    }

    group.finish();
}

criterion_group!(benches, simple_ringbuf_spsc);
criterion_main!(benches);
