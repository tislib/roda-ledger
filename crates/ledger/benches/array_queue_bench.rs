use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::ArrayQueue;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

// Thread-to-thread throughput of crossbeam's lock-free ArrayQueue, both directions.
// Mirrors ringbuf_bench.rs so the two queues can be compared directly.
fn array_queue_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_queue_spsc");
    group.throughput(Throughput::Bytes(PAYLOAD_SIZE as u64));
    group.measurement_time(Duration::from_secs(10));

    // Background thread publishes; the bencher thread is the consumer.
    {
        let queue = Arc::new(ArrayQueue::<Payload>::new(CAPACITY));
        let running = Arc::new(AtomicBool::new(true));
        let pub_queue = queue.clone();
        let pub_running = running.clone();
        let publisher = thread::spawn(move || {
            let payload = Payload::new(1);
            while pub_running.load(Ordering::Relaxed) {
                while pub_queue.push(payload).is_err() {
                    if !pub_running.load(Ordering::Relaxed) {
                        return;
                    }
                    spin_loop();
                }
            }
        });

        group.bench_function("bg_publish__main_consume", |b| {
            b.iter(|| loop {
                if let Some(p) = queue.pop() {
                    break p;
                }
                spin_loop();
            });
        });

        running.store(false, Ordering::Relaxed);
        let _ = publisher.join();
    }

    // Opposite direction: the bencher thread publishes; a background thread consumes.
    {
        let queue = Arc::new(ArrayQueue::<Payload>::new(CAPACITY));
        let running = Arc::new(AtomicBool::new(true));
        let sub_queue = queue.clone();
        let sub_running = running.clone();
        let consumer = thread::spawn(move || {
            while sub_running.load(Ordering::Relaxed) {
                while sub_queue.pop().is_some() {}
                spin_loop();
            }
        });

        let payload = Payload::new(2);
        group.bench_function("bg_consume__main_publish", |b| {
            b.iter(|| {
                while queue.push(payload).is_err() {
                    spin_loop();
                }
            });
        });

        running.store(false, Ordering::Relaxed);
        let _ = consumer.join();
    }

    group.finish();
}

criterion_group!(benches, array_queue_spsc);
criterion_main!(benches);
