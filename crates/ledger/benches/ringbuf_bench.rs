use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Producer, Split};
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

// Thread-to-thread throughput of a lock-free SPSC ring buffer, both directions.
// Throughput is reported in bytes; the `time:` line is per-element latency.
fn ringbuf_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("ringbuf_spsc");
    group.throughput(Throughput::Bytes(PAYLOAD_SIZE as u64));
    group.measurement_time(Duration::from_secs(10));

    // Background thread publishes; the bencher thread is the consumer.
    {
        let (mut prod, mut cons) = HeapRb::<Payload>::new(CAPACITY).split();
        let running = Arc::new(AtomicBool::new(true));
        let pub_running = running.clone();
        let publisher = thread::spawn(move || {
            let payload = Payload::new(1);
            while pub_running.load(Ordering::Relaxed) {
                while prod.try_push(payload).is_err() {
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
                    if let Some(p) = cons.try_pop() {
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
        let (mut prod, mut cons) = HeapRb::<Payload>::new(CAPACITY).split();
        let running = Arc::new(AtomicBool::new(true));
        let sub_running = running.clone();
        let consumer = thread::spawn(move || {
            while sub_running.load(Ordering::Relaxed) {
                while cons.try_pop().is_some() {}
                spin_loop();
            }
        });

        let payload = Payload::new(2);
        group.bench_function("bg_consume__main_publish", |b| {
            b.iter(|| {
                while prod.try_push(payload).is_err() {
                    spin_loop();
                }
            });
        });

        running.store(false, Ordering::Relaxed);
        let _ = consumer.join();
    }

    group.finish();
}

criterion_group!(benches, ringbuf_spsc);
criterion_main!(benches);
