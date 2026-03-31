use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::ArrayQueue;
use roda_ledger::ledger::PipelineMode;
use roda_ledger::transaction::{Operation, Transaction};
use roda_ledger::transactor::Transactor;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

fn transactor_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactor");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let inbound = Arc::new(ArrayQueue::new(1024));
    let outbound = Arc::new(ArrayQueue::new(1024));
    let running = Arc::new(AtomicBool::new(true));

    let mut transactor = Transactor::new(
        inbound.clone(),
        outbound.clone(),
        running.clone(),
        10_000_000,
        PipelineMode::LowLatency,
    );

    let handle = transactor.start();

    let outbound_drain = outbound.clone();
    let running_drain = running.clone();
    let drain_handle = thread::spawn(move || {
        while running_drain.load(Ordering::Relaxed) || !outbound_drain.is_empty() {
            while outbound_drain.pop().is_some() {
                // discard
            }
            spin_loop();
        }
    });

    let mut current_id = 0;

    group.bench_function("process", |b| {
        b.iter(|| {
            current_id += 1;
            let account_id = rand::random::<u64>() % 10_000_000;
            let mut tx = Transaction::new(Operation::Deposit {
                account: account_id,
                amount: 100,
                user_ref: 0,
            });
            tx.id = current_id;
            while let Err(returned_tx) = inbound.push(tx) {
                tx = returned_tx;
                spin_loop();
            }
        });
    });

    group.finish();
    running.store(false, Ordering::Relaxed);
    let _ = handle.join();
    let _ = drain_handle.join();
}

criterion_group!(benches, transactor_bench);
criterion_main!(benches);
