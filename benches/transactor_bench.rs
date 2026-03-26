use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_queue::ArrayQueue;
use roda_ledger::transaction::{Transaction, TransactionDataType, TransactionExecutionContext};
use roda_ledger::transactor::Transactor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct BenchData {
    amount: u64,
}

impl TransactionDataType for BenchData {
    fn process(&self, ctx: &mut TransactionExecutionContext<'_>) {
        ctx.credit(0, self.amount);
        ctx.debit(1, self.amount);
    }
}

fn transactor_bench(c: &mut Criterion) {
    let batch_size = 10_000;

    let mut group = c.benchmark_group("transactor");
    group.throughput(Throughput::Elements(batch_size as u64));
    group.measurement_time(Duration::from_secs(10));

    let inbound = Arc::new(ArrayQueue::new(batch_size as usize * 10));
    let outbound = Arc::new(ArrayQueue::new(batch_size as usize * 10));
    let running = Arc::new(AtomicBool::new(true));

    let mut transactor =
        Transactor::<BenchData>::new(inbound.clone(), outbound.clone(), running.clone());

    let handle = transactor.start();
    let mut current_id = 0;

    group.bench_function("process", |b| {
        b.iter(|| {
            for _ in 0..batch_size {
                current_id += 1;
                let mut tx = Transaction::new(BenchData { amount: 100 });
                tx.id = current_id;
                while inbound.push(tx).is_err() {
                    std::thread::yield_now();
                }
            }
            // Wait for all to be processed by checking outbound
            while outbound.len() < batch_size as usize {
                std::thread::yield_now();
            }
            // Drain outbound for next iteration
            while outbound.pop().is_some() {}
        });
    });

    group.finish();
    running.store(false, Ordering::Relaxed);
    let _ = handle.join();
}

criterion_group!(benches, transactor_bench);
criterion_main!(benches);
