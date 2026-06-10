use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ledger::transactor_computer::TransactorComputer;
use ledger::tx_ring::ring::TxRing;
use ledger::wait_strategy::WaitStrategy;
use std::time::Duration;

/// Benchmarks one Deposit's computation in `TransactorComputer`: debit the
/// target account and credit SYSTEM (zero-sum), folding both into the running
/// CRC. The ring is drained single-threaded so the writer never blocks; only
/// the public surface is used (`finalize_committed`/`ensure_capacity` are
/// `pub(crate)`), so the metadata trailer is left out — this isolates the
/// per-Deposit balance + CRC + ring-push cost.
fn deposit_bench(c: &mut Criterion) {
    const ACCOUNTS: u64 = 1_000_000;

    let mut group = c.benchmark_group("transactor_computer");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let (writer, mut reader) = TxRing::new(1 << 16);
    let mut s = TransactorComputer::new(ACCOUNTS as usize + 1, writer, WaitStrategy::Balanced);
    s.tx_ring_pusher.reserve();
    s.open_accounts(ACCOUNTS as u32, 0); // ids 1..=ACCOUNTS → OPEN
    s.tx_ring_pusher.commit();
    reader.release_to(reader.write_index());

    let mut tx_id = 1u64;
    group.bench_function("deposit", |b| {
        b.iter(|| {
            // Reclaim slots the reader released so the two pushes never hit a
            // full grant window (amortized — grant runs once per ~window).
            if s.tx_ring_pusher.capacity() < 2 {
                s.tx_ring_pusher.grant();
            }
            tx_id += 1;
            let account = (rand::random::<u64>() % ACCOUNTS) + 1;
            s.init(tx_id);
            s.begin(*b"DEPOSIT\0", 0, 0);
            s.debit(account, 100); // target += 100
            s.credit(0, 100); // SYSTEM (id 0) -= 100
            s.finalize_committed();
            s.tx_ring_pusher.commit();
            reader.release_to(reader.write_index());
        });
    });

    group.finish();
}

criterion_group!(benches, deposit_bench);
criterion_main!(benches);
