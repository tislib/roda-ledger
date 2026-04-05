use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{CompositeOperation, CompositeOperationFlags, Operation, Step};
use smallvec::smallvec;
use std::time::Duration;

fn ledger_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("ledger");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));
    let mut i = 0;

    for account_count in [1000, 1_000_000, 10_000_000, 50_000_000] {
        group.bench_function(format!("deposit_{}", account_count), |b| {
            let mut ledger = Ledger::new(LedgerConfig {
                max_accounts: account_count as usize,
                ..LedgerConfig::temp()
            });
            ledger.start().unwrap();
            b.iter(|| {
                i += 1;
                let account = 1 + rand::random::<u64>() % account_count;
                ledger.submit(Operation::Deposit {
                    account,
                    amount: 10000,
                    user_ref: 0,
                });
            });
        });
    }

    group.bench_function("composite_operation", |b| {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start().unwrap();
        b.iter(|| {
            i += 1;
            ledger.submit(Operation::Composite(Box::new(CompositeOperation {
                steps: smallvec![
                    Step::Credit {
                        account_id: 0, // SYSTEM_ACCOUNT_ID
                        amount: 100
                    },
                    Step::Debit {
                        account_id: i % 1000,
                        amount: 100
                    },
                ],
                flags: CompositeOperationFlags::empty(),
                user_ref: 12345,
            })));
        });
    });

    group.finish();
}

criterion_group!(benches, ledger_bench);
criterion_main!(benches);
