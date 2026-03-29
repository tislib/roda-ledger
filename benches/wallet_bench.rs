use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::transaction::{ComplexOperation, ComplexOperationFlags, Operation, Step};
use smallvec::smallvec;
use std::time::Duration;

fn wallet_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("ledger");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(60));
    let mut i = 0;

    group.bench_function("deposit", |b| {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start();
        b.iter(|| {
            i += 1;
            ledger.submit(Operation::Deposit {
                account: i % 1000,
                amount: 100,
                user_ref: 0,
            });
        });
    });

    for account_count in [1000, 1_000_000, 10_000_000, 50_000_000] {
        let mut ledger = Ledger::new(LedgerConfig {
            max_accounts: account_count as usize,
            ..LedgerConfig::temp()
        });
        ledger.start();

        // Pre-fill some balances
        let mut last_tx_id = 0;
        for i in 0..account_count {
            last_tx_id = ledger.submit(Operation::Deposit {
                account: i,
                amount: 10000,
                user_ref: 0,
            });
        }
        ledger.wait_for_transaction(last_tx_id);

        group.bench_function(format!("transfer_{}", account_count), |b| {
            b.iter(|| {
                i += 1;
                let from_account = 1 + rand::random::<u64>() % account_count;
                let to_account = 1 + (i + account_count / 2) % account_count;
                ledger.submit(Operation::Transfer {
                    from: from_account,
                    to: to_account,
                    amount: 10,
                    user_ref: 0,
                });
            });
        });
    }

    group.bench_function("complex_operation", |b| {
        let mut ledger = Ledger::new(LedgerConfig::temp());
        ledger.start();
        b.iter(|| {
            i += 1;
            ledger.submit(Operation::Complex(Box::new(ComplexOperation {
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
                flags: ComplexOperationFlags::empty(),
                user_ref: 12345,
            })));
        });
    });

    group.finish();
}

criterion_group!(benches, wallet_bench);
criterion_main!(benches);
