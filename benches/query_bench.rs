use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use roda_ledger::index::IndexedTxEntry;
use roda_ledger::ledger::{Ledger, LedgerConfig};
use roda_ledger::snapshot::{QueryKind, QueryRequest, QueryResponse};
use roda_ledger::storage::{Storage, StorageConfig};
use roda_ledger::transaction::Operation;
use std::time::Duration;

fn query_transaction(ledger: &Ledger, tx_id: u64) -> Option<Vec<IndexedTxEntry>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetTransaction { tx_id },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::Transaction(result) => result.map(|r| r.entries),
        _ => panic!("unexpected response type"),
    }
}

fn query_account_history(
    ledger: &Ledger,
    account_id: u64,
    from_tx_id: u64,
    limit: usize,
) -> Vec<IndexedTxEntry> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    ledger.query(QueryRequest {
        kind: QueryKind::GetAccountHistory {
            account_id,
            from_tx_id,
            limit,
        },
        respond: Box::new(move |resp| {
            let _ = tx.send(resp);
        }),
    });
    match rx.recv().unwrap() {
        QueryResponse::AccountHistory(result) => result,
        _ => panic!("unexpected response type"),
    }
}

fn query_bench(c: &mut Criterion) {
    // ── Hot path benchmarks ──────────────────────────────────────────────────
    let mut group = c.benchmark_group("query_hot");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    // Set up a ledger with 100K transactions
    let mut ledger = Ledger::new(LedgerConfig::temp());
    ledger.start().unwrap();

    let tx_count = 100_000u64;
    let mut last_id = 0;
    for i in 0..tx_count {
        last_id = ledger.submit(Operation::Deposit {
            account: 1 + (i % 100),
            amount: 100,
            user_ref: i,
        });
    }
    ledger.wait_for_transaction(last_id);

    let mid_tx_id = tx_count / 2;

    group.bench_function("get_transaction_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            // Query recent transactions that are definitely in the hot cache
            let tx_id = last_id - (i % 1000);
            let _ = query_transaction(&ledger, tx_id);
        });
    });

    group.bench_function("get_transaction_miss", |b| {
        b.iter(|| {
            let _ = query_transaction(&ledger, u64::MAX);
        });
    });

    group.bench_function("get_account_history_10", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let account = 1 + (i % 100);
            let _ = query_account_history(&ledger, account, 0, 10);
        });
    });

    group.bench_function("get_account_history_100", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let account = 1 + (i % 100);
            let _ = query_account_history(&ledger, account, 0, 100);
        });
    });

    group.bench_function("get_account_history_from_tx_id", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let account = 1 + (i % 100);
            let _ = query_account_history(&ledger, account, mid_tx_id, 20);
        });
    });

    group.finish();
    drop(ledger);

    // ── Cold path benchmarks ─────────────────────────────────────────────────
    let mut group = c.benchmark_group("query_cold");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    // Create a ledger that triggers segment rotation, then seal
    let dir = {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("bench_query_bench_{}", nanos)
    };

    let cold_tx_count = 100_000u64;
    let mut sample_tx_ids = Vec::new();

    {
        let config = LedgerConfig {
            storage: StorageConfig {
                data_dir: dir.clone(),
                wal_segment_size_mb: 1,
                snapshot_frequency: 0,
                ..Default::default()
            },
            seal_check_internal: Duration::from_millis(1),
            ..Default::default()
        };
        let mut ledger = Ledger::new(config);
        ledger.start().unwrap();

        let mut last_id = 0;
        for i in 0..cold_tx_count {
            let id = ledger.submit(Operation::Deposit {
                account: 1 + (i % 100),
                amount: 100,
                user_ref: i,
            });
            if i % 1000 == 0 {
                sample_tx_ids.push(id);
            }
            last_id = id;
        }
        ledger.wait_for_transaction(last_id);
        ledger.wait_for_seal();
    }

    // Load all sealed segments for cold benchmarks
    let storage = Storage::new(StorageConfig {
        data_dir: dir.clone(),
        wal_segment_size_mb: 1,
        snapshot_frequency: 0,
        ..Default::default()
    })
    .unwrap();
    let mut segments = storage.list_all_segments().unwrap();
    for seg in &mut segments {
        seg.load().unwrap();
    }

    group.bench_function("search_tx_index", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 1) % sample_tx_ids.len();
            let target = sample_tx_ids[i];
            for seg in &segments {
                if let Ok(Some(_)) = seg.search_tx_index(target) {
                    break;
                }
            }
        });
    });

    group.bench_function("search_tx_index_and_read", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 1) % sample_tx_ids.len();
            let target = sample_tx_ids[i];
            for seg in &segments {
                if let Ok(Some(offset)) = seg.search_tx_index(target) {
                    let _ = seg.read_transaction_at_offset(offset);
                    break;
                }
            }
        });
    });

    group.bench_function("search_account_index_limit_10", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let account = 1 + (i % 100);
            for seg in &segments {
                let result = seg.search_account_index(account, 0, 10);
                if let Ok(ids) = result
                    && !ids.is_empty()
                {
                    break;
                }
            }
        });
    });

    group.bench_function("search_account_index_limit_100", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let account = 1 + (i % 100);
            for seg in &segments {
                let result = seg.search_account_index(account, 0, 100);
                if let Ok(ids) = result
                    && !ids.is_empty()
                {
                    break;
                }
            }
        });
    });

    group.finish();

    // Cleanup
    std::fs::remove_dir_all(&dir).ok();
}

criterion_group!(benches, query_bench);
criterion_main!(benches);
