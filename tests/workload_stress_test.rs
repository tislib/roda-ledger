use roda_ledger::ledger::Ledger;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::testing::stress::direct_workload_client::DirectWorkloadClient;
use roda_ledger::testing::stress::workload::{Limit, Power, RunConfig, Workload};
use roda_ledger::transaction::Operation;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_workload_deposit_sustain() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);

    let config = RunConfig {
        limit: Limit::Count(10),
        power: Power::Rate(100),
    };

    workload
        .run(config, |_| Operation::Deposit {
            account: 1001,
            amount: 100,
            user_ref: 0,
        })
        .expect("Deposit failed");

    // Wait for processing
    std::thread::sleep(Duration::from_millis(100));
    let balance = ledger.get_balance(1001);
    assert_eq!(balance, 1000);
}

#[test]
fn test_workload_transfer_spike_direct() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);
    let accounts = [1001, 1002, 1003];

    // First deposit some money
    let config_dep = RunConfig {
        limit: Limit::Count(3),
        power: Power::Full,
    };
    workload
        .run(config_dep, |idx| Operation::Deposit {
            account: accounts[idx as usize % accounts.len()],
            amount: 1_000_000_000,
            user_ref: 0,
        })
        .expect("Initial deposit failed");

    // Wait for deposits to be processed
    std::thread::sleep(Duration::from_millis(100));

    // Then run spike transfer
    let config = RunConfig {
        limit: Limit::Duration(Duration::from_millis(500)),
        power: Power::Full,
    };
    workload
        .run(config, |_| Operation::Transfer {
            from: 1001,
            to: 1002,
            amount: 10,
            user_ref: 0,
        })
        .expect("Spike failed");
}

#[test]
fn test_workload_peak_load_direct() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);

    use std::time::Instant;
    let start_time = Instant::now();
    let duration = Duration::from_secs(1);
    let peak_rate = 100u64;
    let interval = Duration::from_millis(100);
    let total_ticks = duration.as_secs_f64() / interval.as_secs_f64();
    let mut total_count = 0;

    for i in 0..(total_ticks as u64) {
        if start_time.elapsed() >= duration {
            break;
        }
        let progress = i as f64 / total_ticks;
        let current_rate = if progress < 0.5 {
            (progress * 2.0 * peak_rate as f64) as u64
        } else {
            ((1.0 - progress) * 2.0 * peak_rate as f64) as u64
        };

        let config = RunConfig {
            limit: Limit::Duration(interval),
            power: Power::Rate(current_rate.max(1)),
        };

        let (_, count) = workload
            .run_step(
                config,
                |_| Operation::Deposit {
                    account: 2005,
                    amount: 1,
                    user_ref: 0,
                },
                total_count,
            )
            .expect("Step failed");
        total_count += count;
    }
}

#[test]
fn test_workload_range_selector_direct() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);

    let config = RunConfig {
        limit: Limit::Count(5),
        power: Power::Full,
    };

    workload
        .run(config, |idx| {
            let account_id = 3000 + (idx % 11);
            Operation::Deposit {
                account: account_id,
                amount: 100,
                user_ref: 0,
            }
        })
        .expect("Range deposit failed");
}
