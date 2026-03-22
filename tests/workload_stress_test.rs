use roda_ledger::ledger::Ledger;
use roda_ledger::ledger::LedgerConfig;
use roda_ledger::testing::stress::workload::{AccountSelector, Workload, RunConfig, Limit, Power};
use roda_ledger::testing::stress::direct_workload_client::DirectWorkloadClient;
use roda_ledger::wallet::balance::WalletBalance;
use roda_ledger::wallet::transaction::WalletTransaction;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_workload_deposit_sustain() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::<WalletTransaction, WalletBalance>::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);
    
    let config = RunConfig {
        limit: Limit::Count(10),
        power: Power::Rate(100),
    };

    workload.deposit(AccountSelector::Single(1001), 100, config).expect("Deposit failed");

    // Wait for processing
    std::thread::sleep(Duration::from_millis(100));
    let balance = ledger.get_balance(1001);
    assert_eq!(balance.balance, 1000);
}

#[test]
fn test_workload_transfer_spike_direct() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::<WalletTransaction, WalletBalance>::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client).with_accounts(vec![1001, 1002, 1003]);
    
    // First deposit some money
    let config_dep = RunConfig {
        limit: Limit::Count(3),
        power: Power::Full,
    };
    workload.deposit(AccountSelector::All, 1000, config_dep).expect("Initial deposit failed");

    // Then run spike transfer
    let config = RunConfig {
        limit: Limit::Duration(Duration::from_millis(500)),
        power: Power::Full,
    };
    workload.run(config, |_| {
        WalletTransaction::transfer(1001, 1002, 10)
    }).expect("Spike failed");
}

#[test]
fn test_workload_peak_load_direct() {
    let ledger_config = LedgerConfig {
        in_memory: true,
        queue_size: 1024,
        ..Default::default()
    };

    let mut ledger = Ledger::<WalletTransaction, WalletBalance>::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client).with_accounts((2000..2010).collect());
    
    use std::time::Instant;
    let start_time = Instant::now();
    let duration = Duration::from_secs(1);
    let peak_rate = 100u64;
    let interval = Duration::from_millis(100);
    let total_ticks = duration.as_secs_f64() / interval.as_secs_f64();
    let mut total_count = 0;

    for i in 0..(total_ticks as u64) {
        if start_time.elapsed() >= duration { break; }
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

        let (_, count) = workload.run_step(config, |_| WalletTransaction::deposit(2005, 1), total_count).expect("Step failed");
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

    let mut ledger = Ledger::<WalletTransaction, WalletBalance>::new(ledger_config);
    ledger.start();
    let ledger = Arc::new(ledger);

    let client = DirectWorkloadClient::new(ledger.clone());
    let mut workload = Workload::new(client);
    
    let config = RunConfig {
        limit: Limit::Count(5),
        power: Power::Full,
    };

    workload.deposit(AccountSelector::Range { start: 3000, end: 3010 }, 100, config).expect("Range deposit failed");
}
