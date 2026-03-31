use crate::ledger::{Ledger, LedgerConfig, PipelineMode};
use crate::transaction::Operation;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct TuneResult {
    pub value: u64,
    pub tps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuningStrategy {
    PowerOfTwo,
    BinarySearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuneParameter {
    QueueSize,
    SpinUntil,
    YieldUntil,
}

pub fn run_power_tune(
    total_txs: u64,
    parameter: TuneParameter,
    min_val: u64,
    max_val: u64,
    strategy: TuningStrategy,
    base_config: LedgerConfig,
) -> Vec<TuneResult> {
    let mut results = Vec::new();
    let mut tested = std::collections::HashMap::new();

    let test = |val: u64,
                results: &mut Vec<TuneResult>,
                tested: &mut std::collections::HashMap<u64, f64>| {
        if let Some(&tps) = tested.get(&val) {
            return tps;
        }

        println!("Testing {:?}: {}", parameter, val);
        let mut config = base_config.clone();
        match parameter {
            TuneParameter::QueueSize => config.queue_size = val as usize,
            TuneParameter::SpinUntil => {
                config.pipeline_mode = PipelineMode::Custom {
                    spin_until: val,
                    yield_until: config.pipeline_mode.thresholds().yield_until,
                };
            }
            TuneParameter::YieldUntil => {
                config.pipeline_mode = PipelineMode::Custom {
                    spin_until: config.pipeline_mode.thresholds().spin_until,
                    yield_until: val,
                };
            }
        }

        let mut ledger = Ledger::new(config);
        ledger.start();

        let start = Instant::now();
        let mut last_id = 0;
        for i in 0..total_txs {
            last_id = ledger.submit(Operation::Deposit {
                account: i % 1000,
                amount: 1,
                user_ref: 0,
            });
        }

        ledger.wait_for_transaction(last_id);
        let duration = start.elapsed();
        let tps = total_txs as f64 / duration.as_secs_f64();

        println!("  Time: {:?}, TPS: {:.2}", duration, tps);
        results.push(TuneResult { value: val, tps });
        tested.insert(val, tps);
        tps
    };

    match strategy {
        TuningStrategy::PowerOfTwo => {
            let mut val = if min_val > 0 {
                val_to_power_of_two(min_val)
            } else {
                1
            };
            if parameter == TuneParameter::QueueSize && val < 16 {
                val = 16;
            }

            while val <= max_val {
                test(val, &mut results, &mut tested);
                val *= 2;
            }
        }
        TuningStrategy::BinarySearch => {
            let mut low = min_val;
            if parameter == TuneParameter::QueueSize && low < 16 {
                low = 16;
            }
            let mut high = max_val;

            while low < high {
                let mid = (low + high) / 2;
                let tps1 = test(mid, &mut results, &mut tested);
                let tps2 = test(mid + 1, &mut results, &mut tested);

                if tps1 < tps2 {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
        }
    }

    results.sort_by(|a, b| b.tps.partial_cmp(&a.tps).unwrap());
    results
}

fn val_to_power_of_two(v: u64) -> u64 {
    if v.is_power_of_two() {
        v
    } else {
        v.next_power_of_two()
    }
}
