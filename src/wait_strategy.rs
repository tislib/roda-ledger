use serde::Deserialize;
use std::hint;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitStrategy {
    LowLatency,
    #[default]
    Balanced,
    LowCpu,
    Custom {
        spin_until: u64,
        yield_until: u64,
    },
}

pub struct Thresholds {
    pub spin_until: u64,
    pub yield_until: u64,
}

impl WaitStrategy {
    pub fn thresholds(&self) -> Thresholds {
        match self {
            WaitStrategy::LowLatency => Thresholds {
                spin_until: u64::MAX,
                yield_until: u64::MAX,
            },
            WaitStrategy::Balanced => Thresholds {
                spin_until: 32,
                yield_until: 16384,
            },
            WaitStrategy::LowCpu => Thresholds {
                spin_until: 0,
                yield_until: 100,
            },
            WaitStrategy::Custom {
                spin_until,
                yield_until,
            } => Thresholds {
                spin_until: *spin_until,
                yield_until: *yield_until,
            },
        }
    }

    #[inline(always)]
    pub fn wait_strategy(&self, retry_count: u64) {
        if matches!(self, WaitStrategy::LowLatency) {
            return;
        }

        let thresholds = self.thresholds();
        if retry_count < thresholds.spin_until {
            hint::spin_loop();
        } else if retry_count < thresholds.yield_until {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
