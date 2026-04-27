//! Election-timer skeleton (ADR-0016 §3.8).
//!
//! Stage 3b ships only the type + API; no consumer awaits its
//! expiry yet. Stage 4 wires `await_expiry()` into the Initializing
//! → Candidate transition.
//!
//! Design notes:
//! - Each ElectionTimer is constructed with a randomised initial
//!   deadline within `[min_ms, max_ms]`. `reset()` re-randomises
//!   from the same range — every valid `AppendEntries` (including
//!   heartbeats) and every granted `RequestVote` should call it.
//! - `await_expiry()` resolves when `now() >= deadline` and the
//!   timer has not been reset since the await began. If a `reset()`
//!   races, we re-await against the new deadline.
//! - The timer is cancellation-safe (`tokio::select!` against it
//!   alongside other futures works).
//!
//! Implementation shape: a `tokio::sync::Notify` is signalled on
//! every `reset()`. `await_expiry` sleeps until the deadline; on
//! wake, if `Notify` has been signalled it loops with the new
//! deadline; otherwise it returns.

use rand::Rng;
use spdlog::debug;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Configuration for an [`ElectionTimer`]. Defaults match Raft
/// reference values (150–300 ms) and align with the user-chosen
/// "minimal init mode" — Stage 4 may make these per-cluster
/// configurable through `[cluster]`.
#[derive(Clone, Copy, Debug)]
pub struct ElectionTimerConfig {
    pub min_ms: u64,
    pub max_ms: u64,
}

impl Default for ElectionTimerConfig {
    fn default() -> Self {
        Self {
            min_ms: 150,
            max_ms: 300,
        }
    }
}

pub struct ElectionTimer {
    cfg: ElectionTimerConfig,
    /// Locked state. `Mutex` is fine here — contention is bounded
    /// to one timer-owning task + reset callers, all rare.
    state: Mutex<TimerState>,
    /// Pulsed on every `reset()`; `await_expiry` watches it to
    /// re-check the deadline if the user reset the timer mid-await.
    notify: Notify,
}

struct TimerState {
    deadline: Instant,
}

impl ElectionTimer {
    pub fn new(cfg: ElectionTimerConfig) -> Self {
        let deadline = Self::roll_deadline(&cfg);
        Self {
            cfg,
            state: Mutex::new(TimerState { deadline }),
            notify: Notify::new(),
        }
    }

    fn roll_deadline(cfg: &ElectionTimerConfig) -> Instant {
        let lo = cfg.min_ms.max(1);
        let hi = cfg.max_ms.max(lo + 1);
        let pick = rand::thread_rng().gen_range(lo..hi);
        Instant::now() + Duration::from_millis(pick)
    }

    /// Push the deadline forward to a freshly-randomised value
    /// within the configured window. Awakes any in-flight
    /// `await_expiry` so it re-checks against the new deadline.
    pub fn reset(&self) {
        let new_deadline = Self::roll_deadline(&self.cfg);
        let in_ms = new_deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        {
            let mut s = self.state.lock().expect("election timer mutex poisoned");
            s.deadline = new_deadline;
        }
        self.notify.notify_waiters();
        debug!(
            "election_timer: reset — next expiry in {}ms (window {}..{}ms)",
            in_ms, self.cfg.min_ms, self.cfg.max_ms
        );
    }

    /// The currently-scheduled deadline. Observability only — Stage
    /// 4 transitions consume this through `await_expiry`.
    pub fn deadline(&self) -> Instant {
        self.state
            .lock()
            .expect("election timer mutex poisoned")
            .deadline
    }

    /// Resolve when the deadline elapses. Cancellation-safe: drop
    /// the future to abandon the wait. Mid-await `reset()` causes
    /// us to re-arm against the new deadline.
    pub async fn await_expiry(&self) {
        let started = Instant::now();
        let mut iterations = 0u32;
        loop {
            let deadline = self.deadline();
            let now = Instant::now();
            if now >= deadline {
                debug!(
                    "election_timer: expired after {}ms (iterations={})",
                    started.elapsed().as_millis(),
                    iterations
                );
                return;
            }
            let sleep_for = deadline - now;
            iterations += 1;
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {
                    // Timer slept the full window — but a reset()
                    // could have raced. Re-check by looping.
                }
                _ = self.notify.notified() => {
                    // Deadline was bumped; re-read and try again.
                    debug!(
                        "election_timer: await_expiry re-armed after reset (iteration {})",
                        iterations
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::timeout;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fires_within_window() {
        let cfg = ElectionTimerConfig {
            min_ms: 50,
            max_ms: 80,
        };
        let timer = ElectionTimer::new(cfg);
        let started = Instant::now();
        timer.await_expiry().await;
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(50) && waited < Duration::from_millis(200),
            "timer fired at {:?}",
            waited
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reset_pushes_deadline_forward() {
        let cfg = ElectionTimerConfig {
            min_ms: 30,
            max_ms: 60,
        };
        let timer = Arc::new(ElectionTimer::new(cfg));

        // Reset every ~10 ms for 100 ms; the timer must NOT fire.
        let timer_for_reset = timer.clone();
        let resetter = tokio::spawn(async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                timer_for_reset.reset();
            }
        });

        let result = timeout(Duration::from_millis(100), timer.await_expiry()).await;
        assert!(
            result.is_err(),
            "timer fired despite resets (this would let elections happen under healthy heartbeats)"
        );
        resetter.await.unwrap();
    }
}
