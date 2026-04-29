//! Election timer — deadlines as data (ADR-0017 §"Timeout Handling").
//!
//! The library never sleeps. The driver schedules `tokio::time::sleep_until`
//! against `Action::SetWakeup { at }`. This type owns:
//!
//! - the configured `[min, max]` window for randomization,
//! - the seeded RNG that picks a fresh deadline,
//! - the currently-armed `Instant` (or `None` while disarmed — the
//!   leader has no election timer).
//!
//! The state machine calls `arm(now)` whenever it transitions to a
//! state that needs a timer (Initializing, Follower, Candidate) and
//! `disarm()` on becoming Leader. `is_expired(now)` is a pure read on
//! every `Event::Tick`.

use std::time::{Duration, Instant};

use rand::SeedableRng;
use rand::rngs::StdRng;

#[derive(Clone, Copy, Debug)]
pub struct ElectionTimerConfig {
    pub min_ms: u64,
    pub max_ms: u64,
}

impl Default for ElectionTimerConfig {
    fn default() -> Self {
        // Raft reference values — kept identical to the existing
        // cluster::raft::ElectionTimerConfig so behaviour transfers
        // when the library replaces the in-place implementation.
        Self {
            min_ms: 150,
            max_ms: 300,
        }
    }
}

pub struct ElectionTimer {
    cfg: ElectionTimerConfig,
    rng: StdRng,
    deadline: Option<Instant>,
}

impl ElectionTimer {
    pub fn new(cfg: ElectionTimerConfig, seed: u64) -> Self {
        Self {
            cfg,
            rng: StdRng::seed_from_u64(seed),
            deadline: None,
        }
    }

    /// Roll a fresh deadline at `now + random_ms`. Returns the new
    /// deadline so the caller can emit `Action::SetWakeup { at }` if
    /// it's the soonest pending wakeup.
    pub fn arm(&mut self, now: Instant) -> Instant {
        use rand::Rng;
        let lo = self.cfg.min_ms.max(1);
        let hi = self.cfg.max_ms.max(lo + 1);
        let pick = self.rng.gen_range(lo..hi);
        let deadline = now + Duration::from_millis(pick);
        self.deadline = Some(deadline);
        deadline
    }

    /// Equivalent to `arm` — kept as its own method so the call site
    /// reads as "reset the election timer" rather than "set up a new
    /// one", matching Raft's terminology after a successful
    /// AppendEntries / vote-grant.
    pub fn reset(&mut self, now: Instant) -> Instant {
        self.arm(now)
    }

    /// Drop the deadline. Called on Leader entry — the leader does
    /// not run an election timer.
    pub fn disarm(&mut self) {
        self.deadline = None;
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// `true` iff the timer is armed and `now` has reached or
    /// exceeded the deadline.
    pub fn is_expired(&self, now: Instant) -> bool {
        match self.deadline {
            Some(d) => now >= d,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn fresh_timer_is_disarmed() {
        let timer = ElectionTimer::new(ElectionTimerConfig::default(), 7);
        assert_eq!(timer.deadline(), None);
        assert!(!timer.is_expired(t0()));
    }

    #[test]
    fn arm_picks_within_window() {
        let mut timer = ElectionTimer::new(
            ElectionTimerConfig {
                min_ms: 100,
                max_ms: 200,
            },
            7,
        );
        let now = t0();
        let deadline = timer.arm(now);
        let delta = deadline.duration_since(now);
        assert!(delta >= Duration::from_millis(100));
        assert!(delta < Duration::from_millis(200));
    }

    #[test]
    fn is_expired_fires_at_or_after_deadline() {
        let mut timer = ElectionTimer::new(
            ElectionTimerConfig {
                min_ms: 10,
                max_ms: 11,
            },
            7,
        );
        let now = t0();
        let deadline = timer.arm(now);
        assert!(!timer.is_expired(now));
        assert!(!timer.is_expired(deadline - Duration::from_micros(1)));
        assert!(timer.is_expired(deadline));
        assert!(timer.is_expired(deadline + Duration::from_secs(60)));
    }

    #[test]
    fn disarm_clears_deadline() {
        let mut timer = ElectionTimer::new(ElectionTimerConfig::default(), 7);
        timer.arm(t0());
        timer.disarm();
        assert_eq!(timer.deadline(), None);
        assert!(!timer.is_expired(t0()));
    }

    #[test]
    fn same_seed_produces_same_deadline_sequence() {
        let cfg = ElectionTimerConfig {
            min_ms: 100,
            max_ms: 200,
        };
        let now = t0();
        let mut a = ElectionTimer::new(cfg, 42);
        let mut b = ElectionTimer::new(cfg, 42);
        for _ in 0..16 {
            assert_eq!(a.arm(now), b.arm(now));
        }
    }
}
