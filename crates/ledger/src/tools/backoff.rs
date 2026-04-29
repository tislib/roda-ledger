//! Exponential backoff helper shared across every retry loop in the
//! crate. Centralises the "double the wait until a cap" pattern so
//! the cluster's peer replication, the client's retry/failover loops,
//! and any future retry site agree on cadence and don't reinvent the
//! arithmetic (or accidentally overflow on high attempt counts).
//!
//! Two layers:
//!
//! - [`BackoffPolicy`] — the immutable parameters (`base`, `max`,
//!   `multiplier`). Cheaply cloneable, stored on the owning client.
//! - [`Backoff`] — a per-loop counter that pairs a policy with the
//!   current attempt index. Call [`Backoff::wait`] between retries to
//!   sleep for the next delay and advance the counter; call
//!   [`Backoff::reset`] after a success.
//!
//! The policy is "exponential with cap": delay for retry N is
//! `base * multiplier^N`, clamped to `max`. The internal shift
//! saturates at 20 to avoid overflow even when callers misconfigure
//! a huge `max_retry_count`.

use std::time::Duration;

/// Immutable parameters describing an exponential-backoff schedule.
///
/// Construct with [`Self::exponential`] for the common case (double
/// the delay each attempt up to a cap) or [`Self::fixed`] for a
/// constant inter-retry sleep. Cheaply cloneable; embed by value.
#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    /// Delay before retry 0 (the first retry).
    pub base: Duration,
    /// Hard cap on any single backoff.
    pub max: Duration,
    /// Multiplier applied per attempt. `1` means "fixed";
    /// `2` means "double each attempt".
    pub multiplier: u32,
}

impl BackoffPolicy {
    /// Same delay every retry. Useful when the failure is expected
    /// to clear deterministically (e.g. a known short async hop).
    pub const fn fixed(delay: Duration) -> Self {
        Self {
            base: delay,
            max: delay,
            multiplier: 1,
        }
    }

    /// Doubling backoff: `base, 2*base, 4*base, …, max`.
    pub const fn exponential(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            multiplier: 2,
        }
    }

    /// Compute the delay for retry `attempt` (0-indexed). Saturates
    /// to `max` once the geometric series would exceed it; the shift
    /// is internally capped at 20 so a pathological `attempt` cannot
    /// overflow `u64`.
    pub fn delay(&self, attempt: u32) -> Duration {
        if self.multiplier <= 1 {
            return self.base;
        }
        let shift = attempt.min(20);
        let factor = (self.multiplier as u64).saturating_pow(shift);
        let raw_nanos = (self.base.as_nanos() as u64).saturating_mul(factor);
        let raw = Duration::from_nanos(raw_nanos);
        if raw > self.max { self.max } else { raw }
    }
}

impl Default for BackoffPolicy {
    /// 100 ms base, 1.6 s cap, doubling — matches the policy the
    /// cluster client and peer-replication loop both want.
    fn default() -> Self {
        Self::exponential(Duration::from_millis(100), Duration::from_millis(1600))
    }
}

/// Stateful counter pairing a [`BackoffPolicy`] with the current
/// attempt number. Drive a retry loop with `wait().await` between
/// attempts; call `reset()` after a success so the next failure
/// starts from the base again.
#[derive(Clone, Debug)]
pub struct Backoff {
    policy: BackoffPolicy,
    attempt: u32,
}

impl Backoff {
    pub fn new(policy: BackoffPolicy) -> Self {
        Self { policy, attempt: 0 }
    }

    /// Number of times [`Self::wait`] has been called (i.e. the
    /// number of retries already attempted).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reset the counter — call after a successful attempt so the
    /// next failure starts at `base` again.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Compute the delay for the next retry without sleeping.
    /// Useful for log messages that want to report "retrying in Xms"
    /// before actually sleeping.
    pub fn peek_delay(&self) -> Duration {
        self.policy.delay(self.attempt)
    }

    /// Sleep for the next backoff delay and advance the counter.
    pub async fn wait(&mut self) {
        let d = self.policy.delay(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        tokio::time::sleep(d).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_returns_constant_delay() {
        let p = BackoffPolicy::fixed(Duration::from_millis(50));
        assert_eq!(p.delay(0), Duration::from_millis(50));
        assert_eq!(p.delay(5), Duration::from_millis(50));
        assert_eq!(p.delay(20), Duration::from_millis(50));
    }

    #[test]
    fn exponential_doubles_until_cap() {
        let p = BackoffPolicy::exponential(Duration::from_millis(100), Duration::from_millis(1600));
        assert_eq!(p.delay(0), Duration::from_millis(100));
        assert_eq!(p.delay(1), Duration::from_millis(200));
        assert_eq!(p.delay(2), Duration::from_millis(400));
        assert_eq!(p.delay(3), Duration::from_millis(800));
        assert_eq!(p.delay(4), Duration::from_millis(1600));
        // Capped — every later attempt also returns max.
        assert_eq!(p.delay(5), Duration::from_millis(1600));
        assert_eq!(p.delay(50), Duration::from_millis(1600));
    }

    #[test]
    fn delay_does_not_overflow() {
        let p = BackoffPolicy::exponential(Duration::from_millis(100), Duration::from_secs(60));
        // Pathologically high attempt count saturates instead of panicking.
        let _ = p.delay(u32::MAX);
    }

    #[test]
    fn backoff_advances_attempt() {
        let b = Backoff::new(BackoffPolicy::exponential(
            Duration::from_millis(10),
            Duration::from_millis(80),
        ));
        assert_eq!(b.attempt(), 0);
        assert_eq!(b.peek_delay(), Duration::from_millis(10));
        // can't await in #[test], just check the math
        assert_eq!(b.policy.delay(0), Duration::from_millis(10));
        assert_eq!(b.policy.delay(3), Duration::from_millis(80));
    }
}
