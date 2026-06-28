//! Server-side `LatencyProbe.Probe` RPC (`latency-probe` feature).
//!
//! The probe measures *inside* the node, so these tests drive it
//! in-process via the harness rather than over the derived
//! `client_port + 2000` socket (which can overflow `u16` under the
//! harness's OS-assigned ports — same rationale as the fault injector).
#![cfg(feature = "latency-probe")]

use cluster::testing::{ClusterTestingConfig, ClusterTestingControl};
use proto::latency::WaitLevel;
use std::time::{Duration, Instant};

async fn standalone(label: &str) -> ClusterTestingControl {
    ClusterTestingControl::start(ClusterTestingConfig {
        label: label.to_string(),
        ..ClusterTestingConfig::standalone()
    })
    .await
    .expect("standalone bring-up")
}

/// Each stage returns the requested number of samples and never times
/// out: with the pipeline idle (accounts pre-opened, no load) every
/// stage has already caught up to the sequencer head, so each ambient
/// probe completes immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_returns_samples_for_each_stage() {
    let ctl = standalone("latency_probe_stages").await;

    for level in [
        WaitLevel::Computed,
        WaitLevel::Committed,
        WaitLevel::Snapshot,
    ] {
        let resp = ctl
            .probe_latency_at(0, level, 16, 0)
            .await
            .unwrap_or_else(|e| panic!("probe {level:?}: {e}"));
        assert_eq!(resp.latencies_ns.len(), 16, "{level:?} sample count");
        assert_eq!(resp.timeouts, 0, "{level:?} timeouts");
    }
}

/// `probe_count == 0` is treated as a single sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_zero_count_is_one_sample() {
    let ctl = standalone("latency_probe_zero").await;

    let resp = ctl
        .probe_latency_at(0, WaitLevel::Committed, 0, 0)
        .await
        .expect("probe");
    assert_eq!(resp.latencies_ns.len(), 1);
    assert_eq!(resp.timeouts, 0);
}

/// `probe_interval_ms` spaces samples out: 3 probes with a 100 ms gap
/// spend at least two gaps (~200 ms) of wall time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_honors_interval() {
    let ctl = standalone("latency_probe_interval").await;

    let start = Instant::now();
    let resp = ctl
        .probe_latency_at(0, WaitLevel::Committed, 3, 100)
        .await
        .expect("probe");
    let elapsed = start.elapsed();

    assert_eq!(resp.latencies_ns.len(), 3);
    assert!(
        elapsed >= Duration::from_millis(180),
        "expected >= ~200ms from two inter-probe gaps, got {elapsed:?}"
    );
}
