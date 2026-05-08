//! Metrics collected while a scenario is running.
//!
//! Two streams are gathered concurrently with the scenario itself:
//!
//! 1. **Pipeline samples** — a background poller snapshots
//!    `get_pipeline_index` on every node at a fixed cadence
//!    (default 100 ms). Each sample carries every node's
//!    `(compute, commit, snapshot, cluster_commit)` plus which node
//!    reports as leader at sample time. These samples drive
//!    throughput and per-node lag stats.
//!
//! 2. **Submit latencies** — every `Submit` / `SubmitBatch` op that
//!    waits at a non-`None` level pushes its observed elapsed time
//!    into a flat list. Fire-and-forget submits (`WaitLevel::None`)
//!    contribute nothing here; their throughput shows up in the
//!    pipeline samples.
//!
//! Aggregation (avg / min / max / p50 / p99 / lag-per-node) is the
//! caller's job. Keeping the collector raw keeps it cheap and gives
//! the CLI flexibility to render whatever shape it wants.

use std::sync::Arc;
use std::time::{Duration, Instant};

use client::ClusterClient;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

/// One probe of every node's pipeline indices, taken `at` after the
/// run started.
#[derive(Clone, Debug)]
pub struct Sample {
    /// Time since `MetricsCollector::start_clock` was set (i.e. since
    /// the run began).
    pub at: Duration,
    /// Index of whichever node reported `is_leader = true` at sample
    /// time. `None` if no node was reachable / leader at the time.
    pub leader_idx: Option<usize>,
    /// One entry per node, indexed by `node_idx`.
    pub per_node: Vec<NodePipelineSnap>,
}

/// One node's pipeline indices at a moment in time.
#[derive(Clone, Copy, Debug)]
pub struct NodePipelineSnap {
    pub node_idx: usize,
    pub compute: u64,
    pub commit: u64,
    pub snapshot: u64,
    pub cluster_commit: u64,
    pub is_leader: bool,
}

/// One observed submit-RPC round-trip time, stamped with when the
/// submit call returned (relative to the collector's start). The
/// timestamp lets the CLI bucket latencies into per-second windows
/// for the streaming progress table; without it we'd only know the
/// run-wide distribution.
#[derive(Clone, Copy, Debug)]
pub struct LatencyPoint {
    pub at: Duration,
    pub latency: Duration,
}

#[derive(Default)]
struct CollectorInner {
    samples: Vec<Sample>,
    submit_latencies: Vec<LatencyPoint>,
}

/// Shared metrics sink. The runner clones the `Arc` into spawned
/// branches so latency points and pipeline samples land in one place
/// even when a scenario fans out via `AsyncBranch`.
pub struct MetricsCollector {
    inner: Mutex<CollectorInner>,
    start: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CollectorInner::default()),
            start: Instant::now(),
        }
    }

    /// Push one observed submit latency. Stamped with `now -
    /// collector.start` so the CLI can bucket latencies into the
    /// per-second progress table. Recorded for every submit
    /// (waiting or fire-and-forget) — for fire-and-forget it
    /// captures the submission RPC's round-trip time; for waiting
    /// submits it captures the round-trip plus pipeline-stage wait.
    pub fn record_submit_latency(&self, latency: Duration) {
        let at = self.start.elapsed();
        self.inner.lock().submit_latencies.push(LatencyPoint { at, latency });
    }

    /// Push one pipeline sample. Called from the background poller.
    pub fn push_sample(&self, sample: Sample) {
        self.inner.lock().samples.push(sample);
    }

    /// Snapshot the collected data. Cheap clone — samples are small.
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        Snapshot {
            samples: inner.samples.clone(),
            submit_latencies: inner.submit_latencies.clone(),
        }
    }

    /// Reference clock used to stamp samples' `at` field. Exposed so
    /// the poller can stamp samples consistently with anything
    /// outside the collector that wants a since-start duration.
    pub fn start(&self) -> Instant {
        self.start
    }

    /// Take an out-of-band sample right now. Used by the runner to
    /// pin the report's start and end so the throughput delta covers
    /// the entire run window — the periodic poller starts ~one
    /// interval in and stops ~one interval before the run ends, so
    /// without these bookend samples short bursts can fall outside
    /// the sampled range.
    pub async fn snapshot_now(&self, client: &ClusterClient) {
        if let Some(s) = probe_once(client, self.start).await {
            self.push_sample(s);
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Plain data the runner returns at end-of-run alongside the
/// `Result`. Carries enough for the CLI to render aggregates
/// (throughput from `cluster_commit` movement, latency percentiles,
/// per-node lag) without re-querying the cluster.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub samples: Vec<Sample>,
    pub submit_latencies: Vec<LatencyPoint>,
}

/// Throughput aggregate computed from `cluster_commit` deltas
/// between consecutive samples. Throughput is intentionally not
/// derived from per-node `commit_index` — the user-visible figure
/// is quorum-committed, which is what `cluster_commit` reports.
#[derive(Clone, Debug, Default)]
pub struct ThroughputStats {
    /// Number of samples that fed the series.
    pub samples: usize,
    /// Total ops landed on `cluster_commit` between the first and
    /// last sample.
    pub ops_total: u64,
    /// Wall-clock between the first and last sample.
    pub duration: Duration,
    /// `ops_total / duration`.
    pub avg_ops_per_sec: f64,
    /// Smallest per-interval rate seen during the run.
    pub min_ops_per_sec: f64,
    /// Largest per-interval rate seen during the run.
    pub max_ops_per_sec: f64,
}

/// Per-call latency aggregate, populated from waiting submits only.
/// Fire-and-forget submits contribute nothing here.
#[derive(Clone, Debug)]
pub struct LatencyStats {
    pub samples: usize,
    pub min: Duration,
    pub max: Duration,
    pub avg: Duration,
    pub p50: Duration,
    pub p99: Duration,
    pub p999: Duration,
}

/// One row of the streaming progress table. Mirrors the columns of
/// `crates/ledger/src/bin/load.rs`'s output: per-1-second TPS, TPC,
/// latency percentiles, in-flight count.
#[derive(Clone, Copy, Debug)]
pub struct PerSecondStats {
    /// 1-based second index since run start.
    pub second: u32,
    /// `cluster_commit` advanced by this many ops in the window.
    pub tps: f64,
    /// `cluster_commit` watermark at the end of the window.
    pub total_committed: u64,
    /// Latency distribution restricted to submits that *returned*
    /// during this window.
    pub p50_latency: Option<Duration>,
    pub p99_latency: Option<Duration>,
    pub latency_samples: u64,
    /// `submitted_count - committed` at window close.
    pub in_flight: u64,
}

/// Per-node lag relative to the cluster-wide `cluster_commit`
/// watermark. `max_lag` is the worst observed during the run;
/// `final_lag` is the value at the last sample.
#[derive(Clone, Copy, Debug)]
pub struct NodeLagStats {
    pub node_idx: usize,
    pub is_leader_at_end: bool,
    pub max_lag: u64,
    pub final_lag: u64,
}

impl Snapshot {
    /// Throughput aggregates from `cluster_commit` movement. Returns
    /// zeros when fewer than two samples are present.
    pub fn throughput_stats(&self) -> ThroughputStats {
        if self.samples.len() < 2 {
            return ThroughputStats {
                samples: self.samples.len(),
                ..Default::default()
            };
        }

        let cc = |s: &Sample| {
            s.per_node
                .iter()
                .map(|n| n.cluster_commit)
                .max()
                .unwrap_or(0)
        };

        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for w in self.samples.windows(2) {
            let prev = &w[0];
            let curr = &w[1];
            let delta_cc = cc(curr).saturating_sub(cc(prev)) as f64;
            let delta_t = (curr.at - prev.at).as_secs_f64();
            if delta_t > 0.0 {
                let rate = delta_cc / delta_t;
                if rate < min {
                    min = rate;
                }
                if rate > max {
                    max = rate;
                }
            }
        }
        if !min.is_finite() {
            min = 0.0;
        }
        if !max.is_finite() {
            max = 0.0;
        }

        let first = self.samples.first().unwrap();
        let last = self.samples.last().unwrap();
        let ops_total = cc(last).saturating_sub(cc(first));
        let duration = last.at.saturating_sub(first.at);
        let avg = if duration.as_secs_f64() > 0.0 {
            ops_total as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        ThroughputStats {
            samples: self.samples.len(),
            ops_total,
            duration,
            avg_ops_per_sec: avg,
            min_ops_per_sec: min,
            max_ops_per_sec: max,
        }
    }

    /// Per-call latency stats. `None` when no waiting submits ran.
    pub fn latency_stats(&self) -> Option<LatencyStats> {
        if self.submit_latencies.is_empty() {
            return None;
        }
        let mut sorted: Vec<Duration> =
            self.submit_latencies.iter().map(|p| p.latency).collect();
        sorted.sort();
        let n = sorted.len();
        let total: Duration = sorted.iter().sum();
        let avg = if n > 0 {
            total / n as u32
        } else {
            Duration::ZERO
        };
        let p_index = |q: usize| -> usize { ((n * q) / 100).min(n - 1) };
        Some(LatencyStats {
            samples: n,
            min: sorted[0],
            max: sorted[n - 1],
            avg,
            p50: sorted[p_index(50)],
            p99: sorted[p_index(99)],
            p999: sorted[((n * 999) / 1000).min(n - 1)],
        })
    }

    /// Bucket samples + latencies into 1-second windows for the live
    /// progress table. One row per elapsed second. Returns an empty
    /// vec for runs shorter than a second.
    pub fn per_second_stats(&self) -> Vec<PerSecondStats> {
        let mut out = Vec::new();
        if self.samples.is_empty() {
            return out;
        }

        let cc = |s: &Sample| {
            s.per_node
                .iter()
                .map(|n| n.cluster_commit)
                .max()
                .unwrap_or(0)
        };

        let total_secs = self.samples.last().unwrap().at.as_secs();
        let mut prev_committed: u64 = self.samples.first().map(cc).unwrap_or(0);

        for sec in 1..=total_secs {
            let window_start = Duration::from_secs(sec - 1);
            let window_end = Duration::from_secs(sec);

            // Last cluster_commit watermark observed in this window.
            // Falls back to the previous tick's value if no sample
            // happened to land in this second (e.g. cluster paused).
            let committed = self
                .samples
                .iter()
                .filter(|s| s.at >= window_start && s.at < window_end)
                .last()
                .map(cc)
                .unwrap_or(prev_committed);

            let tps = committed.saturating_sub(prev_committed) as f64;

            // Latency distribution within this 1-sec window.
            let mut window_latencies: Vec<Duration> = self
                .submit_latencies
                .iter()
                .filter(|p| p.at >= window_start && p.at < window_end)
                .map(|p| p.latency)
                .collect();
            window_latencies.sort();

            let (p50, p99) = if window_latencies.is_empty() {
                (None, None)
            } else {
                let n = window_latencies.len();
                let p_idx = |q: usize| ((n * q) / 100).min(n - 1);
                (
                    Some(window_latencies[p_idx(50)]),
                    Some(window_latencies[p_idx(99)]),
                )
            };

            // Submitted by end of this window — count of latency
            // points stamped at < window_end.
            let submitted_by_end = self
                .submit_latencies
                .iter()
                .filter(|p| p.at < window_end)
                .count() as u64;
            let in_flight = submitted_by_end.saturating_sub(committed);

            out.push(PerSecondStats {
                second: sec as u32,
                tps,
                total_committed: committed,
                p50_latency: p50,
                p99_latency: p99,
                in_flight,
                latency_samples: window_latencies.len() as u64,
            });

            prev_committed = committed;
        }
        out
    }

    /// One entry per node observed in any sample, with worst-seen
    /// and final lag relative to `cluster_commit`.
    pub fn node_lag_stats(&self) -> Vec<NodeLagStats> {
        use std::collections::BTreeMap;

        let cc = |s: &Sample| {
            s.per_node
                .iter()
                .map(|n| n.cluster_commit)
                .max()
                .unwrap_or(0)
        };

        let mut max_lag: BTreeMap<usize, u64> = BTreeMap::new();
        for s in &self.samples {
            let watermark = cc(s);
            for n in &s.per_node {
                let lag = watermark.saturating_sub(n.commit);
                let entry = max_lag.entry(n.node_idx).or_insert(0);
                if lag > *entry {
                    *entry = lag;
                }
            }
        }

        let final_sample = self.samples.last();
        max_lag
            .into_iter()
            .map(|(idx, max)| {
                let (final_lag, is_leader_at_end) = final_sample
                    .map(|s| {
                        let watermark = cc(s);
                        let entry = s.per_node.iter().find(|n| n.node_idx == idx);
                        match entry {
                            Some(n) => (watermark.saturating_sub(n.commit), n.is_leader),
                            None => (0, false),
                        }
                    })
                    .unwrap_or((0, false));
                NodeLagStats {
                    node_idx: idx,
                    is_leader_at_end,
                    max_lag: max,
                    final_lag,
                }
            })
            .collect()
    }

    /// Final cluster-wide commit watermark. Useful for the report's
    /// "ops total" line.
    pub fn final_cluster_commit(&self) -> u64 {
        self.samples
            .last()
            .map(|s| {
                s.per_node
                    .iter()
                    .map(|n| n.cluster_commit)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }
}

// ============================================================
// Background pipeline poller
// ============================================================

/// Spawn a tokio task that snapshots every node every `interval` and
/// pushes the result into `collector`. Returns a handle the runner
/// aborts when the scenario completes.
pub fn spawn_poller(
    client: ClusterClient,
    collector: Arc<MetricsCollector>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if let Some(sample) = probe_once(&client, collector.start()).await {
                collector.push_sample(sample);
            }
        }
    })
}

/// One snapshot pass across every node. Returns `None` if the call
/// failed everywhere — partial snapshots are recorded as-is so the
/// CLI can see when nodes drop out.
async fn probe_once(client: &ClusterClient, start: Instant) -> Option<Sample> {
    let n = client.node_count();
    let mut per_node = Vec::with_capacity(n);
    let mut leader_idx: Option<usize> = None;
    for i in 0..n {
        match client.node(i).get_pipeline_index().await {
            Ok(pi) => {
                if pi.is_leader && leader_idx.is_none() {
                    leader_idx = Some(i);
                }
                per_node.push(NodePipelineSnap {
                    node_idx: i,
                    compute: pi.compute,
                    commit: pi.commit,
                    snapshot: pi.snapshot,
                    cluster_commit: pi.cluster_commit,
                    is_leader: pi.is_leader,
                });
            }
            Err(_) => {
                // Skip this node for the sample. Sample is still
                // useful — the missing entry tells the CLI that
                // node was unreachable at this tick.
            }
        }
    }
    if per_node.is_empty() {
        return None;
    }
    Some(Sample {
        at: start.elapsed(),
        leader_idx,
        per_node,
    })
}
