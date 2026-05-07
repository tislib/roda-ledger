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

#[derive(Default)]
struct CollectorInner {
    samples: Vec<Sample>,
    submit_latencies: Vec<Duration>,
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

    /// Push one observed submit latency. Only called for waiting
    /// submits; fire-and-forget is excluded since there's no
    /// observable end at the submit site.
    pub fn record_submit_latency(&self, latency: Duration) {
        self.inner.lock().submit_latencies.push(latency);
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
    pub submit_latencies: Vec<Duration>,
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
        let mut sorted = self.submit_latencies.clone();
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
        })
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
