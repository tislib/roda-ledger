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
//! 2. **Latency probes** — a side task calls the leader's
//!    `LatencyProbe.Probe` RPC once per millisecond (one sample per
//!    wait level) and records the server-measured latency here. The
//!    measurement happens *inside* the node, so client→server RTT
//!    never pollutes it. Requires `roda-server` built with the
//!    `latency-probe` feature; absent it, no samples are recorded.
//!
//! Aggregation (per-level p50 / p99 / p999 / max, lag-per-node) is the
//! caller's job. Keeping the collector raw keeps it cheap and gives
//! the CLI flexibility to render whatever shape it wants.

use std::sync::Arc;
use std::time::{Duration, Instant};

use client::ClusterClient;
use parking_lot::Mutex;
use proto::latency::latency_probe_client::LatencyProbeClient;
use proto::latency::{ProbeRequest, WaitLevel};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

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

/// Wait levels probed each tick, shallow → deep. The index into this
/// array is the `level` stamped on every [`ProbeSample`]; the name
/// labels the report row. `CLUSTER_COMMIT_LEVEL` indexes the last one.
pub const PROBE_LEVELS: [(&str, WaitLevel); 4] = [
    ("compute", WaitLevel::Computed),
    ("commit", WaitLevel::Committed),
    ("snapshot", WaitLevel::Snapshot),
    ("cluster_commit", WaitLevel::ClusterCommit),
];

/// Index of `cluster_commit` in [`PROBE_LEVELS`] — the full end-to-end
/// level the live progress table reports.
pub const CLUSTER_COMMIT_LEVEL: usize = 3;

/// One server-measured latency sample for a wait level, stamped with
/// when it was taken (relative to the collector's start) so the CLI can
/// bucket samples into per-second windows for the streaming table.
#[derive(Clone, Copy, Debug)]
pub struct ProbeSample {
    pub at: Duration,
    pub level: usize,
    pub latency_ns: u64,
}

#[derive(Default)]
struct CollectorInner {
    samples: Vec<Sample>,
    probes: Vec<ProbeSample>,
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

    /// Record one server-measured latency sample for `level`, stamped
    /// with `now - collector.start`. Called from the latency-probe task.
    pub fn record_probe(&self, level: usize, latency_ns: u64) {
        let at = self.start.elapsed();
        self.inner.lock().probes.push(ProbeSample {
            at,
            level,
            latency_ns,
        });
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
            probes: inner.probes.clone(),
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
    pub probes: Vec<ProbeSample>,
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

/// Per-wait-level latency aggregate over a run's probe samples. One per
/// entry in [`PROBE_LEVELS`].
#[derive(Clone, Debug)]
pub struct ProbeLevelStats {
    pub name: &'static str,
    pub samples: usize,
    pub p50: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
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

        // Trim the measurement window to the active commit phase: from
        // the last pre-movement sample (cc == min_cc) to the first
        // post-movement sample (cc == max_cc). Without this trim the
        // average spans warmup (provisioner is still bringing the
        // cluster up, runner hasn't fired its first RPC yet, so
        // cluster_commit isn't moving) and drain (the load step is
        // done, the report is just waiting on the OnSnapshot wait
        // step). Both phases have zero ops/sec and would drag
        // `avg_ops_per_sec` far below the cluster's real commit rate.
        let min_cc = cc(&self.samples[0]);
        let max_cc = cc(self.samples.last().unwrap());

        if max_cc <= min_cc {
            // Cluster never advanced past the starting commit index —
            // either no submits landed or the run was too short.
            return ThroughputStats {
                samples: self.samples.len(),
                ops_total: 0,
                duration: Duration::ZERO,
                avg_ops_per_sec: 0.0,
                min_ops_per_sec: 0.0,
                max_ops_per_sec: 0.0,
            };
        }

        // `start_idx` is the last sample whose `cc` was still at the
        // baseline — i.e., the timestamp of the first non-zero commit
        // delta is between `start_idx` and `start_idx + 1`. Using
        // `start_idx`'s timestamp as the time origin is conservative
        // (slightly longer duration, slightly lower TPS) but is
        // honest: that's the latest moment we observed zero progress.
        let start_idx = self
            .samples
            .iter()
            .rposition(|s| cc(s) == min_cc)
            .unwrap_or(0);
        // `end_idx` is the first sample where `cc` had already
        // reached its final value — symmetric reasoning.
        let end_idx = self
            .samples
            .iter()
            .position(|s| cc(s) == max_cc)
            .unwrap_or(self.samples.len() - 1);
        let active = if end_idx > start_idx {
            &self.samples[start_idx..=end_idx]
        } else {
            &self.samples[..]
        };

        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for w in active.windows(2) {
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

        let ops_total = max_cc.saturating_sub(min_cc);
        let duration = active
            .last()
            .unwrap()
            .at
            .saturating_sub(active.first().unwrap().at);
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

    /// Per-wait-level latency aggregates over every probe sample.
    pub fn probe_level_stats(&self) -> Vec<ProbeLevelStats> {
        PROBE_LEVELS
            .iter()
            .enumerate()
            .map(|(level, (name, _))| {
                let mut v: Vec<u64> = self
                    .probes
                    .iter()
                    .filter(|p| p.level == level)
                    .map(|p| p.latency_ns)
                    .collect();
                v.sort_unstable();
                ProbeLevelStats {
                    name,
                    samples: v.len(),
                    p50: quantile_ns(&v, 50, 100),
                    p99: quantile_ns(&v, 99, 100),
                    p999: quantile_ns(&v, 999, 1000),
                    max: Duration::from_nanos(v.last().copied().unwrap_or(0)),
                }
            })
            .collect()
    }

    /// `(p50, p99)` of `level`'s probe samples in `[start, end)`, for the
    /// live progress table. `None`s when the window saw no samples.
    pub fn probe_window(
        &self,
        level: usize,
        start: Duration,
        end: Duration,
    ) -> (Option<Duration>, Option<Duration>) {
        let mut v: Vec<u64> = self
            .probes
            .iter()
            .filter(|p| p.level == level && p.at >= start && p.at < end)
            .map(|p| p.latency_ns)
            .collect();
        if v.is_empty() {
            return (None, None);
        }
        v.sort_unstable();
        (
            Some(quantile_ns(&v, 50, 100)),
            Some(quantile_ns(&v, 99, 100)),
        )
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

// ============================================================
// Latency probe — server-side, leader-only, 1ms cadence
// ============================================================

/// Spawn a task that calls the leader's `LatencyProbe.Probe` RPC once
/// per millisecond — one sample per wait level — recording each into
/// `collector`. Runs until the runner aborts it. Returns early (no
/// samples) if no leader appears or the server lacks the `latency-probe`
/// feature. The 1ms pacing keeps probing from skewing the load it
/// measures.
pub fn spawn_latency_probe(
    client: ClusterClient,
    collector: Arc<MetricsCollector>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(mut probe) = wait_for_leader_probe(&client, Duration::from_secs(10)).await else {
            return;
        };
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            for (level, (_, wait_level)) in PROBE_LEVELS.iter().enumerate() {
                let req = ProbeRequest {
                    wait_level: *wait_level as i32,
                    probe_count: 1,
                    probe_interval_ms: 0,
                };
                match probe.probe(req).await {
                    Ok(resp) => {
                        for ns in resp.into_inner().latencies_ns {
                            collector.record_probe(level, ns);
                        }
                    }
                    // Service absent (server built without the feature) — give up.
                    Err(s) if s.code() == tonic::Code::Unimplemented => return,
                    // Leader moved / transient — re-resolve + reconnect.
                    Err(_) => match wait_for_leader_probe(&client, Duration::from_secs(5)).await {
                        Some(p) => {
                            probe = p;
                            break;
                        }
                        None => return,
                    },
                }
            }
        }
    })
}

/// Poll for a leader and connect a `LatencyProbe` client to its ledger
/// port (the probe service rides that port). `None` past `timeout`.
async fn wait_for_leader_probe(
    client: &ClusterClient,
    timeout: Duration,
) -> Option<LatencyProbeClient<Channel>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader) = resolve_leader(client).await {
            let url = probe_url(client.node(leader).url());
            if let Ok(c) = LatencyProbeClient::connect(url).await {
                return Some(c);
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Index of the node currently reporting `is_leader`, or `None`.
async fn resolve_leader(client: &ClusterClient) -> Option<usize> {
    for i in 0..client.node_count() {
        if let Ok(pi) = client.node(i).get_pipeline_index().await
            && pi.is_leader
        {
            return Some(i);
        }
    }
    None
}

/// Normalize a `NodeClient` URL to an `http://` endpoint tonic accepts.
fn probe_url(url: &str) -> String {
    if url.starts_with("http") {
        url.to_string()
    } else {
        format!("http://{url}")
    }
}

/// `((n * num) / den)`-th element of a pre-sorted ns slice, as a
/// Duration. Clamped to the last index; zero for an empty slice.
fn quantile_ns(sorted: &[u64], num: usize, den: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let n = sorted.len();
    Duration::from_nanos(sorted[((n * num) / den).min(n - 1)])
}
