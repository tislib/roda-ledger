use crate::ledger::Ledger;
use crate::testing::stress::direct_workload_client::DirectWorkloadClient;
use crate::wallet::transaction::WalletTransaction;
use arc_swap::ArcSwap;
use roda_latency_tracker::latency_measurer::{LatencyMeasurer, LatencyStats as RodaLatencyStats};
use serde::Serialize;
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};

#[derive(Debug, Clone, Serialize, Default)]
pub struct LatencyStats {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub seq_id: u64,
}

impl From<RodaLatencyStats> for LatencyStats {
    fn from(stats: RodaLatencyStats) -> Self {
        Self {
            count: stats.count,
            min: stats.min,
            max: stats.max,
            mean: stats.mean,
            p50: stats.p50,
            p90: stats.p90,
            p99: stats.p99,
            p999: stats.p999,
            p9999: stats.p9999,
            seq_id: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunResult {
    pub scenario_name: String,
    pub duration: Duration,
    pub metrics_over_time: Vec<TimeMetric>,
    pub latencies: LatencyStats,
    pub tx_per_sec: TxStats,
    pub cpu_usage: f32,
    pub memory_usage: u64,
    pub latency_history: Vec<Duration>, // Every 1s average
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeMetric {
    pub timestamp: Duration,
    pub tx_per_sec: TxStats,
    pub mean_latency: Duration,
    pub cpu_usage: f32,
    pub memory_usage: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TxStats {
    pub compute: f64,
    pub commit: f64,
    pub snapshot: f64,
    pub tc: u64,
}

pub struct MdReporting {
    file: File,
}

impl MdReporting {
    pub fn new(scenario_name: &str) -> std::io::Result<Self> {
        let _ = create_dir_all("reporting");
        let path = format!("reporting/{}.md", scenario_name);
        let file = File::create(path)?;
        Ok(Self { file })
    }

    pub fn write_report(&mut self, result: &RunResult) {
        let mut output = String::new();
        output.push_str(&format!("# Scenario {} REPORT\n\n", result.scenario_name));

        output.push_str("## Summary\n");
        output.push_str(&format!("- **Total Duration:** {:?}\n", result.duration));
        output.push_str(&format!(
            "- **Total Transactions (TC):** {}\n",
            result.tx_per_sec.tc
        ));
        output.push_str(&format!(
            "- **Average TPS (Compute):** {:.2}\n",
            result.tx_per_sec.compute
        ));
        output.push_str(&format!(
            "- **Average TPS (Commit):**  {:.2}\n",
            result.tx_per_sec.commit
        ));
        output.push_str(&format!(
            "- **Average TPS (Snapshot):** {:.2}\n\n",
            result.tx_per_sec.snapshot
        ));

        output.push_str("## Latencies\n");
        output.push_str(&format!(
            "- **Mean:** {:?}\n",
            Duration::from_nanos(result.latencies.mean as u64)
        ));
        output.push_str(&format!(
            "- **P50:**  {:?}\n",
            Duration::from_nanos(result.latencies.p50)
        ));
        output.push_str(&format!(
            "- **P99:**  {:?}\n",
            Duration::from_nanos(result.latencies.p99)
        ));
        output.push_str(&format!(
            "- **P999:** {:?}\n",
            Duration::from_nanos(result.latencies.p999)
        ));
        output.push_str(&format!(
            "- **P9999:** {:?}\n",
            Duration::from_nanos(result.latencies.p9999)
        ));
        output.push_str(&format!("- **SeqId:** {}\n\n", result.latencies.seq_id));

        output.push_str("## System Usage (Current)\n");
        output.push_str(&format!("- **CPU Usage:**    {:.2}%\n", result.cpu_usage));
        output.push_str(&format!(
            "- **Memory Usage:** {:.2} MB\n\n",
            result.memory_usage as f64 / 1024.0 / 1024.0
        ));

        output.push_str("## Metrics over time (every 1s)\n\n");
        output.push_str(
            "| Time (s) | TC | Compute TPS | Mean Latency | CPU Usage (%) | Memory (MB) |\n",
        );
        output.push_str(
            "|----------|----|-------------|--------------|---------------|-------------|\n",
        );
        for m in &result.metrics_over_time {
            output.push_str(&format!(
                "| {:8.1} | {:8} | {:11.2} | {:12?} | {:13.2} | {:11.2} |\n",
                m.timestamp.as_secs_f64(),
                m.tx_per_sec.tc,
                m.tx_per_sec.compute,
                m.mean_latency,
                m.cpu_usage,
                m.memory_usage as f64 / 1024.0 / 1024.0
            ));
        }

        let _ = self.file.write_all(output.as_bytes());
        let _ = self.file.flush();
    }
}

pub struct JsonOutput;

impl JsonOutput {
    pub fn write(result: &RunResult) -> std::io::Result<()> {
        let _ = create_dir_all("reporting");
        let path = format!("reporting/{}.json", result.scenario_name);
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, result)?;
        Ok(())
    }
}

pub struct Reporter {
    scenario_name: String,
    duration: Duration,
    ledger: Arc<Ledger<WalletTransaction>>,
    metrics: Arc<WorkloadMetrics>,
    metrics_over_time: Vec<TimeMetric>,

    // TPS tracking
    last_computed: u64,
    last_committed: u64,
    last_snapshot: u64,
    last_tick: Instant,

    sys: System,
    pid: Pid,

    md_reporting: MdReporting,
    last_stdout_report: Instant,
}

impl Reporter {
    pub fn new(
        scenario_name: String,
        duration: Duration,
        ledger: Arc<Ledger<WalletTransaction>>,
    ) -> Self {
        let metrics = Arc::new(WorkloadMetrics::new(duration));
        let mut sys = System::new_all();
        sys.refresh_all();
        std::thread::sleep(Duration::from_millis(100));
        sys.refresh_all();
        let pid = Pid::from(std::process::id() as usize);
        let md_reporting =
            MdReporting::new(&scenario_name).expect("Failed to create markdown reporting");

        Self {
            scenario_name,
            duration,
            ledger,
            metrics,
            metrics_over_time: Vec::new(),
            last_computed: 0,
            last_committed: 0,
            last_snapshot: 0,
            last_tick: Instant::now(),
            sys,
            pid,
            md_reporting,
            last_stdout_report: Instant::now(),
        }
    }

    pub fn client(&self) -> DirectWorkloadClient {
        DirectWorkloadClient::new(self.ledger.clone())
    }

    pub fn metrics(&self) -> Arc<WorkloadMetrics> {
        self.metrics.clone()
    }

    pub fn run_loop(&mut self) {
        let start_time = self.metrics.start_time;
        while start_time.elapsed() < self.duration {
            std::thread::sleep(Duration::from_secs(1));
            self.collect_metrics();

            if self.last_stdout_report.elapsed() >= Duration::from_secs(10) {
                if let Some(m) = self.metrics_over_time.last() {
                    println!(
                        "Scenario {}: {:>4.1}s/{:>4.1}s | TC: {:>8} | TPS: {:>10.2} | Latency: {:>12?} | CPU: {:>6.2}% | Mem: {:>8.2}MB",
                        self.scenario_name,
                        m.timestamp.as_secs_f64(),
                        self.duration.as_secs_f64(),
                        m.tx_per_sec.tc,
                        m.tx_per_sec.compute,
                        m.mean_latency,
                        m.cpu_usage,
                        m.memory_usage as f64 / 1024.0 / 1024.0
                    );
                }
                self.last_stdout_report = Instant::now();
            }
        }
    }

    pub fn collect_metrics(&mut self) {
        self.sys.refresh_all();
        let process = self.sys.process(self.pid);
        let cpu_usage = process.map(|p| p.cpu_usage()).unwrap_or(0.0);
        let memory_usage = process.map(|p| p.memory()).unwrap_or(0);

        let current_computed = self.ledger.last_computed_id();
        let current_committed = self.ledger.last_committed_id();
        let current_snapshot = self.ledger.last_snapshot_id();

        let tick_duration = self.last_tick.elapsed().as_secs_f64();
        let tx_stats = TxStats {
            compute: (current_computed - self.last_computed) as f64 / tick_duration,
            commit: (current_committed - self.last_committed) as f64 / tick_duration,
            snapshot: (current_snapshot - self.last_snapshot) as f64 / tick_duration,
            tc: current_computed,
        };

        let mean_latency = self.metrics.get_stats().mean;
        let mean_latency_duration = Duration::from_nanos(mean_latency as u64);

        self.metrics_over_time.push(TimeMetric {
            timestamp: self.metrics.start_time.elapsed(),
            tx_per_sec: tx_stats,
            mean_latency: mean_latency_duration,
            cpu_usage,
            memory_usage,
        });

        self.last_computed = current_computed;
        self.last_committed = current_committed;
        self.last_snapshot = current_snapshot;
        self.last_tick = Instant::now();
    }

    fn get_current_result(&mut self) -> RunResult {
        let total_duration = self.metrics.start_time.elapsed();
        let latencies = self.metrics.get_stats();

        self.sys.refresh_all();
        let process = self.sys.process(self.pid);

        let tx_per_sec = TxStats {
            compute: self.ledger.last_computed_id() as f64 / total_duration.as_secs_f64(),
            commit: self.ledger.last_committed_id() as f64 / total_duration.as_secs_f64(),
            snapshot: self.ledger.last_snapshot_id() as f64 / total_duration.as_secs_f64(),
            tc: self.ledger.last_computed_id(),
        };

        let num_buckets = self.metrics.time_series_sum.len();
        let mut latency_history = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let sum = self.metrics.time_series_sum[i].load(Ordering::Relaxed);
            let count = self.metrics.time_series_count[i].load(Ordering::Relaxed);
            if count > 0 {
                latency_history.push(Duration::from_nanos(sum / count));
            } else {
                latency_history.push(Duration::from_nanos(0));
            }
        }

        RunResult {
            scenario_name: self.scenario_name.clone(),
            duration: total_duration,
            metrics_over_time: self.metrics_over_time.clone(),
            latencies,
            tx_per_sec,
            cpu_usage: process.map(|p| p.cpu_usage()).unwrap_or(0.0),
            memory_usage: process.map(|p| p.memory()).unwrap_or(0),
            latency_history,
        }
    }

    pub fn finish(mut self) -> RunResult {
        let result = self.get_current_result();
        self.md_reporting.write_report(&result);
        let _ = JsonOutput::write(&result);
        result
    }
}

pub struct WorkloadMetrics {
    pub start_time: Instant,
    pub time_series_sum: Vec<AtomicU64>,
    pub time_series_count: Vec<AtomicU64>,
    pub measurer: Mutex<LatencyMeasurer>,
    pub stats: ArcSwap<LatencyStats>,
    pub sample_size: u64,
    pub stepping: AtomicU64,
}

impl WorkloadMetrics {
    pub fn new(duration: Duration) -> Self {
        let num_buckets = (duration.as_secs() as usize + 10).max(1);
        let mut time_series_sum = Vec::with_capacity(num_buckets);
        let mut time_series_count = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            time_series_sum.push(AtomicU64::new(0));
            time_series_count.push(AtomicU64::new(0));
        }

        Self {
            start_time: Instant::now(),
            time_series_sum,
            time_series_count,
            measurer: Mutex::new(LatencyMeasurer::new(1)),
            stats: ArcSwap::new(Arc::new(LatencyStats::default())),
            sample_size: 1000,
            stepping: AtomicU64::new(0),
        }
    }

    pub fn record(&self, elapsed: Duration) {
        let stepping = self.stepping.fetch_add(1, Ordering::Relaxed) + 1;
        if !stepping.is_multiple_of(self.sample_size) {
            return;
        }

        let elapsed_ns = elapsed.as_nanos() as u64;
        let now_sec = self.start_time.elapsed().as_secs() as usize;

        // Time series
        if now_sec < self.time_series_sum.len() {
            self.time_series_sum[now_sec].fetch_add(elapsed_ns, Ordering::Relaxed);
            self.time_series_count[now_sec].fetch_add(1, Ordering::Relaxed);
        }

        // Global stats
        if let Ok(mut measurer) = self.measurer.lock() {
            measurer.measure(elapsed);
            let mut stats: LatencyStats = measurer.get_stats().into();
            stats.seq_id = stepping;
            self.stats.store(Arc::new(stats));
        }
    }

    pub fn get_stats(&self) -> LatencyStats {
        self.stats.load_full().as_ref().clone()
    }
}
