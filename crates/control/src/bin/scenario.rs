//! `roda-scenario` — command-line driver for the scenario catalogue.
//!
//! Provisions a real cluster via [`ProcessProvisioner`], drives a
//! scenario through [`ScenarioRunner`], and prints results in a
//! plain-text format suitable for logs and CI output.
//!
//! Examples:
//!
//! ```text
//! roda-scenario list
//! roda-scenario list --group e2e
//! roda-scenario run single_deposit_committed
//! roda-scenario run-all
//! roda-scenario run-all --group load
//! roda-scenario --server-bin ./target/release/roda-server run-all
//! ```
//!
//! Exit codes: `0` on success, `1` on any scenario failure or setup
//! error. The `roda-server` binary is resolved relative to this
//! binary's location by default; override with `--server-bin` or the
//! `RODA_SERVER_BIN` environment variable.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use control::provisioner::process::ProcessProvisioner;
use control::runner::{
    CLUSTER_COMMIT_LEVEL, MetricsCollector, ProbeLevelStats, ProvisionConfig, Sample,
    ScenarioRunner, Snapshot,
};
use control::scenario::Scenario;
use control::scenarios;
use proto::control::ClusterConfig;

#[derive(Parser)]
#[command(
    name = "roda-scenario",
    about = "Run roda-ledger scenario tests against a fresh cluster.",
    long_about = None,
)]
struct Cli {
    /// Path to the `roda-server` binary. Defaults to a sibling of
    /// this binary; override here or via `RODA_SERVER_BIN`.
    #[arg(long, env = "RODA_SERVER_BIN", global = true)]
    server_bin: Option<PathBuf>,

    /// Number of nodes to provision per scenario run. Override only
    /// when a scenario explicitly needs a different cluster shape.
    #[arg(long, default_value_t = 3, global = true)]
    nodes: u32,

    /// Stream `roda-server` stdout/stderr through this process's
    /// terminal. Off by default to keep scenario output clean; flip
    /// on when debugging cluster-side issues.
    #[arg(long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List scenarios in the catalogue.
    List {
        /// Restrict to a single group (e2e or load).
        #[arg(long, value_enum)]
        group: Option<Group>,
    },
    /// Run one named scenario.
    Run { name: String },
    /// Run every scenario, optionally restricted to a single group.
    RunAll {
        #[arg(long, value_enum)]
        group: Option<Group>,
        /// Print the full metrics report for each scenario instead of
        /// the one-line summary. Auto-enabled for `--group load`.
        #[arg(long)]
        report: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Group {
    E2e,
    Load,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    configure_logging();
    match cli.cmd {
        Command::List { group } => {
            list_scenarios(group);
            ExitCode::SUCCESS
        }
        Command::Run { ref name } => {
            let bin = match resolve_server_bin(cli.server_bin.as_deref()) {
                Ok(p) => p,
                Err(e) => return fail(&e),
            };
            run_one(&bin, name, cli.nodes, cli.verbose).await
        }
        Command::RunAll { group, report } => {
            let bin = match resolve_server_bin(cli.server_bin.as_deref()) {
                Ok(p) => p,
                Err(e) => return fail(&e),
            };
            // Load runs are about measurements — always print the
            // full report there, even if the user didn't pass the
            // flag.
            let render_report = report || matches!(group, Some(Group::Load));
            run_many(&bin, group, cli.nodes, cli.verbose, render_report).await
        }
    }
}

// ============================================================
// Subcommand: list
// ============================================================

fn list_scenarios(group: Option<Group>) {
    let mut sections: Vec<(&'static str, Vec<Scenario>)> = Vec::new();
    if matches!(group, None | Some(Group::E2e)) {
        sections.push(("e2e", scenarios::e2e::all()));
    }
    if matches!(group, None | Some(Group::Load)) {
        sections.push(("load", scenarios::load::all()));
    }

    for (i, (label, scenarios)) in sections.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{label}:");
        for s in scenarios {
            println!("  {} ({} steps)", s.name, s.steps.len());
            if !s.description.is_empty() {
                println!("    {}", s.description);
            }
        }
    }
}

// ============================================================
// Subcommand: run
// ============================================================

async fn run_one(server_bin: &Path, name: &str, nodes: u32, verbose: bool) -> ExitCode {
    let scenario = match find_scenario(name) {
        Some(s) => s,
        None => {
            return fail(&format!(
                "scenario `{name}` not found. List available with `roda-scenario list`."
            ));
        }
    };

    let is_load = is_load_scenario(name);

    println!("=== {} ===", scenario.name);
    if !scenario.description.is_empty() {
        println!("  description: {}", scenario.description);
    }
    println!("  steps:       {}", scenario.steps.len());
    println!("  nodes:       {nodes}");
    println!();

    let outcome = run_scenario(server_bin, &scenario, nodes, verbose, is_load).await;
    print_outcome_block(&outcome);
    println!();
    if is_load {
        print_summary_box(&outcome);
        println!();
        print_node_lag_block(&outcome.metrics);
        println!();
        print_latency_probe_block(&outcome.metrics);
    } else {
        print_report_blocks(&outcome.metrics);
    }
    match outcome.passed {
        true => ExitCode::SUCCESS,
        false => ExitCode::FAILURE,
    }
}

// ============================================================
// Subcommand: run-all
// ============================================================

async fn run_many(
    server_bin: &Path,
    group: Option<Group>,
    nodes: u32,
    verbose: bool,
    render_report: bool,
) -> ExitCode {
    let scenarios = collect(group);
    if scenarios.is_empty() {
        return fail("no scenarios match the requested group");
    }

    let total = scenarios.len();
    println!("=== running {total} scenarios ({nodes} nodes/cluster) ===");
    println!();

    let label_width: usize = 70;
    let mut results: Vec<(String, Outcome)> = Vec::with_capacity(total);
    let mut total_elapsed = Duration::ZERO;

    for (i, scenario) in scenarios.iter().enumerate() {
        let is_load = is_load_scenario(&scenario.name);

        if is_load {
            // Streaming load output — print header, run live table, then
            // summary block. Doesn't fit on one line, so we break the
            // dot-padded format used for terse runs.
            println!("=== [{}/{total}] {} ===", i + 1, scenario.name);
            let outcome = run_scenario(server_bin, scenario, nodes, verbose, true).await;
            total_elapsed += outcome.elapsed;
            print_outcome_block(&outcome);
            println!();
            print_summary_box(&outcome);
            println!();
            print_node_lag_block(&outcome.metrics);
            println!();
            print_latency_probe_block(&outcome.metrics);
            println!();
            results.push((scenario.name.clone(), outcome));
            continue;
        }

        let label = format!("[{}/{total}] {}", i + 1, scenario.name);
        let dots = ".".repeat(label_width.saturating_sub(label.chars().count() + 1));
        print!("{label} {dots} ");
        io::stdout().flush().ok();

        let outcome = run_scenario(server_bin, scenario, nodes, verbose, false).await;
        total_elapsed += outcome.elapsed;
        if outcome.passed {
            println!("PASSED ({:.2}s)", outcome.elapsed.as_secs_f64());
        } else {
            println!("FAILED ({:.2}s)", outcome.elapsed.as_secs_f64());
            if let Some(err) = &outcome.error {
                println!("       {err}");
            }
        }
        if render_report {
            println!();
            print_report_blocks(&outcome.metrics);
            println!();
        }
        results.push((scenario.name.clone(), outcome));
    }

    let passed = results.iter().filter(|(_, o)| o.passed).count();
    let failed = results.len() - passed;

    println!();
    println!("=== summary ===");
    println!("  total:    {}", results.len());
    println!("  passed:   {passed}");
    println!("  failed:   {failed}");
    println!("  duration: {:.2}s", total_elapsed.as_secs_f64());

    if failed > 0 {
        println!();
        println!("failures:");
        for (name, outcome) in &results {
            if !outcome.passed {
                let err = outcome.error.as_deref().unwrap_or("(no error message)");
                println!("  - {name}: {err}");
            }
        }
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

// ============================================================
// Core runner
// ============================================================

struct Outcome {
    passed: bool,
    elapsed: Duration,
    error: Option<String>,
    metrics: Snapshot,
}

async fn run_scenario(
    server_bin: &Path,
    scenario: &Scenario,
    nodes: u32,
    verbose: bool,
    streaming: bool,
) -> Outcome {
    // Load scenarios spin (`low_latency`) so the transactor never
    // idle-sleeps under bursty load; e2e uses `balanced` (low idle CPU).
    // Both use the requested node count. `RODA_WAIT_STRATEGY` overrides
    // the default so wait strategies can be A/B'd without a rebuild.
    let default_ws = "balanced";
    let wait_strategy =
        std::env::var("RODA_WAIT_STRATEGY").unwrap_or_else(|_| default_ws.to_string());
    let provisioner = Arc::new(
        ProcessProvisioner::new(server_bin.to_path_buf())
            .quiet(!verbose)
            .with_wait_strategy(wait_strategy),
    );
    let runner = ScenarioRunner::new(provisioner);
    let config = default_config(nodes);

    // The latency probe runs inside the runner (a 1ms-paced side task over the
    // execute window), so its samples land mid-load. Results arrive in
    // `report.metrics`.
    let metrics = Arc::new(MetricsCollector::new());
    let ticker = if streaming {
        print_table_header();
        Some(spawn_progress_ticker(metrics.clone()))
    } else {
        None
    };

    let report = runner.run(scenario, &config, metrics).await;

    if let Some(handle) = ticker {
        handle.abort();
        print_table_footer();
    }

    Outcome {
        passed: report.result.is_ok(),
        elapsed: report.elapsed,
        error: report.result.err().map(|e| e.to_string()),
        metrics: report.metrics,
    }
}

fn print_outcome_block(outcome: &Outcome) {
    if outcome.passed {
        println!("result:  PASSED");
    } else {
        println!("result:  FAILED");
    }
    println!("elapsed: {:.2}s", outcome.elapsed.as_secs_f64());
    if let Some(err) = &outcome.error {
        println!("error:   {err}");
    }
}

// ============================================================
// Metrics report rendering
// ============================================================

fn print_report_blocks(metrics: &Snapshot) {
    let throughput = metrics.throughput_stats();
    println!("throughput (cluster_commit):");
    if throughput.samples < 2 {
        println!("  (no samples — scenario was too short to measure)");
    } else {
        println!(
            "  ops total:    {} (cluster_commit advanced over {:.2}s)",
            throughput.ops_total,
            throughput.duration.as_secs_f64()
        );
        println!("  avg ops/s:    {:>8.1}", throughput.avg_ops_per_sec);
        println!("  min ops/s:    {:>8.1}", throughput.min_ops_per_sec);
        println!("  max ops/s:    {:>8.1}", throughput.max_ops_per_sec);
        println!("  samples:      {}", throughput.samples);
    }

    println!();
    print_latency_probe_block(metrics);

    let lags = metrics.node_lag_stats();
    println!();
    println!("per-node lag (vs cluster_commit):");
    if lags.is_empty() {
        println!("  (no samples)");
    } else {
        for l in &lags {
            let role = if l.is_leader_at_end { " [leader]" } else { "" };
            println!(
                "  node[{}]: max {} ops behind, final {} ops behind{}",
                l.node_idx, l.max_lag, l.final_lag, role
            );
        }
    }
}

// ============================================================
// Helpers
// ============================================================

fn find_scenario(name: &str) -> Option<Scenario> {
    scenarios::list().into_iter().find(|s| s.name == name)
}

fn collect(group: Option<Group>) -> Vec<Scenario> {
    match group {
        Some(Group::E2e) => scenarios::e2e::all(),
        Some(Group::Load) => scenarios::load::all(),
        None => scenarios::list(),
    }
}

fn default_config(node_count: u32) -> ProvisionConfig {
    ProvisionConfig {
        node_count,
        cluster: ClusterConfig {
            max_accounts: 10_000,
            queue_size: 1024,
            transaction_count_per_segment: 1_000_000,
            snapshot_frequency: 2,
            append_entries_max_bytes: 4 * 1024 * 1024,
        },
    }
}

fn resolve_server_bin(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(p) = explicit {
        if !p.exists() {
            return Err(format!("server-bin {} does not exist", p.display()));
        }
        return Ok(p.to_path_buf());
    }

    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let dir = exe
        .parent()
        .ok_or_else(|| "current_exe has no parent directory".to_string())?;
    let candidate = dir.join(if cfg!(windows) {
        "roda-server.exe"
    } else {
        "roda-server"
    });
    if !candidate.exists() {
        return Err(format!(
            "roda-server binary not found at {}.\n\
             Build it with: cargo build -p cluster --bin roda-server\n\
             Or set --server-bin / RODA_SERVER_BIN to override.",
            candidate.display()
        ));
    }
    Ok(candidate)
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("error: {msg}");
    ExitCode::FAILURE
}

fn is_load_scenario(name: &str) -> bool {
    scenarios::load::all().iter().any(|s| s.name == name)
}

// ============================================================
// Live progress ticker — prints one table row per second while a
// load scenario runs. Mirrors the layout of
// `crates/ledger/src/bin/load.rs`.
// ============================================================

fn spawn_progress_ticker(metrics: Arc<MetricsCollector>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sec: u32 = 0;
        let mut last_committed: u64 = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sec += 1;
            let snap = metrics.snapshot();
            let row = compute_progress_row(&snap, sec, last_committed);
            print_table_row(&row);
            last_committed = row.total_committed;
        }
    })
}

struct ProgressRow {
    second: u32,
    tps: u64,
    total_committed: u64,
    p50: Duration,
    p99: Duration,
}

fn compute_progress_row(snap: &Snapshot, sec: u32, prev_committed: u64) -> ProgressRow {
    let cc = |s: &Sample| {
        s.per_node
            .iter()
            .map(|n| n.cluster_commit)
            .max()
            .unwrap_or(0)
    };
    let total_committed = snap.samples.last().map(cc).unwrap_or(prev_committed);

    // Live latency columns = cluster_commit probe samples taken this second.
    let window_start = Duration::from_secs((sec - 1) as u64);
    let window_end = Duration::from_secs(sec as u64);
    let (p50, p99) = snap.probe_window(CLUSTER_COMMIT_LEVEL, window_start, window_end);

    ProgressRow {
        second: sec,
        tps: total_committed.saturating_sub(prev_committed),
        total_committed,
        p50: p50.unwrap_or(Duration::ZERO),
        p99: p99.unwrap_or(Duration::ZERO),
    }
}

fn print_table_header() {
    println!();
    println!("  +-----+--------+------------+------------+----------+----------+");
    println!(
        "  | {:>3} | {:>6} | {:>10} | {:>10} | {:>8} | {:>8} |",
        "#", "time", "TPS", "TPC", "cc P50", "cc P99"
    );
    println!("  +-----+--------+------------+------------+----------+----------+");
}

fn print_table_footer() {
    println!("  +-----+--------+------------+------------+----------+----------+");
}

fn print_table_row(row: &ProgressRow) {
    println!(
        "  | {:>3} | {:>5}s | {:>10} | {:>10} | {:>8} | {:>8} |",
        row.second,
        row.second,
        row.tps,
        row.total_committed,
        fmt_dur(row.p50),
        fmt_dur(row.p99),
    );
}

fn fmt_dur(d: Duration) -> String {
    let ns = d.as_nanos() as u64;
    if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

// ============================================================
// Summary box — printed after a load scenario finishes.
// Layout mirrors `crates/ledger/src/bin/load.rs`.
// ============================================================

fn print_summary_box(outcome: &Outcome) {
    let tput = outcome.metrics.throughput_stats();
    // Report the active-commit window, not the full scenario wall-clock.
    // `tput.duration` runs from the last pre-movement metrics sample to
    // the first post-movement sample (i.e., it covers exactly the
    // window where `cluster_commit` advanced from its starting value
    // to its final value), and `tput.avg_ops_per_sec` is the rate over
    // that window — the cluster's actual commit throughput, not
    // committed-ops divided by total elapsed (which would be diluted
    // by provisioner setup, runner startup, and the post-burst
    // OnSnapshot drain wait).
    let active_secs = tput.duration.as_secs_f64();

    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║              LOAD TEST SUMMARY               ║");
    println!("  ╠══════════════════════════════════════════════╣");
    println!(
        "  ║  Wall clock    : {:>10.2}s                 ║",
        outcome.elapsed.as_secs_f64()
    );
    println!(
        "  ║  Commit window : {:>10.2}s                 ║",
        active_secs
    );
    println!(
        "  ║  Committed     : {:>10}                  ║",
        tput.ops_total
    );
    println!(
        "  ║  Avg TPS       : {:>10.0}                  ║",
        tput.avg_ops_per_sec
    );
    println!(
        "  ║  Peak TPS      : {:>10.0}                  ║",
        tput.max_ops_per_sec
    );
    println!("  ╚══════════════════════════════════════════════╝");
}

fn print_node_lag_block(metrics: &Snapshot) {
    let lags = metrics.node_lag_stats();
    if lags.is_empty() {
        return;
    }
    println!("per-node lag (vs cluster_commit):");
    for l in &lags {
        let role = if l.is_leader_at_end { " [leader]" } else { "" };
        println!(
            "  node[{}]: max {} ops behind, final {} ops behind{}",
            l.node_idx, l.max_lag, l.final_lag, role
        );
    }
}

/// Per-wait-level latency measured on the leader, probed server-side at
/// 1ms cadence during the load (see `runner::metrics`). One row per stage.
fn print_latency_probe_block(metrics: &Snapshot) {
    let stats: Vec<ProbeLevelStats> = metrics.probe_level_stats();
    println!("latency probe (leader, server-side, per wait level):");
    if stats.iter().all(|s| s.samples == 0) {
        println!("  (no samples — build roda-server with `--features latency-probe`)");
        return;
    }

    let border = "  +----------------+---------+----------+----------+----------+----------+";
    println!("{border}");
    println!(
        "  | {:<14} | {:>7} | {:>8} | {:>8} | {:>8} | {:>8} |",
        "stage", "samples", "p50", "p99", "p999", "max"
    );
    println!("{border}");
    for s in &stats {
        println!(
            "  | {:<14} | {:>7} | {:>8} | {:>8} | {:>8} | {:>8} |",
            s.name,
            s.samples,
            fmt_dur(s.p50),
            fmt_dur(s.p99),
            fmt_dur(s.p999),
            fmt_dur(s.max),
        );
    }
    println!("{border}");
}

/// Default the in-process `spdlog-rs` output (used by `client` /
/// `cluster`) to `Debug`. The scenario harness is the e2e entry
/// point — debug-level logs are the right baseline for diagnosing
/// failed runs, and the `--verbose` flag (which forwards the
/// spawned servers' stdout/stderr) controls *child* output, not
/// this process's. Override via `RODA_SCENARIO_LOG_LEVEL` when a
/// tighter or looser filter is needed.
fn configure_logging() {
    let level = std::env::var("RODA_SCENARIO_LOG_LEVEL")
        .ok()
        .and_then(|s| parse_log_level(&s))
        .unwrap_or(spdlog::Level::Warn);
    spdlog::default_logger().set_level_filter(spdlog::LevelFilter::MoreSevereEqual(level));
}

fn parse_log_level(s: &str) -> Option<spdlog::Level> {
    match s.to_ascii_lowercase().as_str() {
        "trace" => Some(spdlog::Level::Trace),
        "debug" => Some(spdlog::Level::Debug),
        "info" => Some(spdlog::Level::Info),
        "warn" | "warning" => Some(spdlog::Level::Warn),
        "error" => Some(spdlog::Level::Error),
        "critical" | "crit" => Some(spdlog::Level::Critical),
        _ => None,
    }
}
