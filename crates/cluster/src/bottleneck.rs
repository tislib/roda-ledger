//! Scheduler-starvation detector (feature `bottleneck-detector`).
//!
//! [`spawn`] puts a task on the *current* tokio runtime that times how long a
//! bare `yield_now()` takes to round-trip — i.e. how long the runtime's thread
//! was monopolised by other work before this task got scheduled again. On a
//! healthy thread that's tens of nanoseconds; a millisecond tail means some
//! task is hogging the thread (e.g. a no-`await` handler chewing a big batch),
//! which inflates everything else that shares the runtime — including the
//! latency probe. It isolates "is the thread starved?" from "is the pipeline
//! slow?".
//!
//! It busy-polls (no sleep — a sleep would be recorded as a fake gap and also
//! hits tokio's ~1ms timer granularity), so it pegs one core for the life of
//! the runtime. Enable it only while diagnosing.

use std::time::{Duration, Instant};

use roda_latency_tracker::latency_measurer::LatencyMeasurer;
use spdlog::warn;
use tokio::task::yield_now;

/// Spawn the detector on the current runtime. `label` tags the log line;
/// the yield round-trip distribution is reported every `report_every`.
pub fn spawn(label: &'static str, report_every: Duration) {
    tokio::spawn(async move {
        let mut measurer = LatencyMeasurer::new(1);
        let mut window = Instant::now();
        loop {
            let t0 = Instant::now();
            yield_now().await;
            measurer.measure(t0.elapsed());

            if window.elapsed() >= report_every {
                let s = measurer.get_stats();
                warn!(
                    "bottleneck[{label}]: yield round-trip over {:.1}s — samples={} p50={} p99={} p999={} p9999={} max={}",
                    window.elapsed().as_secs_f64(),
                    s.count,
                    fmt_ns(s.p50),
                    fmt_ns(s.p99),
                    fmt_ns(s.p999),
                    fmt_ns(s.p9999),
                    fmt_ns(s.max),
                );
                measurer = LatencyMeasurer::new(1);
                window = Instant::now();
            }
        }
    });
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}
