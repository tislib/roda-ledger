//! Server-side latency probe (`latency-probe` feature).
//!
//! Measures *ambient* pipeline latency to a chosen wait level without
//! injecting traffic: capture the current sequencer head, then wait — via
//! the shared [`Waiter`] — until that stage reaches it. Repeated
//! `probe_count` times with an optional inter-probe delay. Samples where
//! the stage is already caught up at capture (nothing in flight) are
//! *excluded* — they aren't real waits and would otherwise peg the median
//! near zero. See `crates/proto/proto/latency.proto`.

use crate::ledger_slot::LedgerSlot;
use crate::waiter::Waiter;
use proto::latency::latency_probe_server::LatencyProbe;
use proto::latency::{ProbeRequest, ProbeResponse};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

pub struct LatencyProbeHandler {
    ledger: Arc<LedgerSlot>,
    waiter: Arc<Waiter>,
}

impl LatencyProbeHandler {
    pub fn new(ledger: Arc<LedgerSlot>, waiter: Arc<Waiter>) -> Self {
        Self { ledger, waiter }
    }

    /// One ambient sample: capture the sequencer head, then wait until the
    /// stage reaches it via the shared [`Waiter`]. Returns `(elapsed,
    /// timed_out)` — `timed_out` when the waiter hit its internal timeout.
    async fn probe_once(&self, level: ::proto::ledger::WaitLevel) -> (Duration, bool) {
        let target = self.ledger.current().last_sequenced_id();
        let wait_result = self.waiter.wait_for_transaction_level(target, level).await;
        match wait_result {
            Ok(duration) => (duration, false),
            Err(e) => (
                Duration::from_secs(0),
                e.kind() == std::io::ErrorKind::TimedOut,
            ),
        }
    }
}

#[tonic::async_trait]
impl LatencyProbe for LatencyProbeHandler {
    async fn probe(
        &self,
        request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        let req = request.into_inner();
        // The probe's WaitLevel mirrors the ledger's value-for-value, so the
        // raw i32 maps straight across to the type the `Waiter` expects.
        let level = ::proto::ledger::WaitLevel::try_from(req.wait_level)
            .map_err(|_| Status::invalid_argument("unknown wait_level"))?;
        let count = req.probe_count.max(1);
        let interval = Duration::from_millis(req.probe_interval_ms);

        let mut latencies_ns = Vec::with_capacity(count as usize);
        let mut timeouts = 0u64;
        for i in 0..count {
            let (elapsed, timed_out) = self.probe_once(level).await;
            if timed_out {
                timeouts += 1;
            } else if !elapsed.is_zero() {
                // A zero elapsed means the stage was already caught up at
                // capture — nothing was in flight, so it isn't a latency
                // sample. Exclude it; counting it would peg the median at ~0.
                latencies_ns.push(elapsed.as_nanos() as u64);
            }
            if !interval.is_zero() && i + 1 < count {
                tokio::time::sleep(interval).await;
            }
        }

        Ok(Response::new(ProbeResponse {
            latencies_ns,
            timeouts,
        }))
    }
}
