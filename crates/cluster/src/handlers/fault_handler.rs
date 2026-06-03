//! Tonic implementation of the `Fault` service from
//! `proto::fault::fault_server::Fault`.
//!
//! Compiled only with `--features fault-injection`. Production
//! builds compile this whole module out so the gRPC surface
//! literally doesn't exist on the wire.

use crate::fault::{ClusterFaultInjector, FaultError};
use proto::fault::fault_server::Fault;
use proto::fault::{
    apply_file_event_request, ApplyFileEventRequest, ApplyFileEventResponse,
    ClearAllFaultsRequest, ClearAllFaultsResponse, ClearFaultRequest, ClearFaultResponse,
    ClockJumpRequest, ClockJumpResponse, SetClockSkewRequest, SetClockSkewResponse,
    SetFaultRequest, SetFaultResponse, UnstuckOperationRequest, UnstuckOperationResponse,
};
use std::path::Path;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct FaultHandler {
    injector: Arc<ClusterFaultInjector>,
    /// Resolved at construction so file-event RPCs don't need to
    /// thread the `Config` through.
    data_dir: String,
}

impl FaultHandler {
    pub fn new(injector: Arc<ClusterFaultInjector>, data_dir: String) -> Self {
        Self { injector, data_dir }
    }
}

#[tonic::async_trait]
impl Fault for FaultHandler {
    async fn set_fault(
        &self,
        request: Request<SetFaultRequest>,
    ) -> Result<Response<SetFaultResponse>, Status> {
        let req = request.into_inner();
        let level = req
            .level
            .ok_or_else(|| Status::invalid_argument("SetFaultRequest.level is required"))?;
        let outcome = req
            .outcome
            .ok_or_else(|| Status::invalid_argument("SetFaultRequest.outcome is required"))?;

        let assigned_stuck_id = self
            .injector
            .set_fault(&level, &outcome)
            .map_err(|e| match e {
                FaultError::InvalidOutcome(_, _) => Status::invalid_argument(e.to_string()),
                FaultError::UnknownBucket => Status::invalid_argument(e.to_string()),
                FaultError::FileEventFailed(_) => Status::internal(e.to_string()),
            })?;

        Ok(Response::new(SetFaultResponse { assigned_stuck_id }))
    }

    async fn clear_fault(
        &self,
        request: Request<ClearFaultRequest>,
    ) -> Result<Response<ClearFaultResponse>, Status> {
        let req = request.into_inner();
        let level = req
            .level
            .ok_or_else(|| Status::invalid_argument("ClearFaultRequest.level is required"))?;
        let removed = self.injector.clear_fault(&level);
        Ok(Response::new(ClearFaultResponse { removed }))
    }

    async fn clear_all_faults(
        &self,
        _request: Request<ClearAllFaultsRequest>,
    ) -> Result<Response<ClearAllFaultsResponse>, Status> {
        let (cleared_levels, released_stuck_ops) = self.injector.clear_all();
        Ok(Response::new(ClearAllFaultsResponse {
            cleared_levels,
            released_stuck_ops,
        }))
    }

    async fn unstuck_operation(
        &self,
        request: Request<UnstuckOperationRequest>,
    ) -> Result<Response<UnstuckOperationResponse>, Status> {
        let req = request.into_inner();
        let released = self.injector.unstuck(&req.stuck_id);
        Ok(Response::new(UnstuckOperationResponse { released }))
    }

    async fn apply_file_event(
        &self,
        request: Request<ApplyFileEventRequest>,
    ) -> Result<Response<ApplyFileEventResponse>, Status> {
        let req = request.into_inner();
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("ApplyFileEventRequest.event is required"))?;
        let (applied, bytes_affected) = apply_file_event(Path::new(&self.data_dir), event)
            .map_err(|e| Status::internal(format!("file event failed: {}", e)))?;
        Ok(Response::new(ApplyFileEventResponse {
            applied,
            bytes_affected,
        }))
    }

    async fn set_clock_skew(
        &self,
        request: Request<SetClockSkewRequest>,
    ) -> Result<Response<SetClockSkewResponse>, Status> {
        let req = request.into_inner();
        self.injector.set_clock_skew(req.offset_ms);
        Ok(Response::new(SetClockSkewResponse {}))
    }

    async fn clock_jump(
        &self,
        _request: Request<ClockJumpRequest>,
    ) -> Result<Response<ClockJumpResponse>, Status> {
        // The wrapped MockClock that backs clock control lands in a
        // follow-up PR (see ADR-018 "Time and process-control
        // primitives"). For now the RPC succeeds so test fixtures can
        // already call it; the jump is a no-op until the wrapper is
        // installed in Consensus / Ledger.
        spdlog::warn!(
            "fault: ClockJump RPC received but MockClock wrapper not yet installed; no-op"
        );
        Ok(Response::new(ClockJumpResponse {}))
    }
}

// ─── File-event execution ──────────────────────────────────────

fn apply_file_event(
    data_dir: &Path,
    event: apply_file_event_request::Event,
) -> std::io::Result<(bool, u64)> {
    use apply_file_event_request::Event;
    match event {
        Event::DeleteActiveWal(_) => {
            let path = data_dir.join("wal.bin");
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path)?;
            Ok((true, len))
        }
        Event::DeleteSealedWal(req) => {
            let path = data_dir.join(format!("wal_{:06}.bin", req.segment_id));
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path)?;
            Ok((true, len))
        }
        Event::DeleteSnapshot(req) => {
            let bin = data_dir.join(format!("snapshot_{:06}.bin", req.segment_id));
            let mut affected = std::fs::metadata(&bin).map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&bin)?;
            if req.delete_crc {
                let crc = data_dir.join(format!("snapshot_{:06}.crc", req.segment_id));
                if let Ok(m) = std::fs::metadata(&crc) {
                    affected += m.len();
                    let _ = std::fs::remove_file(&crc);
                }
            }
            Ok((true, affected))
        }
        Event::DeleteFunctionSnapshot(req) => {
            let path = data_dir.join(format!("function_snapshot_{:06}.bin", req.segment_id));
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path)?;
            Ok((true, len))
        }
        Event::CorruptWalFile(req) => {
            let path = data_dir.join(format!("wal_{:06}.bin", req.segment_id));
            corrupt_range(&path, req.offset, req.length as u64, req.pattern)?;
            Ok((true, req.length as u64))
        }
        Event::OverwriteSegmentBytes(req) => {
            let path = data_dir.join(format!("wal_{:06}.bin", req.segment_id));
            overwrite_at(&path, req.offset, &req.replacement)?;
            Ok((true, req.replacement.len() as u64))
        }
    }
}

fn corrupt_range(
    path: &Path,
    offset: u64,
    length: u64,
    pattern: i32,
) -> std::io::Result<()> {
    use proto::fault::CorruptionPattern;
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
    let mut buf = vec![0u8; length as usize];
    file.read_at(&mut buf, offset)?;
    match CorruptionPattern::try_from(pattern).unwrap_or(CorruptionPattern::PatternUnspecified) {
        CorruptionPattern::FlipAllBits => {
            for b in buf.iter_mut() {
                *b ^= 0xFF;
            }
        }
        CorruptionPattern::Zero => {
            buf.fill(0);
        }
        CorruptionPattern::Random => {
            // Use a stable seed so a test that runs twice produces
            // the same corrupted bytes — important for determinism.
            // Seed = (offset ^ length); cheap and reproducible.
            let mut x: u64 = offset ^ length ^ 0xDEAD_BEEF_CAFE_BABE;
            for b in buf.iter_mut() {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                *b = (x >> 33) as u8;
            }
        }
        CorruptionPattern::PatternUnspecified => {
            // Treat as a no-op rather than silently fall through.
            spdlog::warn!("fault: CorruptWalFile pattern UNSPECIFIED — no bytes mutated");
            return Ok(());
        }
    }
    file.write_at(&buf, offset)?;
    file.sync_data()?;
    Ok(())
}

fn overwrite_at(path: &Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.write_at(bytes, offset)?;
    file.sync_data()?;
    Ok(())
}
