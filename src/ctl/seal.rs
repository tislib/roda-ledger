use std::path::Path;

use crate::storage::SegmentStaus;

use super::{CtlError, make_storage, parse_segment_id_from_path};

pub fn run(segment_path: &Path, force: bool) -> Result<(), CtlError> {
    let segment_id = parse_segment_id_from_path(segment_path).ok_or_else(|| {
        CtlError::new(format!(
            "cannot parse segment ID from {:?} (expected wal_NNNNNN.bin)",
            segment_path.file_name().unwrap_or_default()
        ))
    })?;

    let data_dir = segment_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_string_lossy()
        .to_string();

    if !segment_path.exists() {
        return Err(CtlError::new(format!("file not found: {:?}", segment_path)));
    }
    if segment_path.file_name().is_some_and(|f| f == "wal.bin") {
        return Err(CtlError::new("cannot seal the active WAL (wal.bin)"));
    }

    let seal_marker = segment_path.with_extension("seal");
    if seal_marker.exists() && !force {
        return Err(CtlError::new(
            "segment is already SEALED (.seal marker exists). Use --force to reseal.",
        ));
    }

    let storage = make_storage(&data_dir)?;
    let mut segment = storage.segment(segment_id)?;

    if force {
        segment.force_unseal()?;
    }

    if segment.status() != SegmentStaus::CLOSED {
        return Err(CtlError::new(format!(
            "segment is {:?}, expected CLOSED",
            segment.status()
        )));
    }

    segment.load()?;
    segment.seal()?;
    eprintln!("Sealed segment {:06}", segment_id);
    Ok(())
}
