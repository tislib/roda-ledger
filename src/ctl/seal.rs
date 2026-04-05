use std::path::Path;

use crate::storage::SegmentStaus;

use super::{CtlError, open_segment_from_path};

pub fn run(segment_path: &Path, force: bool) -> Result<(), CtlError> {
    if !segment_path.exists() {
        return Err(CtlError::new(format!("file not found: {:?}", segment_path)));
    }

    let mut segment = open_segment_from_path(segment_path)?;

    if segment.status() == SegmentStaus::SEALED && !force {
        return Err(CtlError::new(
            "segment is already SEALED (.seal marker exists). Use --force to reseal.",
        ));
    }

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
    eprintln!("Sealed segment {:06}", segment.id());
    Ok(())
}
