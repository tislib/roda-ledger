/// Parses `wal_NNNNNN.bin` → Some(NNNNNN), anything else → None.
pub fn parse_segment_id(name: &str) -> Option<u32> {
    let name = name.strip_prefix("wal_")?.strip_suffix(".bin")?;
    if name.len() == 6 && name.chars().all(|c| c.is_ascii_digit()) {
        name.parse().ok()
    } else {
        None
    }
}
