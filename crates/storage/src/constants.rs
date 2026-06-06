/// Every WAL record is `#[repr(C)]` and exactly 40 bytes (entities.rs asserts).
pub const WAL_RECORD_SIZE: usize = 40;

/// `tx_id` offset shared by TxMetadata, TxEntry, TxLink.
pub const TX_ID_OFFSET: usize = 8;
