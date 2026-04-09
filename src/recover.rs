use crate::entities::{FailReason, WalEntry};
use crate::pipeline::Pipeline;
use crate::seal::Seal;
use crate::snapshot::Snapshot;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use crate::transactor::Transactor;
use spdlog::info;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Recover<'r> {
    transactor: &'r mut Transactor,
    snapshot: &'r mut Snapshot,
    seal: &'r mut Seal,
    pipeline: &'r Arc<Pipeline>,
    storage: &'r Storage,
    segments: Vec<Segment>,
}

impl<'r> Recover<'r> {
    pub fn new(
        transactor: &'r mut Transactor,
        snapshot: &'r mut Snapshot,
        seal: &'r mut Seal,
        pipeline: &'r Arc<Pipeline>,
        storage: &'r Storage,
    ) -> Self {
        Self {
            transactor,
            snapshot,
            seal,
            pipeline,
            storage,
            segments: vec![],
        }
    }

    pub fn recover(&mut self) -> Result<(), std::io::Error> {
        info!("Starting recovery...");

        // locate segments
        self.segments = self.storage.list_all_segments().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to list segments during recovery: {}", e),
            )
        })?;

        // find the latest snapshot
        let latest_snapshot_segment_id = self.locate_latest_snapshot_segment_id().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to locate latest snapshot segment: {}", e),
            )
        })?;
        let mut last_tx_id = 0;
        let mut recover_balances = HashMap::new();

        // restore the latest snapshot
        for segment in self.segments.iter_mut() {
            // ignore segments before snapshot
            if segment.id() < latest_snapshot_segment_id {
                continue;
            }

            // restore the snapshot
            if segment.id() == latest_snapshot_segment_id {
                let data = segment
                    .load_snapshot()
                    .map_err(|e| {
                        std::io::Error::new(
                            e.kind(),
                            format!(
                                "failed to load snapshot for segment {}: {}",
                                segment.id(),
                                e
                            ),
                        )
                    })?
                    .unwrap();

                for (account_id, balance) in data.balances {
                    recover_balances.insert(account_id, balance);
                    self.seal
                        .recover_balance(account_id as usize, balance)
                        .map_err(|e| {
                            std::io::Error::new(
                                e.kind(),
                                format!(
                                    "failed to recover balance for account {} from snapshot: {}",
                                    account_id, e
                                ),
                            )
                        })?;
                }

                last_tx_id = data.last_tx_id;

                continue;
            }

            // process the segments after snapshot
            if segment.status() != SEALED {
                let sealed_id = self.seal.recover_pre_seal(segment).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to recover pre-seal for segment {}: {}",
                            segment.id(),
                            e
                        ),
                    )
                })?;
                self.pipeline.set_seal_index(sealed_id);
            }

            segment.load().map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to load segment {}: {}", segment.id(), e),
                )
            })?;

            segment
                .visit_wal_records(|record| match record {
                    WalEntry::Metadata(metadata) => {
                        last_tx_id = metadata.tx_id;
                    }
                    WalEntry::Entry(entry) => {
                        recover_balances.insert(entry.account_id, entry.computed_balance);
                    }
                    WalEntry::Link(_) => {}
                    _ => {}
                })
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to visit wal records for segment {}: {}",
                            segment.id(),
                            e
                        ),
                    )
                })?;
        }

        // process active WAL records
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let active_segment = self.storage.active_segment().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to get active segment: {}", e))
        })?;
        let mut current_recover_tx_id = 0u64;
        active_segment
            .visit_wal_records(|record| match record {
                WalEntry::Metadata(metadata) => {
                    last_tx_id = metadata.tx_id;
                    current_recover_tx_id = metadata.tx_id;
                    self.snapshot.recover_index_tx_metadata(metadata);
                }
                WalEntry::Entry(entry) => {
                    recover_balances.insert(entry.account_id, entry.computed_balance);
                    self.snapshot.recover_index_tx_entry(entry);
                }
                WalEntry::Link(link) => {
                    self.snapshot
                        .recover_index_tx_link(current_recover_tx_id, link);
                }
                _ => {}
            })
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to visit active wal records: {}", e),
                )
            })?;

        // Rebuild dedup cache from active WAL committed transactions
        // Re-scan active WAL to populate dedup entries
        active_segment
            .visit_wal_records(|record| {
                if let WalEntry::Metadata(metadata) = record {
                    // Only non-duplicate committed transactions should be in the dedup cache
                    if metadata.user_ref != 0 && metadata.fail_reason != FailReason::DUPLICATE {
                        let timestamp_ms = metadata.timestamp / 1_000_000; // nanos → ms
                        self.transactor.dedup_cache_mut().recover_entry(
                            metadata.user_ref,
                            metadata.tx_id,
                            timestamp_ms,
                            now_ms,
                        );
                    }
                }
            })
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("failed to re-scan active wal records for dedup: {}", e),
                )
            })?;

        self.transactor.recover_balances(&recover_balances);
        self.snapshot.recover_balances(&recover_balances);

        // Restore last tx ids in the pipeline indexes.
        self.pipeline.set_compute_index(last_tx_id);
        self.pipeline.set_snapshot_index(last_tx_id);
        self.pipeline.set_sequencer_next_id(last_tx_id + 1);

        info!("Recovery completed successfully.");

        Ok(())
    }

    fn locate_latest_snapshot_segment_id(&self) -> Result<u32, std::io::Error> {
        let mut last_snapshot_segment_id = 0;

        for segment in self.segments.iter() {
            if segment.has_snapshot() {
                last_snapshot_segment_id = segment.id();
            }
        }

        Ok(last_snapshot_segment_id)
    }
}
