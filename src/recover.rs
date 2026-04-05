use crate::entities::WalEntry;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use crate::transactor::Transactor;
use spdlog::info;
use std::collections::HashMap;

pub struct Recover<'r> {
    transactor: &'r mut Transactor,
    snapshot: &'r mut Snapshot,
    seal: &'r mut Seal,
    sequencer: &'r Sequencer,
    storage: &'r Storage,
    segments: Vec<Segment>,
}

impl<'r> Recover<'r> {
    pub fn new(
        transactor: &'r mut Transactor,
        snapshot: &'r mut Snapshot,
        seal: &'r mut Seal,
        sequencer: &'r Sequencer,
        storage: &'r Storage,
    ) -> Self {
        Self {
            transactor,
            snapshot,
            seal,
            sequencer,
            storage,
            segments: vec![],
        }
    }

    pub fn recover(&mut self) -> Result<(), std::io::Error> {
        info!("Starting recovery...");

        // locate segments
        self.segments = self.storage.list_all_segments()?;

        // find the latest snapshot
        let latest_snapshot_segment_id = self.locate_latest_snapshot_segment_id()?;
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
                let data = segment.load_snapshot()?.unwrap();

                for (account_id, balance) in data.balances {
                    recover_balances.insert(account_id, balance);
                    self.seal.recover_balance(account_id as usize, balance)?;
                }

                last_tx_id = data.last_tx_id;

                continue;
            }

            // process the segments after snapshot
            if segment.status() != SEALED {
                self.seal.recover_pre_seal(segment)?;
            }

            segment.load()?;

            segment.visit_wal_records(|record| match record {
                WalEntry::Metadata(metadata) => last_tx_id = metadata.tx_id,
                WalEntry::Entry(entry) => {
                    recover_balances.insert(entry.account_id, entry.computed_balance);
                }
                _ => {}
            })?;
        }

        // process active WAL records
        let active_segment = self.storage.active_segment()?;
        active_segment.visit_wal_records(|record| match record {
            WalEntry::Metadata(metadata) => {
                last_tx_id = metadata.tx_id;
                self.snapshot.recover_index_tx_metadata(metadata);
            }
            WalEntry::Entry(entry) => {
                recover_balances.insert(entry.account_id, entry.computed_balance);
                self.snapshot.recover_index_tx_entry(entry);
            }
            _ => {}
        })?;

        self.transactor.recover_balances(&recover_balances);
        self.snapshot.recover_balances(&recover_balances);

        // restore last tx ids
        self.transactor.store_last_processed_id(last_tx_id);
        self.snapshot.store_last_processed_id(last_tx_id);
        self.sequencer.set_next_id(last_tx_id + 1);

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
