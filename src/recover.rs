use crate::entities::WalEntry;
use crate::seal::Seal;
use crate::sequencer::Sequencer;
use crate::snapshot::Snapshot;
use crate::storage::SegmentStaus::SEALED;
use crate::storage::{Segment, Storage};
use crate::transactor::Transactor;
use crate::wal::Wal;
use spdlog::info;

pub struct Recover<'r> {
    transactor: &'r mut Transactor,
    wal: &'r Wal,
    snapshot: &'r Snapshot,
    seal: &'r mut Seal,
    sequencer: &'r Sequencer,
    storage: &'r Storage,
    segments: Vec<Segment>,
}

impl<'r> Recover<'r> {
    pub fn new(
        transactor: &'r mut Transactor,
        wal: &'r Wal,
        snapshot: &'r Snapshot,
        seal: &'r mut Seal,
        sequencer: &'r Sequencer,
        storage: &'r Storage,
    ) -> Self {
        Self {
            transactor,
            wal,
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
                    self.transactor
                        .recover_balance(account_id as usize, balance);
                    self.snapshot.recover_balance(account_id as usize, balance);
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
                    self.transactor
                        .recover_balance(entry.account_id as usize, entry.computed_balance);
                    self.snapshot
                        .recover_balance(entry.account_id as usize, entry.computed_balance);
                }
                _ => {}
            })?;
        }

        // process active WAL records
        let active_segment = self.storage.active_segment()?;
        active_segment.visit_wal_records(|record| match record {
            WalEntry::Metadata(metadata) => last_tx_id = metadata.tx_id,
            WalEntry::Entry(entry) => {
                self.transactor
                    .recover_balance(entry.account_id as usize, entry.computed_balance);
                self.snapshot
                    .recover_balance(entry.account_id as usize, entry.computed_balance);
            }
            _ => {}
        })?;

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
                if segment.id() <= last_snapshot_segment_id {
                    panic!("Snapshot segments must be in ascending order by id"); // fixme delete this panic
                }
                last_snapshot_segment_id = segment.id();
            }
        }

        Ok(last_snapshot_segment_id)
    }
}
