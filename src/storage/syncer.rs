use std::fs::File;

pub struct Syncer {
    wal_file: File,
}

impl Clone for Syncer {
    fn clone(&self) -> Self {
        Self {
            wal_file: self.wal_file.try_clone().expect("Failed to clone WAL file"),
        }
    }
}

impl Syncer {
    pub(super) fn new(file: File) -> Self {
        Syncer { wal_file: file }
    }

    pub(crate) fn sync(&mut self) -> std::io::Result<()> {
        self.wal_file.sync_data()
    }
}
