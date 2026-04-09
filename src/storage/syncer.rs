use std::fs::File;

pub struct Syncer {
    wal_file: File,
}

impl Syncer {
    pub(super) fn new(file: File) -> Self {
        Syncer { wal_file: file }
    }

    pub(crate) fn sync(&mut self) -> std::io::Result<()> {
        self.wal_file.sync_data()
    }
}
