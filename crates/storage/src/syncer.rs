use std::fs::File;
use std::os::fd::AsRawFd;

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
    pub fn new(file: File) -> Self {
        Syncer { wal_file: file }
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.wal_file.sync_data()
    }

    pub fn id(&self) -> i32 {
        self.wal_file.as_raw_fd()
    }
}
