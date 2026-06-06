use std::fs::File;
use std::os::fd::AsRawFd;

pub struct Syncer {
    wal_file: File,
    #[cfg(feature = "fault-injection")]
    fault: Option<crate::fault::WalFaultHook>,
}

impl Clone for Syncer {
    fn clone(&self) -> Self {
        Self {
            wal_file: self.wal_file.try_clone().expect("Failed to clone WAL file"),
            #[cfg(feature = "fault-injection")]
            fault: self.fault.clone(),
        }
    }
}

impl Syncer {
    pub fn new(file: File) -> Self {
        Syncer {
            wal_file: file,
            #[cfg(feature = "fault-injection")]
            fault: None,
        }
    }

    /// Attach a fault hook; only available with the `fault-injection`
    /// feature. Callers (Segment / Storage) thread this through so
    /// the WAL committer's `sync_data` call passes through the hook
    /// before hitting the kernel.
    #[cfg(feature = "fault-injection")]
    pub fn with_fault(mut self, fault: Option<crate::fault::WalFaultHook>) -> Self {
        self.fault = fault;
        self
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        #[cfg(feature = "fault-injection")]
        if let Some(hook) = self.fault.as_ref() {
            hook.before_sync();
        }
        self.wal_file.sync_data()
    }

    pub fn id(&self) -> i32 {
        self.wal_file.as_raw_fd()
    }
}
