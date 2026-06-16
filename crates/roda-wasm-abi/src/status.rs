//! The `execute` return status (ADR-014).
//!
//! `0` commits (subject to the zero-sum check); any non-zero code rolls the
//! whole transaction back. `1..=127` are standard reasons, `128..=255` are
//! module-defined.

/// Status code returned from a module's `execute` body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status(u8);

impl Status {
    /// Commit the transaction.
    pub const OK: Status = Status(0);

    /// Roll back with a custom failure `code` (use `1..=255`).
    pub const fn fail(code: u8) -> Status {
        Status(code)
    }

    /// The raw status byte.
    pub const fn code(self) -> u8 {
        self.0
    }
}

impl From<u8> for Status {
    fn from(code: u8) -> Self {
        Status(code)
    }
}

impl From<()> for Status {
    fn from(_: ()) -> Self {
        Status::OK
    }
}

/// `Ok(())` commits; `Err(code)` rolls back with `code`.
impl From<Result<(), u8>> for Status {
    fn from(r: Result<(), u8>) -> Self {
        match r {
            Ok(()) => Status::OK,
            Err(code) => Status(code),
        }
    }
}
