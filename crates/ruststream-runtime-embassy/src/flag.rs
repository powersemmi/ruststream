//! A flag read and set under the frame lock.
//!
//! Without the `tokio` feature the lock is a no-op, so the flag is a plain cell and an embedded
//! build needs neither atomics nor a critical-section implementation. Next to a multi-thread
//! runtime the lock is a critical section.

use core::cell::Cell;

use embassy_sync::blocking_mutex::Mutex;

use crate::frames::FrameLock;

/// A boolean that starts lowered.
pub(crate) struct Flag(Mutex<FrameLock, Cell<bool>>);

impl Flag {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Cell::new(false)))
    }

    pub(crate) fn raise(&self) {
        self.0.lock(|flag| flag.set(true));
    }

    pub(crate) fn is_raised(&self) -> bool {
        self.0.lock(Cell::get)
    }

    /// Lowers the flag and says whether it was raised.
    pub(crate) fn take(&self) -> bool {
        self.0.lock(Cell::take)
    }
}
