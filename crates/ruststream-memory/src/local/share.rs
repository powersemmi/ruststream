//! How the local bus shares its state: by reference counting and a cell that a single thread
//! owns, or, where the `std` feature puts the bus next to a multi-thread runtime, by `Arc` and a
//! cell behind a critical section.
//!
//! Without `std` the sharing costs no atomic: `Rc` is a plain counter and the cell a borrow flag.
//! With `std` an `Arc` clone is one atomic increment, and each access to a cell enters a critical
//! section, which the embedding binary supplies (`critical-section/std` on a host: one
//! process-wide reentrant mutex).

use core::cell::RefCell;

#[cfg(not(feature = "std"))]
pub(super) use alloc::rc::Rc as Shared;
#[cfg(feature = "std")]
pub(super) use alloc::sync::Arc as Shared;

#[cfg(feature = "std")]
use embassy_sync::blocking_mutex::Mutex;
#[cfg(feature = "std")]
pub(super) use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex as RawLock;
#[cfg(not(feature = "std"))]
pub(super) use embassy_sync::blocking_mutex::raw::NoopRawMutex as RawLock;

/// A value that several holders of one bus read and change, one access at a time.
pub(super) struct Guarded<Value> {
    #[cfg(feature = "std")]
    cell: Mutex<RawLock, RefCell<Value>>,
    #[cfg(not(feature = "std"))]
    cell: RefCell<Value>,
}

impl<Value> Guarded<Value> {
    pub(super) const fn new(value: Value) -> Self {
        Self {
            #[cfg(feature = "std")]
            cell: Mutex::new(RefCell::new(value)),
            #[cfg(not(feature = "std"))]
            cell: RefCell::new(value),
        }
    }

    /// Runs `access` with the value; no other access to this value runs inside it.
    pub(super) fn with<Out>(&self, access: impl FnOnce(&mut Value) -> Out) -> Out {
        #[cfg(feature = "std")]
        return self.cell.lock(|cell| access(&mut cell.borrow_mut()));
        #[cfg(not(feature = "std"))]
        access(&mut self.cell.borrow_mut())
    }
}

impl<Value: Default> Default for Guarded<Value> {
    fn default() -> Self {
        Self::new(Value::default())
    }
}
