//! How the local bus shares its state: by `Rc` and a cell no lock guards where the runtime is
//! single-threaded, or by `Arc` and a cell behind a critical section next to a multi-thread
//! runtime. The core's `send` feature picks, as the build unifies it.
//!
//! Without `send` the sharing costs no atomic: `Rc` is a plain counter, the cell a borrow flag,
//! and the embedding binary needs no critical-section implementation. With `send` an `Arc` clone
//! is one atomic increment, and each access to a cell enters a critical section (on a host,
//! `critical-section/std`: one process-wide reentrant mutex).

use alloc::rc::Rc;
use alloc::sync::Arc;
use core::cell::RefCell;
use core::ops::Deref;

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::{CriticalSectionRawMutex, NoopRawMutex};
use ruststream_core::IfSend;

/// The lock the bus's channels and cells take.
pub(super) type RawLock = IfSend<CriticalSectionRawMutex, NoopRawMutex>;

/// A pointer to a value several holders of one bus share.
pub(super) type Shared<Value> = <Sharing as Share>::Pointer<Value>;

/// How the bus shares: `Arc` next to a multi-thread runtime, `Rc` without one.
pub(super) type Sharing = IfSend<ByArc, ByRc>;

/// Makes shared pointers of one kind.
pub(super) trait Share {
    type Pointer<Value>: Deref<Target = Value> + Clone;

    fn new<Value>(value: Value) -> Self::Pointer<Value>;

    fn same<Value>(left: &Self::Pointer<Value>, right: &Self::Pointer<Value>) -> bool;
}

/// Sharing by `Arc`.
pub(super) struct ByArc;

impl Share for ByArc {
    type Pointer<Value> = Arc<Value>;

    fn new<Value>(value: Value) -> Arc<Value> {
        Arc::new(value)
    }

    fn same<Value>(left: &Arc<Value>, right: &Arc<Value>) -> bool {
        Arc::ptr_eq(left, right)
    }
}

/// Sharing by `Rc`.
pub(super) struct ByRc;

impl Share for ByRc {
    type Pointer<Value> = Rc<Value>;

    fn new<Value>(value: Value) -> Rc<Value> {
        Rc::new(value)
    }

    fn same<Value>(left: &Rc<Value>, right: &Rc<Value>) -> bool {
        Rc::ptr_eq(left, right)
    }
}

/// A value that several holders of one bus read and change, one access at a time.
pub(super) struct Guarded<Value> {
    cell: Mutex<RawLock, RefCell<Value>>,
}

impl<Value> Guarded<Value> {
    pub(super) const fn new(value: Value) -> Self {
        Self {
            cell: Mutex::new(RefCell::new(value)),
        }
    }

    /// Runs `access` with the value; no other access to this value runs inside it.
    pub(super) fn with<Out>(&self, access: impl FnOnce(&mut Value) -> Out) -> Out {
        self.cell.lock(|cell| access(&mut cell.borrow_mut()))
    }
}

impl<Value: Default> Default for Guarded<Value> {
    fn default() -> Self {
        Self::new(Value::default())
    }
}
