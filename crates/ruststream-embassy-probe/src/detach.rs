//! Where a delivery's leftover work would go: this probe runs none, and counts what arrives.

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;

use ruststream_runtime::{BoxFuture, Detach};

/// A host that counts the work deliveries leave behind instead of running it.
///
/// The probe's handler leaves no continuation and no delayed retry, so a count above zero means
/// the body detached work the scenario did not ask for.
#[derive(Debug, Default)]
pub struct CountDetached {
    detached: AtomicUsize,
}

impl CountDetached {
    /// A host with nothing detached yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            detached: AtomicUsize::new(0),
        }
    }

    /// How many pieces of work deliveries detached.
    #[must_use]
    pub fn detached(&self) -> usize {
        self.detached.load(Ordering::Relaxed)
    }
}

impl Detach for CountDetached {
    fn detach(&self, _work: BoxFuture<'static, ()>) {
        self.detached.fetch_add(1, Ordering::Relaxed);
    }

    fn detach_after(&self, _delay: Duration, _work: BoxFuture<'static, ()>) {
        self.detached.fetch_add(1, Ordering::Relaxed);
    }
}
