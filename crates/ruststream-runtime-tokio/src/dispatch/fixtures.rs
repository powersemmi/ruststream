//! The test doubles the dispatch loop's tests share: a delivery that records how it settled, and
//! the context a loop dispatches under outside any app.

use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;
use tokio_util::task::TaskTracker;

use crate::detach::Hosted;
use crate::{AckError, Delivery, HeaderMap, IncomingMessage};

/// What a test delivery's transport does when asked to settle. The cases differ in kind, not
/// degree, so they are variants rather than a flag.
#[derive(Clone, Copy)]
pub(super) enum Settlement {
    /// Settlement lands, as on a broker with acknowledgements.
    Accepted,
    /// The broker's settlement panics: a fault of the transport, past the handler's guard.
    Panics,
}

impl Settlement {
    // The settle methods hand this straight back, so it keeps their result's shape; these loops'
    // tests never need a refusal.
    #[allow(clippy::unnecessary_wraps)]
    fn apply(self) -> Result<(), AckError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Panics => panic!("the broker's settlement panicked"),
        }
    }
}

/// A delivery without native delayed redelivery: `supports_nack_after` stays at the trait
/// default (`false`), and the default `nack_after` would error. It records how it was settled
/// so a test can assert the fallback dropped it rather than calling `nack(true)`.
pub(super) struct PlainMessage {
    pub(super) payload: Bytes,
    pub(super) headers: HeaderMap,
    // 0 = unset, 1 = nack(false) (dropped), 2 = nack(true) (requeued).
    pub(super) settled: Arc<AtomicU8>,
    pub(super) settlement: Settlement,
}

impl IncomingMessage for PlainMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settlement.apply())
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.settled
            .store(if requeue { 2 } else { 1 }, Ordering::SeqCst);
        ready(self.settlement.apply())
    }
}

/// A subscription's context outside any app, its leftovers tracked on the test's own runtime.
pub(super) trait Unscoped {
    /// An empty context: no deferred retry, a fresh continuation tracker.
    fn empty() -> Self;
}

impl<C> Unscoped for Hosted<C> {
    fn empty() -> Self {
        Self::new(Delivery::unscoped(), TaskTracker::new())
    }
}
