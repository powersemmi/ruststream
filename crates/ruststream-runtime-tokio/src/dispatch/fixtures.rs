//! The test doubles the dispatch loop's tests share: a delivery whose transport settles as the test
//! says, and the context a loop dispatches under outside any app.

use std::future::ready;

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

/// A delivery whose transport settles the way its [`Settlement`] says.
pub(super) struct PlainMessage {
    pub(super) payload: Bytes,
    pub(super) headers: HeaderMap,
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

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
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
