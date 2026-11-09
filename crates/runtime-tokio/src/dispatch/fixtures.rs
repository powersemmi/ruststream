//! The test doubles the dispatch loop's tests share: a delivery whose transport settles as the test
//! says, the batch form of a test subscriber's stream, and the context a loop dispatches under
//! outside any app.

use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use ruststream_core::BatchStream;
use tokio_util::task::TaskTracker;

use crate::detach::Hosted;
use crate::{AckError, Delivery, HeaderMap, IncomingMessage, NoRetry, Settle};

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
}

impl Settle for PlainMessage {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settlement.apply())
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settlement.apply())
    }
}

/// What a test subscriber's stream yields in place of a delivery.
#[derive(Debug, thiserror::Error)]
#[error("subscriber stream failed")]
pub(super) struct StreamFault;

/// The batch form of a subscriber's own stream. A batch closes as soon as it is full, so the
/// size stays at one: a subscription that goes quiet hands over the delivery it has rather than
/// waiting for the rest of the batch.
pub(super) fn batched<S>(
    stream: S,
    size: NonZeroUsize,
) -> impl BatchStream<Vec<PlainMessage>, Error = StreamFault> + Send
where
    S: Stream<Item = Result<PlainMessage, StreamFault>> + Send,
{
    Poured(Box::pin(
        stream
            .chunks(size.get())
            .map(|chunk| chunk.into_iter().collect::<Result<Vec<_>, _>>()),
    ))
}

/// Fills a slot with the batches of a stream of whole batches.
pub(super) struct Poured<S>(Pin<Box<S>>);

impl<S> BatchStream<Vec<PlainMessage>> for Poured<S>
where
    S: Stream<Item = Result<Vec<PlainMessage>, StreamFault>>,
{
    type Error = StreamFault;

    fn poll_fill(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        slot: &mut Vec<PlainMessage>,
    ) -> Poll<Option<Result<(), StreamFault>>> {
        self.0.as_mut().poll_next(cx).map(|item| {
            item.map(|batch| {
                batch.map(|batch| {
                    slot.extend(batch);
                })
            })
        })
    }
}

/// A subscription's context outside any app, its leftovers tracked on the test's own runtime.
pub(super) trait Unscoped {
    /// An empty context: no deferred retry, a fresh continuation tracker.
    fn empty() -> Self;
}

impl<C> Unscoped for Hosted<C, NoRetry> {
    fn empty() -> Self {
        Self::new(Delivery::unscoped(), None, TaskTracker::new())
    }
}
