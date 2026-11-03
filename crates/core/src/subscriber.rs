//! The [`Subscriber`] trait: a stream of broker deliveries.

use core::error::Error as StdError;

use futures_core::Stream;

use crate::{MaybeSend, MaybeSendSync, Settle};

/// A consumer attached to one or more broker names.
///
/// `Subscriber` yields messages via a [`Stream`], so users get back-pressure and
/// integration with the rest of the futures ecosystem for free. Each yielded item is a
/// broker-specific delivery ([`IncomingMessage`](crate::IncomingMessage), settled through
/// [`Settle`]) that must be acknowledged.
///
/// # Cancel safety
///
/// Polling [`stream`] is cancel-safe: dropping the returned `Stream` between polls is
/// allowed. Implementations must not buffer partially-decoded frames in `&self` state; if
/// buffering is required, it belongs in `&mut self`.
///
/// [`stream`]: Self::stream
pub trait Subscriber: MaybeSend {
    /// The message type yielded by this subscriber.
    type Message: Settle;

    /// The error type yielded by the stream when delivery fails.
    type Error: StdError + Send + Sync + 'static;

    /// Returns a stream of broker deliveries.
    ///
    /// The broker defines its own reconnection policy. The stream ends when the subscription
    /// is cancelled, the broker shuts down, or the broker's reconnection policy gives up.
    fn stream(&mut self)
    -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_;

    /// Returns the probe an embedded app asks, in place of polling [`stream`](Self::stream),
    /// whether an idle subscription may have something to yield.
    ///
    /// The runtime takes it once, before it opens the stream, and keeps it beside the stream for
    /// the life of the subscription. The default is `()`: the stream is polled on every wake of
    /// the app, which is also what a multi-thread runtime does with any probe, because each of its
    /// subscriptions has a task and a waker of its own.
    ///
    /// A device broker over a channel or a socket returns a handle on the state its stream reads;
    /// see [`ReadyHint`] for the contract and a full example.
    ///
    /// The probe is an owned handle that does not borrow the subscriber: the type captures the
    /// subscriber's own type and nothing else (`use<Self>` in the trait, the impl's generic
    /// parameters in an implementation), so the app holds it beside the subscription without
    /// borrowing it.
    fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Self> {}
}

/// What an embedded app asks an idle subscription before it polls the subscription's stream.
///
/// An embedded app is one task that joins every subscription, and a wake of that task cannot tell
/// which subscription woke it, so each idle subscription is polled on every wake. A subscriber
/// that returns a `ReadyHint` from [`Subscriber::ready_hint`] lets the app skip the subscription
/// while the answer is `false`: the join that runs the subscriptions asks the probe and does not
/// resume the subscription at all. The cost of an idle subscription per wake falls from a resume
/// and a stream poll to that call.
/// In the `embedded_idle` benchmark of the repository (release, fat LTO, one busy subscription
/// among sixteen) an idle subscription costs 51 instructions per delivery in place of 143, which
/// is 64% less; what remains is the join's walk over the subscriptions and the probe's own check.
///
/// # The contract
///
/// - [`may_be_ready`](Self::may_be_ready) is `true` whenever the next poll of the stream could
///   return `Ready`: an item, an error or the end. This includes the end that follows a shutdown
///   of the broker. A `false` while a delivery waits stalls the subscription.
/// - A `true` that turns out to be early costs one poll that answers `Pending`. The constant
///   `true` is legal and gains nothing.
/// - The stream registers the task's waker whenever it returns `Pending`, as every stream does.
///   The app skips polls only after a poll that returned `Pending`, so that registration stands
///   until the delivery that wakes it.
/// - The probe reads state the broker owns and takes no lock a poll of the stream holds.
/// - The probe is an owned handle: it does not borrow the subscriber, so the app keeps it beside
///   the subscription while the stream borrows the subscriber mutably. An implementation writes
///   `use<..>` after the bounds, listing its type and const parameters and no lifetime.
///
/// `conformance::harness::local_readiness` holds a broker to this contract.
///
/// # Examples
///
/// A device broker whose frames arrive in a queue a driver fills. The probe is a second handle on
/// the queue, taken once before the stream opens.
///
/// ```
/// use std::collections::VecDeque;
/// use std::convert::Infallible;
/// use std::pin::Pin;
/// use std::sync::{Arc, Mutex};
/// use std::task::{Context, Poll, Waker};
///
/// use futures::Stream;
/// use ruststream::{AckError, HeaderMap, IncomingMessage, ReadyHint, Settle, Subscriber};
///
/// /// What the radio driver fills and the subscription drains.
/// #[derive(Default)]
/// struct Inbox {
///     frames: VecDeque<Vec<u8>>,
///     reader: Option<Waker>,
/// }
///
/// type SharedInbox = Arc<Mutex<Inbox>>;
///
/// /// One received frame.
/// struct Frame {
///     bytes: Vec<u8>,
///     headers: HeaderMap,
/// }
///
/// impl IncomingMessage for Frame {
///     fn payload(&self) -> &[u8] {
///         &self.bytes
///     }
///
///     fn headers(&self) -> &HeaderMap {
///         &self.headers
///     }
/// }
///
/// impl Settle for Frame {
///     async fn ack(self) -> Result<(), AckError> {
///         Ok(())
///     }
///
///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
///         Ok(())
///     }
/// }
///
/// /// The stream of the subscription: it parks its waker with the driver while the queue is empty.
/// struct Frames(SharedInbox);
///
/// impl Stream for Frames {
///     type Item = Result<Frame, Infallible>;
///
///     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
///         let mut inbox = self.0.lock().expect("the driver does not panic");
///         if let Some(bytes) = inbox.frames.pop_front() {
///             return Poll::Ready(Some(Ok(Frame { bytes, headers: HeaderMap::new() })));
///         }
///         inbox.reader = Some(cx.waker().clone());
///         Poll::Pending
///     }
/// }
///
/// /// The probe: a frame is waiting.
/// struct FramesWaiting(SharedInbox);
///
/// impl ReadyHint for FramesWaiting {
///     fn may_be_ready(&self) -> bool {
///         !self.0.lock().expect("the driver does not panic").frames.is_empty()
///     }
/// }
///
/// struct RadioSubscriber(SharedInbox);
///
/// impl Subscriber for RadioSubscriber {
///     type Message = Frame;
///     type Error = Infallible;
///
///     fn stream(&mut self) -> impl Stream<Item = Result<Frame, Infallible>> + Send + '_ {
///         Frames(Arc::clone(&self.0))
///     }
///
///     fn ready_hint(&self) -> impl ReadyHint + 'static + use<> {
///         FramesWaiting(Arc::clone(&self.0))
///     }
/// }
///
/// let inbox = SharedInbox::default();
/// let radio = RadioSubscriber(Arc::clone(&inbox));
/// let probe = radio.ready_hint();
/// assert!(!probe.may_be_ready());
/// inbox.lock().expect("the test does not panic").frames.push_back(b"21".to_vec());
/// assert!(probe.may_be_ready());
/// ```
pub trait ReadyHint {
    /// Whether the runtime consults the probe at all: `false` for the unit probe of a subscriber
    /// that declares none, so the check compiles away.
    #[doc(hidden)]
    const GATED: bool = true;

    /// `true` whenever the next poll of the subscription's stream could return `Ready`.
    fn may_be_ready(&self) -> bool;
}

/// The probe of a subscriber that declares none: the stream is polled on every wake.
impl ReadyHint for () {
    const GATED: bool = false;

    fn may_be_ready(&self) -> bool {
        true
    }
}
