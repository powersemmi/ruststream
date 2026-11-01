//! Client-side batching: turn any [`Subscriber`] into a [`BatchSubscriber`].
//!
//! This is broker-author machinery. Every broker offers [`BatchSubscriber`], because every batch
//! handler asks for one; a broker whose client batches on the wire implements it directly, and a
//! broker whose transport delivers one message at a time gives its subscriber the capability
//! through [`BufferedSubscriber`], which assembles the batches on the client. The wrapper is
//! explicit: a blanket impl would collide with the native ones broker crates write.
//!
//! A service never names it. The batch size is the registration's
//! [`batch(n)`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.SubscriberSettings.html#method.batch) and arrives as the argument of
//! [`BatchSubscriber::batches`]; what stays the adapter's own is the deadline that closes a
//! partial batch.

// A subscriber wrapper generic over a subscriber that need not be Send.
#![allow(clippy::future_not_send)]

use std::mem;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll, ready};
use std::time::Duration;

use futures::Stream;
use tokio::time::{self, Instant, Sleep};

use crate::{
    AddressedCopies, BatchStream, BatchSubscriber, ConnectedBroker, DeclareRetryError,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, Seekable, Subscriber,
    SubscriptionSource,
};

const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(10);

/// A [`SubscriptionSource`] adapter that buffers the wrapped source's subscriber into a
/// [`BatchSubscriber`].
///
/// A batch closes when it holds the size the registration asked for, or once
/// [`max_wait`](Self::max_wait) has elapsed after its first delivery, whichever comes first; an
/// idle subscription waits indefinitely for that first delivery. The default deadline is 10 ms.
///
/// Under the `testing` harness, injecting through `tb.message(&value).publish()` drives the whole
/// reaction to a standstill before it returns, which closes the batch: one call per message yields
/// one batch per message, each holding a single element. A test that wants a longer batch takes a
/// producer handle off the broker before the app is built, publishes the run through it, and then
/// drives the reaction once with `tb.settle()`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::Buffered;
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{Declared, SubscriberBuilder};
/// use serde::Deserialize;
///
/// /// A broker crate's mount-site step for a transport that delivers one message at a time: the
/// /// batches are assembled on the client, and a partial one closes `wait` after it opened.
/// pub trait Linger {
///     type Out;
///     fn linger(self, wait: Duration) -> Self::Out;
/// }
///
/// impl<Def: Declared, State, DefCodec> Linger
///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
/// {
///     type Out = SubscriberBuilder<Def, Buffered<MemorySource>, State, DefCodec>;
///
///     fn linger(self, wait: Duration) -> Self::Out {
///         self.map_source(|source| Buffered::new(source).max_wait(wait))
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(MemorySource)]
/// async fn settle(orders: &[Order]) -> HandlerOutcome {
///     tracing::info!(count = orders.len(), "settled a batch");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(
///             settle
///                 .name("orders")
///                 .batch(nonzero!(64))
///                 .linger(Duration::from_millis(20)),
///         )
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct Buffered<S> {
    source: S,
    max_wait: Duration,
}

impl<S> Buffered<S> {
    /// Wraps `source`, batching its subscriber with the default deadline.
    #[must_use]
    pub fn new(source: S) -> Self {
        Self {
            source,
            max_wait: DEFAULT_MAX_WAIT,
        }
    }

    /// Caps how long a partial batch waits for more deliveries after its first one.
    #[must_use]
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }
}

// A decorator wraps a source the mount site already has; it is never the answer to "which source
// does this broker take", so it stays out of the list a failed obligation suggests.
#[diagnostic::do_not_recommend]
impl<C, S> SubscriptionSource<C> for Buffered<S>
where
    C: ConnectedBroker,
    // `Sync` on the wrapped source is what lets the redelivery address be asked for by reference.
    S: SubscriptionSource<C> + Send + Sync,
    S::Subscriber: Send,
{
    type Subscriber = BufferedSubscriber<S::Subscriber>;
    type Copies = S::Copies;

    fn name(&self) -> &str {
        self.source.name()
    }

    /// Batching happens on the client, so the declaration reaches the wrapped source unchanged.
    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.source = self.source.declare_retry(declaration);
        self
    }

    /// The same for the half of it the broker takes: batching is this process's business.
    fn declare_retry_on(
        &self,
        connected: &C,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        self.source.declare_retry_on(connected, declaration)
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(BufferedSubscriber {
            inner: self.source.subscribe(connected).await?,
            max_wait: self.max_wait,
        })
    }
}

/// Batching happens on the client, so a retry copy goes where the wrapped source says it does.
impl<C, S> RedeliveryAddressed<C> for Buffered<S>
where
    Self: SubscriptionSource<C, Copies = AddressedCopies>,
    C: ConnectedBroker,
    S: RedeliveryAddressed<C> + Send + Sync,
    S::Subscriber: Send,
{
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> {
        self.source.redelivery_address(connected)
    }
}

/// The subscriber [`Buffered`] opens: the wrapped source's subscriber plus client-side batching.
///
/// As a plain [`Subscriber`] it forwards to the wrapped subscriber unchanged; as a
/// [`BatchSubscriber`] it assembles batches by the size it is asked for and its own deadline.
///
/// A broker crate can also build one around a subscriber it already has, which is how a
/// transport with no native batches implements the capability:
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::time::Duration;
///
/// use futures::Stream;
/// use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
///
/// /// The broker's own subscriber: one message at a time on the wire, batches on the client.
/// struct Batching<S>(BufferedSubscriber<S>);
///
/// impl<S: Subscriber> Batching<S> {
///     /// The deadline is the broker's own choice; the batch size arrives per subscription.
///     fn new(wire: S) -> Self {
///         Self(BufferedSubscriber::new(wire).max_wait(Duration::from_millis(20)))
///     }
/// }
///
/// impl<S: Subscriber> Subscriber for Batching<S> {
///     type Message = S::Message;
///     type Error = S::Error;
///
///     fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
///         self.0.stream()
///     }
/// }
///
/// impl<S: Subscriber> BatchSubscriber for Batching<S> {
///     type Batch = Vec<S::Message>;
///
///     fn batches(
///         &mut self,
///         size: NonZeroUsize,
///     ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
///         self.0.batches(size)
///     }
/// }
/// ```
#[derive(Debug)]
pub struct BufferedSubscriber<S> {
    inner: S,
    max_wait: Duration,
}

impl<S> BufferedSubscriber<S> {
    /// Batches `inner` on the client, with the default deadline.
    ///
    /// The entry point for a broker crate that already holds its subscriber; a broker wrapping
    /// the whole subscription goes through [`Buffered`] instead.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            max_wait: DEFAULT_MAX_WAIT,
        }
    }

    /// Caps how long a partial batch waits for more deliveries after its first one.
    #[must_use]
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }
}

impl<S: Subscriber> Subscriber for BufferedSubscriber<S> {
    type Message = S::Message;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

/// Buffering does not move the subscription: the seeker is the wrapped subscriber's own, so a
/// batch subscription over a broker that batches only through this wrapper still opens at a chosen
/// position (`.start_at(..)`) and still repositions from a handler.
///
/// The handle is minted before the stream is opened, as [`Seekable`] requires, and the wrapped
/// subscriber applies the reposition where it always did - underneath the buffer. A batch being
/// assembled when a seek lands keeps the deliveries it already collected: they were pulled
/// before the seek, and the buffer holds them for the batch they belong to.
impl<S: Seekable> Seekable for BufferedSubscriber<S> {
    type Seeker = S::Seeker;

    fn seeker(&self) -> S::Seeker {
        self.inner.seeker()
    }
}

/// What interrupted filling a batch; resolved on the next fill.
enum Carry<E> {
    Nothing,
    Error(E),
    Ended,
}

/// The batch stream of a [`BufferedSubscriber`]: pulls the wrapped subscriber's deliveries into
/// the slot until it holds the size, the deadline that opened with its first delivery passes, or
/// the wrapped stream fails or ends.
///
/// The partial batch lives in the slot the runtime lends and the deadline in this struct, so a
/// cancelled poll loses nothing. The timer is allocated once, with the stream.
struct Filling<Inner, Error> {
    stream: Pin<Box<Inner>>,
    size: usize,
    max_wait: Duration,
    /// Armed while a batch is open with its first delivery in hand and room left.
    deadline: Option<Pin<Box<Sleep>>>,
    open: bool,
    carry: Carry<Error>,
}

// Everything held is behind a box or plain data; nothing is pinned in place.
impl<Inner, Error> Unpin for Filling<Inner, Error> {}

impl<Inner, Message, Error> BatchStream<Vec<Message>> for Filling<Inner, Error>
where
    Inner: Stream<Item = Result<Message, Error>>,
{
    type Error = Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        slot: &mut Vec<Message>,
    ) -> Poll<Option<Result<(), Error>>> {
        let this = self.get_mut();
        // An error or end-of-stream observed while filling the previous batch could not be
        // reported then (the batch itself had to go out first); report it now.
        match mem::replace(&mut this.carry, Carry::Nothing) {
            Carry::Error(err) => return Poll::Ready(Some(Err(err))),
            Carry::Ended => return Poll::Ready(None),
            Carry::Nothing => {}
        }
        if !this.open {
            match ready!(this.stream.as_mut().poll_next(cx)) {
                None => return Poll::Ready(None),
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                Some(Ok(first)) => slot.push(first),
            }
            this.open = true;
            if this.size > 1 {
                let due = Instant::now() + this.max_wait;
                match this.deadline.as_mut() {
                    Some(deadline) => deadline.as_mut().reset(due),
                    None => this.deadline = Some(Box::pin(time::sleep_until(due))),
                }
            }
        }
        while slot.len() < this.size {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(next))) => slot.push(next),
                Poll::Ready(Some(Err(err))) => {
                    this.carry = Carry::Error(err);
                    break;
                }
                Poll::Ready(None) => {
                    this.carry = Carry::Ended;
                    break;
                }
                Poll::Pending => {
                    let expired = this
                        .deadline
                        .as_mut()
                        .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready());
                    if !expired {
                        return Poll::Pending;
                    }
                    break;
                }
            }
        }
        this.open = false;
        Poll::Ready(Some(Ok(())))
    }
}

impl<S: Subscriber> BatchSubscriber for BufferedSubscriber<S> {
    type Batch = Vec<S::Message>;

    /// # Cancel safety
    ///
    /// Cancel-safe between polls, like [`Subscriber::stream`]: a partially filled batch lives
    /// in the slot the runtime lends and survives a cancelled poll (for example in `select!`).
    /// Dropping the whole stream abandons the partial batch's deliveries unacknowledged, subject
    /// to the broker's redelivery policy.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Self::Batch, Error = <Self as Subscriber>::Error> + Send + '_ {
        Filling {
            stream: Box::pin(self.inner.stream()),
            size: size.get(),
            max_wait: self.max_wait,
            deadline: None,
            open: false,
            carry: Carry::Nothing,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, ready};
    use std::sync::LazyLock;

    use futures::{StreamExt, stream};

    use super::*;
    use crate::{IncomingMessage, Settle};

    /// The batch size the checks below open their stream at, spelled once.
    fn batch(size: usize) -> NonZeroUsize {
        NonZeroUsize::new(size).expect("test sizes are nonzero")
    }

    #[derive(Debug, thiserror::Error)]
    #[error("subscriber stream failed")]
    struct StreamFault;

    struct Frame(Vec<u8>);

    impl IncomingMessage for Frame {
        fn payload(&self) -> &[u8] {
            &self.0
        }

        fn headers(&self) -> &crate::HeaderMap {
            static EMPTY: LazyLock<crate::HeaderMap> = LazyLock::new(crate::HeaderMap::new);
            &EMPTY
        }
    }

    impl Settle for Frame {
        fn ack(self) -> impl Future<Output = Result<(), crate::AckError>> {
            ready(Ok(()))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), crate::AckError>> {
            ready(Ok(()))
        }
    }

    /// Replays a fixed script, so a test can place a fault or the end of the stream mid-batch.
    struct ScriptedSubscriber(Vec<Result<Frame, StreamFault>>);

    impl Subscriber for ScriptedSubscriber {
        type Message = Frame;
        type Error = StreamFault;

        fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
            stream::iter(mem::take(&mut self.0))
        }
    }

    fn scripted(script: Vec<Result<Frame, StreamFault>>) -> BufferedSubscriber<ScriptedSubscriber> {
        // The deadline is far away, so only a fault or the end of the stream closes a batch here
        // (the checks below open their streams at a size larger than any script).
        BufferedSubscriber::new(ScriptedSubscriber(script)).max_wait(Duration::from_secs(60))
    }

    /// Fills the lent slot once, as the runtime's loop does.
    async fn fill<Fill>(
        stream: &mut Pin<&mut Fill>,
        slot: &mut Vec<Frame>,
    ) -> Option<Result<(), StreamFault>>
    where
        Fill: BatchStream<Vec<Frame>, Error = StreamFault>,
    {
        poll_fn(|cx| stream.as_mut().poll_fill(cx, slot)).await
    }

    #[tokio::test(start_paused = true)]
    async fn a_fault_mid_batch_is_carried_until_after_the_batch_it_interrupted() {
        let mut sub = scripted(vec![
            Ok(Frame(b"a".to_vec())),
            Err(StreamFault),
            Ok(Frame(b"b".to_vec())),
        ]);
        let mut stream = std::pin::pin!(sub.batches(batch(8)));
        let mut slot = Vec::new();

        // The batch collected so far goes out first; the fault cannot jump ahead of it.
        fill(&mut stream, &mut slot).await.unwrap().unwrap();
        assert_eq!(slot.len(), 1);
        assert_eq!(slot[0].payload(), b"a");
        slot.clear();

        assert!(fill(&mut stream, &mut slot).await.unwrap().is_err());
        assert!(slot.is_empty());

        // Batching resumes after the fault rather than ending the subscription.
        fill(&mut stream, &mut slot).await.unwrap().unwrap();
        assert_eq!(slot[0].payload(), b"b");
        slot.clear();
        assert!(fill(&mut stream, &mut slot).await.is_none());
        assert!(slot.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_fault_as_the_first_item_is_yielded_without_a_batch() {
        let mut sub = scripted(vec![Err(StreamFault), Ok(Frame(b"a".to_vec()))]);
        let mut stream = std::pin::pin!(sub.batches(batch(8)));
        let mut slot = Vec::new();

        assert!(fill(&mut stream, &mut slot).await.unwrap().is_err());
        assert!(slot.is_empty());
        fill(&mut stream, &mut slot).await.unwrap().unwrap();
        assert_eq!(slot[0].payload(), b"a");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_ending_mid_batch_flushes_it_before_terminating() {
        let mut sub = scripted(vec![Ok(Frame(b"a".to_vec())), Ok(Frame(b"b".to_vec()))]);
        let mut stream = std::pin::pin!(sub.batches(batch(8)));
        let mut slot = Vec::new();

        // The end of the stream is carried the same way a fault is: the partial batch goes out.
        fill(&mut stream, &mut slot).await.unwrap().unwrap();
        assert_eq!(slot.len(), 2);
        slot.clear();
        assert!(fill(&mut stream, &mut slot).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_batch_closes_at_its_size_into_the_same_slot_every_time() {
        let script = (0..6u8).map(|n| Ok(Frame(vec![n]))).collect();
        let mut sub = scripted(script);
        let mut stream = std::pin::pin!(sub.batches(batch(3)));
        let mut slot = Vec::with_capacity(3);
        let storage = slot.as_ptr();

        for first in [0u8, 3] {
            fill(&mut stream, &mut slot).await.unwrap().unwrap();
            let payloads: Vec<u8> = slot.iter().map(|frame| frame.payload()[0]).collect();
            assert_eq!(payloads, [first, first + 1, first + 2]);
            assert_eq!(slot.as_ptr(), storage, "the batch grew the lent slot");
            slot.clear();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_closes_a_partial_batch_that_a_pending_poll_left_in_the_slot() {
        let mut sub = BufferedSubscriber::new(OneThenQuiet).max_wait(Duration::from_millis(10));
        let mut stream = std::pin::pin!(sub.batches(batch(8)));
        let mut slot = Vec::new();

        // The first poll takes the delivery and parks on the quiet stream with the batch open; the
        // deadline, not a second delivery, closes it.
        assert!(
            futures::poll!(poll_fn(|cx| stream.as_mut().poll_fill(cx, &mut slot))).is_pending()
        );
        assert_eq!(slot.len(), 1);
        fill(&mut stream, &mut slot).await.unwrap().unwrap();
        assert_eq!(slot.len(), 1);
    }

    /// One delivery, then silence.
    struct OneThenQuiet;

    impl Subscriber for OneThenQuiet {
        type Message = Frame;
        type Error = StreamFault;

        fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
            stream::once(ready(Ok(Frame(b"a".to_vec())))).chain(stream::pending())
        }
    }
}
