//! Client-side paging: turn any [`Subscriber`] into a [`BatchSubscriber`].
//!
//! This is broker-author machinery. Every broker offers [`BatchSubscriber`], because every page
//! handler asks for one; a broker whose client batches on the wire implements it directly, and a
//! broker whose transport delivers one message at a time gives its subscriber the capability
//! through [`BufferedSubscriber`], which assembles the pages on the client. The wrapper is
//! explicit: a blanket impl would collide with the native ones broker crates write.
//!
//! A service never names it. The page size is the registration's
//! [`batch(n)`](crate::runtime::SubscriberSettings::batch) and arrives as the argument of
//! [`BatchSubscriber::batches`]; what stays the adapter's own is the deadline that closes a
//! partial page.

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time::sleep;

use crate::{BatchSubscriber, ConnectedBroker, Seekable, Subscriber, SubscriptionSource};

const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(10);

/// A [`SubscriptionSource`] adapter that buffers the wrapped source's subscriber into a
/// [`BatchSubscriber`].
///
/// A page closes when it holds the size the registration asked for, or once
/// [`max_wait`](Self::max_wait) has elapsed after its first delivery, whichever comes first; an
/// idle subscription waits indefinitely for that first delivery. The default deadline is 10 ms.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::{Buffered, Name};
///
/// // What a broker crate wraps its own source in, so its subscriber pages on the client.
/// let source = Buffered::new(Name::new("orders")).max_wait(Duration::from_millis(20));
/// # let _ = source;
/// ```
#[derive(Debug, Clone)]
pub struct Buffered<S> {
    source: S,
    max_wait: Duration,
}

impl<S> Buffered<S> {
    /// Wraps `source`, paging its subscriber with the default deadline.
    #[must_use]
    pub fn new(source: S) -> Self {
        Self {
            source,
            max_wait: DEFAULT_MAX_WAIT,
        }
    }

    /// Caps how long a partial page waits for more deliveries after its first one.
    #[must_use]
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }
}

impl<C, S> SubscriptionSource<C> for Buffered<S>
where
    C: ConnectedBroker,
    S: SubscriptionSource<C> + Send,
    S::Subscriber: Send,
{
    type Subscriber = BufferedSubscriber<S::Subscriber>;

    fn name(&self) -> &str {
        self.source.name()
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(BufferedSubscriber {
            inner: self.source.subscribe(connected).await?,
            max_wait: self.max_wait,
        })
    }
}

/// The subscriber [`Buffered`] opens: the wrapped source's subscriber plus client-side paging.
///
/// As a plain [`Subscriber`] it forwards to the wrapped subscriber unchanged; as a
/// [`BatchSubscriber`] it assembles pages by the size it is asked for and its own deadline.
///
/// A broker crate can also build one around a subscriber it already has, which is how a
/// transport with no native pages implements the capability:
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::time::Duration;
///
/// use futures::Stream;
/// use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
///
/// /// The broker's own subscriber: one message at a time on the wire, pages on the client.
/// struct Paged<S>(BufferedSubscriber<S>);
///
/// impl<S: Subscriber> Paged<S> {
///     /// The deadline is the broker's own choice; the page size arrives per subscription.
///     fn new(wire: S) -> Self {
///         Self(BufferedSubscriber::new(wire).max_wait(Duration::from_millis(20)))
///     }
/// }
///
/// impl<S: Subscriber> Subscriber for Paged<S> {
///     type Message = S::Message;
///     type Error = S::Error;
///
///     fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
///         self.0.stream()
///     }
/// }
///
/// impl<S: Subscriber> BatchSubscriber for Paged<S> {
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
    /// Pages `inner` on the client, with the default deadline.
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

    /// Caps how long a partial page waits for more deliveries after its first one.
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
/// page subscription over a broker that pages only through this wrapper still opens at a chosen
/// position (`.start_at(..)`) and still repositions from a handler.
///
/// The handle is minted before the stream is opened, as [`Seekable`] requires, and the wrapped
/// subscriber applies the reposition where it always did - underneath the buffer. A page being
/// assembled when a seek lands keeps the deliveries it already collected: they were pulled
/// before the seek, and the buffer holds them for the page they belong to.
impl<S: Seekable> Seekable for BufferedSubscriber<S> {
    type Seeker = S::Seeker;

    fn seeker(&self) -> S::Seeker {
        self.inner.seeker()
    }
}

/// What interrupted filling a batch; resolved on the next pull from the stream.
enum Carry<E> {
    Nothing,
    Error(E),
    Ended,
}

impl<S: Subscriber> BatchSubscriber for BufferedSubscriber<S> {
    type Batch = Vec<S::Message>;

    /// # Cancel safety
    ///
    /// Cancel-safe between polls, like [`Subscriber::stream`]: a partially filled page lives
    /// inside the stream and survives a cancelled poll (for example in `select!`). Dropping the
    /// whole stream abandons the partial page's deliveries unacknowledged, subject to the
    /// broker's redelivery policy.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let max_size = size.get();
        let max_wait = self.max_wait;
        let inner = Box::pin(self.inner.stream());
        futures::stream::unfold(
            (inner, Carry::Nothing),
            move |(mut stream, carry)| async move {
                // An error or end-of-stream observed while filling the previous batch could not be
                // yielded then (the batch itself had to go out first); deliver it now.
                match carry {
                    Carry::Error(err) => return Some((Err(err), (stream, Carry::Nothing))),
                    Carry::Ended => return None,
                    Carry::Nothing => {}
                }
                let first = match stream.next().await? {
                    Ok(msg) => msg,
                    Err(err) => return Some((Err(err), (stream, Carry::Nothing))),
                };
                let mut batch = Vec::with_capacity(max_size.min(64));
                batch.push(first);
                let mut carry = Carry::Nothing;
                if max_size > 1 {
                    let deadline = sleep(max_wait);
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            () = &mut deadline => break,
                            next = stream.next() => match next {
                                Some(Ok(msg)) => {
                                    batch.push(msg);
                                    if batch.len() >= max_size {
                                        break;
                                    }
                                }
                                Some(Err(err)) => {
                                    carry = Carry::Error(err);
                                    break;
                                }
                                None => {
                                    carry = Carry::Ended;
                                    break;
                                }
                            }
                        }
                    }
                }
                Some((Ok(batch), (stream, carry)))
            },
        )
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::future::ready;

    use futures::StreamExt;

    use super::*;
    use crate::memory::{MemoryBroker, MemorySubscriber};
    use crate::{Broker, IncomingMessage, Name, OutgoingMessage, Publisher};

    /// The page size the checks below open their stream at, spelled once.
    fn page(size: usize) -> NonZeroUsize {
        NonZeroUsize::new(size).expect("test sizes are nonzero")
    }

    async fn buffered(
        broker: &MemoryBroker,
        max_wait: Duration,
    ) -> BufferedSubscriber<MemorySubscriber> {
        let connected = broker
            .clone()
            .connect()
            .await
            .expect("memory connect is infallible");
        Buffered::new(Name::new("buffered"))
            .max_wait(max_wait)
            .subscribe(&connected)
            .await
            .unwrap()
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
            static EMPTY: std::sync::LazyLock<crate::HeaderMap> =
                std::sync::LazyLock::new(crate::HeaderMap::new);
            &EMPTY
        }

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
            futures::stream::iter(std::mem::take(&mut self.0))
        }
    }

    fn scripted(script: Vec<Result<Frame, StreamFault>>) -> BufferedSubscriber<ScriptedSubscriber> {
        // The deadline is far away, so only a fault or the end of the stream closes a page here
        // (the checks below open their streams at a size larger than any script).
        BufferedSubscriber::new(ScriptedSubscriber(script)).max_wait(Duration::from_secs(60))
    }

    #[tokio::test(start_paused = true)]
    async fn a_fault_mid_batch_is_carried_until_after_the_batch_it_interrupted() {
        let mut sub = scripted(vec![
            Ok(Frame(b"a".to_vec())),
            Err(StreamFault),
            Ok(Frame(b"b".to_vec())),
        ]);
        let mut stream = std::pin::pin!(sub.batches(page(8)));

        // The batch collected so far goes out first; the fault cannot jump ahead of it.
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].payload(), b"a");

        assert!(stream.next().await.unwrap().is_err());

        // Batching resumes after the fault rather than ending the subscription.
        let resumed = stream.next().await.unwrap().unwrap();
        assert_eq!(resumed[0].payload(), b"b");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_fault_as_the_first_item_is_yielded_without_a_batch() {
        let mut sub = scripted(vec![Err(StreamFault), Ok(Frame(b"a".to_vec()))]);
        let mut stream = std::pin::pin!(sub.batches(page(8)));

        assert!(stream.next().await.unwrap().is_err());
        let recovered = stream.next().await.unwrap().unwrap();
        assert_eq!(recovered[0].payload(), b"a");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_ending_mid_batch_flushes_it_before_terminating() {
        let mut sub = scripted(vec![Ok(Frame(b"a".to_vec())), Ok(Frame(b"b".to_vec()))]);
        let mut stream = std::pin::pin!(sub.batches(page(8)));

        // The end of the stream is carried the same way a fault is: the partial batch goes out.
        let flushed = stream.next().await.unwrap().unwrap();
        assert_eq!(flushed.len(), 2);
        assert!(stream.next().await.is_none());
    }

    /// The size the stream is opened at is what closes a full page, and it is the
    /// registration's own: the adapter has no size of its own to disagree with.
    #[tokio::test]
    async fn the_page_size_closes_a_full_page() {
        let broker = MemoryBroker::new();
        let mut sub = buffered(&broker, Duration::from_secs(60)).await;
        let publisher = broker.publisher();
        for i in 0..4u8 {
            publisher
                .publish(OutgoingMessage::new("buffered", &[i]))
                .await
                .unwrap();
        }

        // The wait bound is far away; only the size can close these pages.
        let mut stream = std::pin::pin!(sub.batches(page(2)));
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.len(), 2);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.len(), 2);
        for msg in first.into_iter().chain(second) {
            msg.ack().await.unwrap();
        }
    }

    // Paused time needs the current-thread runtime; the deadline auto-advances instead of
    // sleeping for real. Nothing spawns.
    #[tokio::test(start_paused = true)]
    async fn deadline_flushes_a_partial_batch() {
        let broker = MemoryBroker::new();
        let mut sub = buffered(&broker, Duration::from_millis(10)).await;
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("buffered", b"only"))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.batches(page(64)));
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload(), b"only");
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    /// The seeker is the wrapped subscriber's own, so a buffered subscription replays: the
    /// batches that come out after the seek are assembled from the replayed deliveries.
    #[tokio::test]
    async fn the_seeker_reaches_through_the_buffer() {
        use crate::memory::MemoryPosition;
        use crate::{Seekable, Seeker};

        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        for i in 0..2u8 {
            publisher
                .publish(OutgoingMessage::new("buffered", &[i]))
                .await
                .unwrap();
        }
        // Opened after the publishes, so only a reposition can reach them.
        let mut sub = buffered(&broker, Duration::from_millis(10)).await;
        sub.seeker()
            .seek(MemoryPosition::start())
            .await
            .expect("the in-memory log replays from the start");

        let mut stream = std::pin::pin!(sub.batches(page(8)));
        let replayed = stream.next().await.unwrap().unwrap();
        let payloads: Vec<&[u8]> = replayed.iter().map(IncomingMessage::payload).collect();
        assert_eq!(payloads, [[0].as_slice(), [1].as_slice()]);
        for msg in replayed {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn plain_stream_passes_through() {
        let broker = MemoryBroker::new();
        let mut sub = buffered(&broker, Duration::from_millis(10)).await;
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("buffered", b"single"))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"single");
        msg.ack().await.unwrap();
    }
}
