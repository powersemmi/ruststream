//! Client-side batching: buffer any [`Subscriber`] into a [`BatchSubscriber`].
//!
//! Brokers whose clients batch on the wire implement [`BatchSubscriber`] natively. [`Buffered`]
//! gives every other broker the same consumption shape by buffering single deliveries on the
//! client, bounded by batch size and wait time. A blanket implementation is impossible without
//! robbing broker crates of their native implementations (coherence), hence the explicit
//! wrapper.

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time::sleep;

use crate::{BatchSubscriber, ConnectedBroker, Subscriber, SubscriptionSource};

const DEFAULT_MAX_SIZE: NonZeroUsize = NonZeroUsize::new(64).unwrap();
const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(10);

/// A [`SubscriptionSource`] adapter that buffers the wrapped source's subscriber into a
/// [`BatchSubscriber`].
///
/// A batch closes when it holds [`max_size`](Self::max_size) deliveries, or once
/// [`max_wait`](Self::max_wait) has elapsed after its first delivery, whichever comes first; an
/// idle subscription waits indefinitely for that first delivery. Defaults: 64 deliveries, 10 ms.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::{Buffered, Name, nonzero};
///
/// let source = Buffered::new(Name::new("orders"))
///     .max_size(nonzero!(128))
///     .max_wait(Duration::from_millis(20));
/// # let _ = source;
/// ```
#[derive(Debug, Clone)]
pub struct Buffered<S> {
    source: S,
    max_size: NonZeroUsize,
    max_wait: Duration,
}

impl<S> Buffered<S> {
    /// Wraps `source`, batching its subscriber with the default size and wait bounds.
    #[must_use]
    pub fn new(source: S) -> Self {
        Self {
            source,
            max_size: DEFAULT_MAX_SIZE,
            max_wait: DEFAULT_MAX_WAIT,
        }
    }

    /// Caps how many deliveries one batch may carry.
    #[must_use]
    pub fn max_size(mut self, max_size: NonZeroUsize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Caps how long a partial batch waits for more deliveries after its first one.
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
            max_size: self.max_size,
            max_wait: self.max_wait,
        })
    }
}

/// The subscriber [`Buffered`] opens: the wrapped source's subscriber plus client-side batching.
///
/// As a plain [`Subscriber`] it forwards to the wrapped subscriber unchanged; as a
/// [`BatchSubscriber`] it assembles batches by size and deadline.
#[derive(Debug)]
pub struct BufferedSubscriber<S> {
    inner: S,
    max_size: NonZeroUsize,
    max_wait: Duration,
}

impl<S: Subscriber> Subscriber for BufferedSubscriber<S> {
    type Message = S::Message;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
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
    /// Cancel-safe between polls, like [`Subscriber::stream`]: a partially filled batch lives
    /// inside the stream and survives a cancelled poll (for example in `select!`). Dropping the
    /// whole stream abandons the partial batch's deliveries unacknowledged, subject to the
    /// broker's redelivery policy.
    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let max_size = self.max_size.get();
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
    use futures::StreamExt;

    use super::*;
    use crate::memory::{MemoryBroker, MemorySubscriber};
    use crate::{Broker, IncomingMessage, Name, OutgoingMessage, Publisher};

    async fn buffered(
        broker: &MemoryBroker,
        max_size: usize,
        max_wait: Duration,
    ) -> BufferedSubscriber<MemorySubscriber> {
        let connected = broker
            .clone()
            .connect()
            .await
            .expect("memory connect is infallible");
        Buffered::new(Name::new("buffered"))
            .max_size(NonZeroUsize::new(max_size).expect("test sizes are nonzero"))
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

        fn headers(&self) -> &crate::Headers {
            static EMPTY: std::sync::LazyLock<crate::Headers> =
                std::sync::LazyLock::new(crate::Headers::new);
            &EMPTY
        }

        async fn ack(self) -> Result<(), crate::AckError> {
            Ok(())
        }

        async fn nack(self, _requeue: bool) -> Result<(), crate::AckError> {
            Ok(())
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
        BufferedSubscriber {
            inner: ScriptedSubscriber(script),
            // Large enough that only a fault or the end of the stream can close a batch.
            max_size: NonZeroUsize::new(8).expect("test sizes are nonzero"),
            max_wait: Duration::from_secs(60),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_fault_mid_batch_is_carried_until_after_the_batch_it_interrupted() {
        let mut sub = scripted(vec![
            Ok(Frame(b"a".to_vec())),
            Err(StreamFault),
            Ok(Frame(b"b".to_vec())),
        ]);
        let mut stream = std::pin::pin!(sub.batches());

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
        let mut stream = std::pin::pin!(sub.batches());

        assert!(stream.next().await.unwrap().is_err());
        let recovered = stream.next().await.unwrap().unwrap();
        assert_eq!(recovered[0].payload(), b"a");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_ending_mid_batch_flushes_it_before_terminating() {
        let mut sub = scripted(vec![Ok(Frame(b"a".to_vec())), Ok(Frame(b"b".to_vec()))]);
        let mut stream = std::pin::pin!(sub.batches());

        // The end of the stream is carried the same way a fault is: the partial batch goes out.
        let flushed = stream.next().await.unwrap().unwrap();
        assert_eq!(flushed.len(), 2);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn size_cap_closes_the_batch() {
        let broker = MemoryBroker::new();
        let mut sub = buffered(&broker, 2, Duration::from_secs(60)).await;
        let publisher = broker.publisher();
        for i in 0..4u8 {
            publisher
                .publish(OutgoingMessage::new("buffered", &[i]))
                .await
                .unwrap();
        }

        // The wait bound is far away; only the size cap can close these batches.
        let mut stream = std::pin::pin!(sub.batches());
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
        let mut sub = buffered(&broker, 64, Duration::from_millis(10)).await;
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("buffered", b"only".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.batches());
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload(), b"only");
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn plain_stream_passes_through() {
        let broker = MemoryBroker::new();
        let mut sub = buffered(&broker, 8, Duration::from_millis(10)).await;
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("buffered", b"single".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"single");
        msg.ack().await.unwrap();
    }
}
