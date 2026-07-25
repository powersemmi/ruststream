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

use crate::{BatchSubscriber, Broker, Subscriber, SubscriptionSource};

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

impl<B, S> SubscriptionSource<B> for Buffered<S>
where
    B: Broker,
    S: SubscriptionSource<B> + Send,
    S::Subscriber: Send,
{
    type Subscriber = BufferedSubscriber<S::Subscriber>;

    fn name(&self) -> &str {
        self.source.name()
    }

    async fn subscribe(self, broker: &B) -> Result<Self::Subscriber, B::Error> {
        Ok(BufferedSubscriber {
            inner: self.source.subscribe(broker).await?,
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
                    let deadline = tokio::time::sleep(max_wait);
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
    use crate::memory::MemoryBroker;
    use crate::{IncomingMessage, Name, OutgoingMessage, Publisher};

    async fn buffered(
        broker: &MemoryBroker,
        max_size: usize,
        max_wait: Duration,
    ) -> BufferedSubscriber<crate::memory::MemorySubscriber> {
        Buffered::new(Name::new("buffered"))
            .max_size(NonZeroUsize::new(max_size).expect("test sizes are nonzero"))
            .max_wait(max_wait)
            .subscribe(broker)
            .await
            .unwrap()
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
