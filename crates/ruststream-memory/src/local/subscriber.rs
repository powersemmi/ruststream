//! The subscription descriptor of the local bus, its subscriber and one delivery.

use alloc::borrow::Cow;
use alloc::rc::Rc;
use core::convert::Infallible;
use core::fmt;
use core::future::ready;
use core::mem;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use ruststream_core::{
    AckError, AddressedCopies, FromName, HeaderMap, LocalBatchSubscriber, LocalIncomingMessage,
    LocalRedeliveryAddressed, LocalSubscriber, LocalSubscriptionSource, RedeliveryAddress,
};
use ruststream_runtime::IntoSource;

use super::alloc_prelude::*;
use super::bus::{Bus, Delivery, Subscription};
use super::{ConnectedLocalMemoryBroker, LocalMemoryError};

/// The subscription descriptor of the local bus: a subscription to one name.
///
/// It resolves at startup against the connected bus. A publisher reaches the subscription by the
/// same name, which is also where the runtime publishes its retry copies.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// The logger reads every reading the sensor task publishes.
/// async fn logger(connected: &ConnectedLocalMemoryBroker<4>) -> Result<u8, LocalMemoryError> {
///     let mut readings = LocalMemorySource::new("sensors/temp")
///         .subscribe(connected)
///         .await?;
///     connected
///         .publisher()
///         .publish(OutgoingMessage::produced("sensors/temp", BytesMut::from(&[21u8][..])), None)
///         .await?;
///     let mut deliveries = core::pin::pin!(readings.stream());
///     let Some(Ok(reading)) = deliveries.next().await else {
///         unreachable!("the bus is open and its stream yields no error");
///     };
///     let celsius = reading.payload()[0];
///     reading.ack().await.expect("the bus settles");
///     Ok(celsius)
/// }
///
/// let celsius = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
///     logger(&connected).await
/// });
/// assert_eq!(celsius, Ok(21));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMemorySource {
    name: String,
}

impl LocalMemorySource {
    /// A subscription to `name`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// /// The descriptors of the firmware's subscriptions, declared before the bus connects.
    /// fn subscriptions() -> [LocalMemorySource; 2] {
    ///     [LocalMemorySource::new("sensors/temp"), LocalMemorySource::new("actuator/cmd")]
    /// }
    ///
    /// let opened = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
    ///     let [readings, commands] = subscriptions();
    ///     readings.subscribe(&connected).await?;
    ///     commands.subscribe(&connected).await?;
    ///     connected.shutdown().await
    /// })?;
    /// assert_eq!(opened.subscriptions_closed(), 0);
    /// # Ok::<(), LocalMemoryError>(())
    /// ```
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

// The local subscription needs nothing beyond a name, so it offers the name-only constructor the
// `#[subscriber(LocalMemorySource)]` form builds through.
impl FromName for LocalMemorySource {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

// The manual subscriber definition takes the descriptor as its source unchanged.
impl IntoSource for LocalMemorySource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<const CAP: usize> LocalSubscriptionSource<ConnectedLocalMemoryBroker<CAP>>
    for LocalMemorySource
{
    type Subscriber = LocalMemorySubscriber<CAP>;
    // The bus moves nothing on its own, and one name is both ends of it, so a copy goes back to
    // the name the subscription reads.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<CAP>,
    ) -> impl Future<Output = Result<Self::Subscriber, LocalMemoryError>> {
        ready(connected.open(&self.name))
    }
}

impl<const CAP: usize> LocalRedeliveryAddressed<ConnectedLocalMemoryBroker<CAP>>
    for LocalMemorySource
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedLocalMemoryBroker<CAP>,
    ) -> impl Future<Output = Result<RedeliveryAddress, LocalMemoryError>> {
        // One name is both ends of the bus, and no lookup is needed to say so.
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

/// A subscription to one name on the local bus: the stream of its deliveries.
///
/// Dropping it closes the subscription: what waits in it is released, and publishes to its name
/// no longer reach it.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// The command loop of an actuator task: it ends when the bus shuts down.
/// async fn commands(mut subscriber: LocalMemorySubscriber<4>) -> usize {
///     let mut handled = 0;
///     let mut deliveries = core::pin::pin!(subscriber.stream());
///     while let Some(Ok(command)) = deliveries.next().await {
///         command.ack().await.expect("the bus settles");
///         handled += 1;
///     }
///     handled
/// }
///
/// let handled = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
///     let subscriber = connected.subscribe("actuator/cmd").await?;
///     connected
///         .publisher()
///         .publish(OutgoingMessage::produced("actuator/cmd", BytesMut::from("open")), None)
///         .await?;
///     connected.shutdown().await?;
///     Ok::<_, LocalMemoryError>(commands(subscriber).await)
/// })?;
/// assert_eq!(handled, 1);
/// # Ok::<(), LocalMemoryError>(())
/// ```
pub struct LocalMemorySubscriber<const CAP: usize> {
    subscription: Rc<Subscription<CAP>>,
    bus: Rc<Bus<CAP>>,
}

impl<const CAP: usize> LocalMemorySubscriber<CAP> {
    pub(super) const fn new(subscription: Rc<Subscription<CAP>>, bus: Rc<Bus<CAP>>) -> Self {
        Self { subscription, bus }
    }

    fn message(&self, delivery: Delivery) -> LocalMemoryMessage<CAP> {
        LocalMemoryMessage {
            delivery,
            settled: false,
            subscription: Rc::clone(&self.subscription),
        }
    }
}

impl<const CAP: usize> Drop for LocalMemorySubscriber<CAP> {
    fn drop(&mut self) {
        self.bus.unsubscribe(&self.subscription);
    }
}

impl<const CAP: usize> fmt::Debug for LocalMemorySubscriber<CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemorySubscriber")
            .field("name", &self.subscription.name())
            .finish_non_exhaustive()
    }
}

impl<const CAP: usize> LocalSubscriber for LocalMemorySubscriber<CAP> {
    type Message = LocalMemoryMessage<CAP>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + '_ {
        Deliveries { subscriber: self }
    }
}

impl<const CAP: usize> LocalBatchSubscriber for LocalMemorySubscriber<CAP> {
    type Batch = Vec<LocalMemoryMessage<CAP>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + '_ {
        Batches {
            subscriber: self,
            size,
        }
    }
}

/// The stream of one subscription. It reads the subscription in place, so a stream dropped
/// between polls loses nothing and the subscriber opens another.
struct Deliveries<'a, const CAP: usize> {
    subscriber: &'a LocalMemorySubscriber<CAP>,
}

impl<const CAP: usize> Stream for Deliveries<'_, CAP> {
    type Item = Result<LocalMemoryMessage<CAP>, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let subscriber = self.subscriber;
        subscriber
            .subscription
            .poll_next(cx)
            .map(|delivery| delivery.map(|delivery| Ok(subscriber.message(delivery))))
    }
}

/// The batch stream of one subscription: it waits for the first delivery and takes what else
/// already waits, up to the batch size, in the same poll.
struct Batches<'a, const CAP: usize> {
    subscriber: &'a LocalMemorySubscriber<CAP>,
    size: NonZeroUsize,
}

impl<const CAP: usize> Stream for Batches<'_, CAP> {
    type Item = Result<Vec<LocalMemoryMessage<CAP>>, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let subscriber = self.subscriber;
        let Poll::Ready(first) = subscriber.subscription.poll_next(cx) else {
            return Poll::Pending;
        };
        let Some(first) = first else {
            return Poll::Ready(None);
        };
        // At most the capacity waits in the queue, so a batch never grows past it.
        let mut batch = Vec::with_capacity(self.size.get().min(CAP));
        batch.push(subscriber.message(first));
        while batch.len() < self.size.get() {
            let Some(next) = subscriber.subscription.try_next() else {
                break;
            };
            batch.push(subscriber.message(next));
        }
        Poll::Ready(Some(Ok(batch)))
    }
}

/// One delivery of the local bus.
///
/// `ack` consumes it, `nack(true)` puts it back at the end of its subscription's queue with its
/// delivery count raised, `nack(false)` drops it. A delivery dropped unsettled comes back as a
/// `nack(true)` would bring it. A requeue after the bus shut down is refused with an error.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// A command the actuator cannot run yet goes back to the queue, and comes back counted.
/// async fn retry_once(connected: &ConnectedLocalMemoryBroker<4>) -> Option<u64> {
///     let mut commands = connected.subscribe("actuator/cmd").await.ok()?;
///     connected
///         .publisher()
///         .publish(OutgoingMessage::produced("actuator/cmd", BytesMut::from("open")), None)
///         .await
///         .ok()?;
///     let mut deliveries = core::pin::pin!(commands.stream());
///     let busy = deliveries.next().await?.ok()?;
///     busy.nack(true).await.ok()?;
///     let again = deliveries.next().await?.ok()?;
///     let count = again.redelivery_count();
///     again.ack().await.ok()?;
///     count
/// }
///
/// let count = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::<4>::new().connect().await.ok()?;
///     retry_once(&connected).await
/// });
/// assert_eq!(count, Some(2));
/// ```
pub struct LocalMemoryMessage<const CAP: usize> {
    delivery: Delivery,
    // Set by a settlement, so the drop that follows it releases the place without a requeue.
    settled: bool,
    subscription: Rc<Subscription<CAP>>,
}

impl<const CAP: usize> LocalMemoryMessage<CAP> {
    /// The name this delivery was published to: the name of its subscription.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures::StreamExt;
    /// use ruststream_memory::local::prelude::*;
    ///
    /// /// The logger tags each line with the name the reading came from.
    /// fn line(reading: &LocalMemoryMessage<4>) -> String {
    ///     format!("{}: {:?}", reading.name(), reading.payload())
    /// }
    ///
    /// let logged = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::<4>::new().connect().await.ok()?;
    ///     let mut readings = connected.subscribe("sensors/temp").await.ok()?;
    ///     let reading = OutgoingMessage::produced("sensors/temp", BytesMut::from(&[21u8][..]));
    ///     connected.publisher().publish(reading, None).await.ok()?;
    ///     let mut deliveries = core::pin::pin!(readings.stream());
    ///     let reading = deliveries.next().await?.ok()?;
    ///     Some(line(&reading))
    /// });
    /// assert_eq!(logged.as_deref(), Some("sensors/temp: [21]"));
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        self.subscription.name()
    }

    /// Puts the delivery back at the end of its subscription's queue, counted as one more
    /// delivery, and marks this one settled.
    fn requeue(&mut self) -> Result<(), LocalMemoryError> {
        self.settled = true;
        let delivery = Delivery {
            payload: mem::take(&mut self.delivery.payload),
            headers: mem::take(&mut self.delivery.headers),
            deliveries: self.delivery.deliveries.saturating_add(1),
        };
        self.subscription.requeue(delivery)
    }
}

impl<const CAP: usize> Drop for LocalMemoryMessage<CAP> {
    fn drop(&mut self) {
        if !self.settled {
            // A delivery nobody settled comes back, as a server redelivers what a consumer
            // dropped; after a shutdown there is nowhere to bring it back to.
            let _ = self.requeue();
        }
        self.subscription.settle();
    }
}

impl<const CAP: usize> fmt::Debug for LocalMemoryMessage<CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryMessage")
            .field("name", &self.name())
            .field("deliveries", &self.delivery.deliveries)
            .finish_non_exhaustive()
    }
}

impl<const CAP: usize> LocalIncomingMessage for LocalMemoryMessage<CAP> {
    fn payload(&self) -> &[u8] {
        &self.delivery.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.delivery.headers
    }

    fn redelivery_count(&self) -> Option<u64> {
        Some(self.delivery.deliveries.get())
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.settled = true;
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        if !requeue {
            self.settled = true;
            return ready(Ok(()));
        }
        ready(
            self.requeue()
                .map_err(|refused| AckError::Broker(Box::new(refused))),
        )
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Wake;

    use embassy_futures::block_on;
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{
        BytesMut, LocalBroker, LocalConnectedBroker, LocalPublisher, LocalSubscribe,
        OutgoingMessage,
    };

    use super::*;
    use crate::local::LocalMemoryBroker;
    use crate::local::drive::{ended, waiting};

    async fn open(name: &str) -> (ConnectedLocalMemoryBroker<4>, LocalMemorySubscriber<4>) {
        let connected = LocalMemoryBroker::<4>::new().connect().await.unwrap();
        let subscriber = connected.subscribe(name).await.unwrap();
        (connected, subscriber)
    }

    async fn publish(connected: &ConnectedLocalMemoryBroker<4>, name: &str, payload: &[u8]) {
        let msg = OutgoingMessage::produced(name, BytesMut::from(payload));
        connected.publisher().publish(msg, None).await.unwrap();
    }

    #[test]
    fn nack_with_requeue_brings_the_delivery_back_counted() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            publish(&connected, "cmd", b"open").await;
            let mut stream = pin!(subscriber.stream());
            let first = waiting(&mut stream).unwrap();
            assert_eq!(first.redelivery_count(), Some(1));
            first.nack(true).await.unwrap();

            let again = waiting(&mut stream).unwrap();
            assert_eq!(again.payload(), b"open");
            assert_eq!(again.redelivery_count(), Some(2));
            again.ack().await.unwrap();
            assert!(stream.next().now_or_never().is_none());
        });
    }

    #[test]
    fn nack_without_requeue_drops_the_delivery() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            publish(&connected, "cmd", b"open").await;
            let mut stream = pin!(subscriber.stream());
            waiting(&mut stream).unwrap().nack(false).await.unwrap();
            assert!(stream.next().now_or_never().is_none());
        });
    }

    #[test]
    fn a_delivery_dropped_unsettled_comes_back() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            publish(&connected, "cmd", b"open").await;
            let mut stream = pin!(subscriber.stream());
            drop(waiting(&mut stream).unwrap());
            let again = waiting(&mut stream).unwrap();
            assert_eq!(again.redelivery_count(), Some(2));
            again.ack().await.unwrap();
        });
    }

    #[test]
    fn a_requeue_after_shutdown_is_refused() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            publish(&connected, "cmd", b"open").await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            connected.shutdown().await.unwrap();
            assert!(matches!(
                delivery.nack(true).await,
                Err(AckError::Broker(refused)) if refused.to_string().contains("shut down")
            ));
        });
    }

    #[test]
    fn headers_reach_the_subscriber_unchanged() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            let mut headers = HeaderMap::new();
            headers.insert("x-origin", "panel");
            let msg =
                OutgoingMessage::produced("cmd", BytesMut::from("open")).with_headers(headers);
            connected.publisher().publish(msg, None).await.unwrap();
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(
                delivery.headers().get("x-origin"),
                Some(b"panel".as_slice())
            );
            assert_eq!(delivery.name(), "cmd");
            delivery.ack().await.unwrap();
        });
    }

    #[test]
    fn a_batch_takes_what_waits_up_to_its_size() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            for payload in [b"a", b"b", b"c"] {
                publish(&connected, "cmd", payload).await;
            }
            let mut batches = pin!(subscriber.batches(NonZeroUsize::new(2).unwrap()));
            let first = waiting(&mut batches).unwrap();
            let second = waiting(&mut batches).unwrap();
            let sizes = [first.len(), second.len()];
            for delivery in first.into_iter().chain(second) {
                delivery.ack().await.unwrap();
            }
            assert_eq!(sizes, [2, 1]);
            connected.shutdown().await.unwrap();
            assert!(ended(&mut batches));
        });
    }

    struct Counting(AtomicUsize);

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_shutdown_wakes_a_reader_waiting_on_an_empty_subscription() {
        block_on(async {
            let (connected, mut subscriber) = open("cmd").await;
            let counted = Arc::new(Counting(AtomicUsize::new(0)));
            let waker = Arc::clone(&counted).into();
            let mut cx = Context::from_waker(&waker);
            let mut stream = pin!(subscriber.stream());
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());

            connected.shutdown().await.unwrap();
            assert_eq!(counted.0.load(Ordering::Relaxed), 1);
            assert!(matches!(stream.poll_next(&mut cx), Poll::Ready(None)));
        });
    }
}
