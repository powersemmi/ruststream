//! The subscription descriptor of the fixed local bus, its subscriber and one delivery.

use core::convert::Infallible;
use core::fmt;
use core::future::ready;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use heapless::Vec as FixedVec;
use ruststream_core::{
    AckError, AddressedCopies, BatchSlot, BatchStream, BatchSubscriber, BatchVec, FromName,
    Headers, IncomingMessage, ReadyHint, RedeliveryAddress, RedeliveryAddressed, Settle,
    Subscriber, SubscriptionSource,
};
use ruststream_runtime::IntoSource;

use super::bus::{Delivery, Held, LocalMemoryBus, Place, SlotName};
use super::{ConnectedLocalMemoryBroker, LocalMemoryError};

/// The subscription descriptor of the local bus: a subscription to one name.
///
/// It resolves at startup against the connected bus. A publisher reaches the subscription by the
/// same name, which is also where the runtime publishes its retry copies. The name is a literal of
/// the program, held whole.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// The logger reads every reading the sensor task publishes.
/// async fn logger(connected: &ConnectedLocalMemoryBroker<4>) -> Result<u8, LocalMemoryError> {
///     let mut readings = LocalMemorySource::new("sensors/temp")
///         .subscribe(connected)
///         .await?;
///     let celsius = &[21u8][..];
///     connected
///         .publisher()
///         .publish(OutgoingMessage::with_payload("sensors/temp", celsius), None)
///         .await
///         .map_err(PublishMessageError::into_error)?;
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
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     logger(&connected).await
/// });
/// assert_eq!(celsius, Ok(21));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalMemorySource {
    name: &'static str,
}

impl LocalMemorySource {
    /// A subscription to `name`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    ///
    /// /// The descriptors of the firmware's subscriptions, declared before the bus connects.
    /// const SUBSCRIPTIONS: [LocalMemorySource; 2] = [
    ///     LocalMemorySource::new("sensors/temp"),
    ///     LocalMemorySource::new("actuator/cmd"),
    /// ];
    ///
    /// let opened = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
    ///     let [readings, commands] = SUBSCRIPTIONS;
    ///     readings.subscribe(&connected).await?;
    ///     commands.subscribe(&connected).await?;
    ///     connected.shutdown().await
    /// })?;
    /// assert_eq!(opened.subscriptions_closed(), 0);
    /// # Ok::<(), LocalMemoryError>(())
    /// ```
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }
}

// The local subscription needs nothing beyond a name, so it offers the name-only constructor the
// `#[subscriber(LocalMemorySource)]` form builds through.
impl FromName for LocalMemorySource {
    fn from_name(name: impl Into<&'static str>) -> Self {
        Self { name: name.into() }
    }
}

// The manual subscriber definition takes the descriptor as its source unchanged.
impl IntoSource for LocalMemorySource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    SubscriptionSource<ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>>
    for LocalMemorySource
{
    type Subscriber = LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>;
    // The bus moves nothing on its own, and one name is both ends of it, so a copy goes back to
    // the name the subscription reads.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    fn static_name(&self) -> Option<&'static str> {
        Some(self.name)
    }

    fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<Self::Subscriber, LocalMemoryError>> {
        ready(connected.open(SlotName::Static(self.name)))
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    RedeliveryAddressed<ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>>
    for LocalMemorySource
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<RedeliveryAddress, LocalMemoryError>> {
        // One name is both ends of the bus, and no lookup is needed to say so.
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

/// The subscriber of the local bus: the stream of one subscription and its batches.
///
/// Dropping it frees its place on the bus and releases what waits in its queue.
pub struct LocalMemorySubscriber<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    held: Held,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    pub(super) const fn new(
        bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
        held: Held,
    ) -> Self {
        Self { bus, held }
    }

    const fn message(
        &self,
        place: Place<PAYLOAD>,
    ) -> LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS> {
        LocalMemoryMessage {
            place: Some(place),
            bus: self.bus,
            held: self.held,
        }
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Drop
    for LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        self.bus.unsubscribe(self.held);
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemorySubscriber")
            .field("subscription", &self.held)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Subscriber
    for LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Message = LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + '_ {
        Deliveries { subscriber: self }
    }

    fn ready_hint(&self) -> impl ReadyHint + 'static + use<MESSAGES, PAYLOAD, SUBSCRIPTIONS> {
        DeliveriesWaiting {
            bus: self.bus,
            held: self.held,
        }
    }
}

struct DeliveriesWaiting<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> {
    bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    held: Held,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> ReadyHint
    for DeliveriesWaiting<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn may_be_ready(&self) -> bool {
        self.bus.may_be_ready(self.held)
    }
}

/// The messages of one batch: a subscription holds at most `MESSAGES`, so a batch never holds
/// more.
pub struct LocalMemoryBatch<Message, const MESSAGES: usize>(FixedVec<Message, MESSAGES>);

impl<Message, const MESSAGES: usize> fmt::Debug for LocalMemoryBatch<Message, MESSAGES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryBatch")
            .field("messages", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<Message, const MESSAGES: usize> BatchSlot for LocalMemoryBatch<Message, MESSAGES> {
    type Message = Message;

    fn with_capacity(_size: NonZeroUsize) -> Self {
        Self(FixedVec::new())
    }

    // The messages lie inline, so each moves into the runtime's buffer once.
    fn hand_over(&mut self, taken: &mut BatchVec<Message>) {
        taken.extend(self.0.drain(..));
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> BatchSubscriber
    for LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Batch = LocalMemoryBatch<LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>, MESSAGES>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Self::Batch, Error = Self::Error> + '_ {
        Batches {
            subscriber: self,
            size,
        }
    }
}

struct Deliveries<'a, const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> {
    subscriber: &'a LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Stream
    for Deliveries<'_, MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Item = Result<LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let subscriber = self.subscriber;
        subscriber
            .bus
            .poll_next(subscriber.held, cx)
            .map(|place| place.map(|place| Ok(subscriber.message(place))))
    }
}

struct Batches<'a, const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> {
    subscriber: &'a LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    size: NonZeroUsize,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    BatchStream<LocalMemoryBatch<LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>, MESSAGES>>
    for Batches<'_, MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Error = Infallible;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut LocalMemoryBatch<LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>, MESSAGES>,
    ) -> Poll<Option<Result<(), Infallible>>> {
        let subscriber = self.subscriber;
        let Poll::Ready(first) = subscriber.bus.poll_next(subscriber.held, cx) else {
            return Poll::Pending;
        };
        let Some(first) = first else {
            return Poll::Ready(None);
        };
        let limit = self.size.get().min(MESSAGES);
        // The slot is empty and holds at least one place, so the first push cannot be refused.
        let _ = slot.0.push(subscriber.message(first));
        while slot.0.len() < limit {
            let Some(next) = subscriber.bus.try_next(subscriber.held) else {
                break;
            };
            if slot.0.push(subscriber.message(next)).is_err() {
                break;
            }
        }
        Poll::Ready(Some(Ok(())))
    }
}

/// One delivery of the local bus, with its settlement.
///
/// The delivery lies in its place on the bus, and the message holds the place: moving the message
/// moves a reference, never the payload and headers. It keeps the place until it is settled or
/// dropped. One that is dropped unsettled returns to the end of the queue, as a server redelivers
/// what a consumer dropped.
pub struct LocalMemoryMessage<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    // Taken by the settlement, so the drop that follows it has nothing to requeue.
    place: Option<Place<PAYLOAD>>,
    bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    held: Held,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    /// The delivery, which the message holds until a settlement takes it.
    // Why the place is an `Option`: a settlement consumes the message and its `Drop` still runs,
    // which must then find nothing to requeue. Nothing reads the message after a settlement took
    // the place.
    fn delivery(&self) -> &Delivery<PAYLOAD> {
        self.place
            .as_deref()
            .unwrap_or_else(|| unreachable!("a message holds its place until it is settled"))
    }

    /// Puts the delivery back at the end of its queue.
    fn requeue(&mut self) -> Result<(), LocalMemoryError> {
        match self.place.take() {
            Some(place) => self.bus.requeue(self.held, place),
            None => Ok(()),
        }
    }

    /// Frees the delivery's place: the settlement is done.
    fn release(&mut self) {
        if let Some(place) = self.place.take() {
            self.bus.settle(self.held, place);
        }
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Drop
    for LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        // A delivery nobody settled comes back, as a server redelivers what a consumer dropped;
        // after a shutdown there is nowhere to bring it back to, and its place is freed.
        let _ = self.requeue();
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryMessage")
            .field(
                "deliveries",
                &self.place.as_deref().map(|delivery| delivery.deliveries),
            )
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> IncomingMessage
    for LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn payload(&self) -> &[u8] {
        &self.delivery().payload
    }

    fn headers(&self) -> &Headers {
        &self.delivery().headers
    }

    fn redelivery_count(&self) -> Option<u64> {
        Some(self.delivery().deliveries.get())
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Settle
    for LocalMemoryMessage<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.release();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        if !requeue {
            self.release();
            return ready(Ok(()));
        }
        ready(self.requeue().map_err(AckError::from))
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use core::pin::pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll};
    use std::sync::Arc;
    use std::task::Wake;

    use embassy_futures::block_on;
    use futures::{FutureExt, Stream, StreamExt};
    use ruststream_core::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
    use ruststream_runtime::RETRY_COUNT_HEADER;

    use super::*;
    use crate::local::drive::waiting;
    use crate::local::{LocalMemoryBroker, LocalMemoryBus};

    type Bus = LocalMemoryBus<4, 16, 2>;
    type Connected = ConnectedLocalMemoryBroker<4, 16, 2>;

    async fn open(
        bus: &'static Bus,
        name: &'static str,
    ) -> (Connected, LocalMemorySubscriber<4, 16, 2>) {
        let connected = LocalMemoryBroker::new(bus).connect().await.unwrap();
        let subscriber = LocalMemorySource::new(name)
            .subscribe(&connected)
            .await
            .unwrap();
        (connected, subscriber)
    }

    async fn publish(connected: &Connected, name: &str, payload: &[u8], headers: Headers) {
        let payload = payload;
        let message = OutgoingMessage::with_payload(name, payload).with_headers(headers);
        connected.publisher().publish(message, None).await.unwrap();
    }

    #[test]
    fn nack_with_requeue_brings_the_delivery_back_counted_each_time() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            publish(&connected, "cmd", b"open", Headers::new()).await;
            let mut stream = pin!(subscriber.stream());
            for expected in 1..=3 {
                let delivery = waiting(&mut stream).unwrap();
                assert_eq!(delivery.payload(), b"open");
                assert_eq!(delivery.redelivery_count(), Some(expected));
                delivery.nack(true).await.unwrap();
            }
            let last = waiting(&mut stream).unwrap();
            last.nack(false).await.unwrap();
            assert!(stream.next().now_or_never().is_none());
        });
    }

    #[test]
    fn a_malformed_retry_header_counts_as_a_first_delivery() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            let mut headers = Headers::new();
            headers.insert(RETRY_COUNT_HEADER, b"soon").unwrap();
            publish(&connected, "cmd", b"open", headers).await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(delivery.redelivery_count(), Some(1));
        });
    }

    #[test]
    fn headers_reach_the_subscriber_unchanged() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            let mut headers = Headers::new();
            headers.insert("trace", b"a1").unwrap();
            headers.insert("kind", b"open").unwrap();
            publish(&connected, "cmd", b"open", headers).await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(delivery.headers().get_str("trace"), Some("a1"));
            assert_eq!(delivery.headers().get_str("kind"), Some("open"));
            assert_eq!(delivery.headers().len(), 2);
        });
    }

    #[test]
    fn a_publish_to_the_redelivery_address_reaches_the_subscription() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let source = LocalMemorySource::new("cmd");
            assert_eq!(
                SubscriptionSource::<Connected>::static_name(&source),
                Some("cmd")
            );
            let address = source.redelivery_address(&connected).await.unwrap();
            let mut subscriber = source.subscribe(&connected).await.unwrap();
            publish(&connected, address.as_str(), b"again", Headers::new()).await;
            let copy = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(copy.payload(), b"again");
        });
    }

    #[test]
    fn a_requeue_after_shutdown_stays_refused_once_the_subscriber_left() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            publish(&connected, "cmd", b"open", Headers::new()).await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            connected.shutdown().await.unwrap();
            drop(subscriber);
            assert!(matches!(
                delivery.nack(true).await,
                Err(AckError::Broker(refused)) if refused.code() == 1
            ));
        });
    }

    #[test]
    fn a_requeue_after_the_subscriber_left_lands_nowhere() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            publish(&connected, "cmd", b"open", Headers::new()).await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            drop(subscriber);
            assert!(delivery.nack(true).await.is_ok());
        });
    }

    struct Counting(AtomicUsize);

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn waker() -> (Arc<Counting>, core::task::Waker) {
        let counted = Arc::new(Counting(AtomicUsize::new(0)));
        let waker = Arc::clone(&counted).into();
        (counted, waker)
    }

    #[test]
    fn a_publish_wakes_a_reader_waiting_on_an_empty_subscription() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            let (counted, waker) = waker();
            let mut cx = Context::from_waker(&waker);
            let mut stream = pin!(subscriber.stream());
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());

            publish(&connected, "cmd", b"open", Headers::new()).await;
            assert_eq!(counted.0.load(Ordering::Relaxed), 1);
            assert!(matches!(
                stream.poll_next(&mut cx),
                Poll::Ready(Some(Ok(_)))
            ));
        });
    }

    #[test]
    fn a_shutdown_wakes_a_reader_waiting_on_an_empty_subscription() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            let (counted, waker) = waker();
            let mut cx = Context::from_waker(&waker);
            let mut stream = pin!(subscriber.stream());
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());

            connected.shutdown().await.unwrap();
            assert_eq!(counted.0.load(Ordering::Relaxed), 1);
            assert!(matches!(stream.poll_next(&mut cx), Poll::Ready(None)));
        });
    }

    #[test]
    fn a_delivery_is_a_handle_to_its_place_whatever_the_room_of_the_bus() {
        // The payload and the headers lie in the bus, so a message moves no room of its own: it
        // is as large for a bus of 4 KiB payloads as for one of a byte.
        let large = size_of::<LocalMemoryMessage<4, 4096, 16>>();
        assert_eq!(large, size_of::<LocalMemoryMessage<4, 1, 16>>());
        assert!(large <= 4 * size_of::<usize>(), "{large} bytes");
    }

    #[test]
    fn the_debug_forms_name_the_capacity_and_not_the_messages() {
        static BUS: Bus = LocalMemoryBus::new();
        block_on(async {
            let (connected, mut subscriber) = open(&BUS, "cmd").await;
            publish(&connected, "cmd", b"secret", Headers::new()).await;
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();

            let shown = std::format!("{:?} {:?} {:?}", BUS, connected, delivery);
            assert!(shown.contains("messages: 4"), "{shown}");
            assert!(shown.contains("capacity: 4"), "{shown}");
            assert!(!shown.contains("secret"), "{shown}");
        });
    }
}
