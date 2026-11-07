//! The fixed local broker, its connected and closed forms.

use core::fmt;
use core::future::ready;

use ruststream_core::{AddressedCopies, Broker, ConnectedBroker, Subscribe};

use super::bus::{LocalMemoryBus, NAME_BYTES, SlotName};
use super::{LocalMemoryError, LocalMemoryPublisher, LocalMemorySubscriber};

/// The in-process broker for local runtimes: a full broker whose queues live in a [`LocalMemoryBus`]
/// of fixed size that the board declares as a `static`, so it needs no allocator.
///
/// It is the reference implementation of the contract for a broker whose types are not `Send`,
/// and it runs on any executor: its primitives wake whatever task polls them, and it reads no
/// clock.
///
/// `MESSAGES` is the capacity of each subscription: how many messages one subscription holds,
/// waiting or in its handler's hands, before a publish to it is refused with
/// [`LocalMemoryError::Full`]. A message in hand keeps its place, so a requeue always finds room.
/// `PAYLOAD` is the largest payload in bytes, and `SUBSCRIPTIONS` how many subscriptions are open
/// at once. A subscription receives what is published after it opened.
///
/// The broker is a copy of a reference to the bus: a copy shares the bus, and a broker connected
/// again after a shutdown opens it again.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// The firmware's bus: every subscription holds four messages of up to 64 bytes.
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// A sensor task and the part of the firmware that logs its readings, joined by one bus.
/// async fn firmware(broker: LocalMemoryBroker<4>) -> Result<u8, LocalMemoryError> {
///     let connected = broker.connect().await?;
///     let mut readings = LocalMemorySource::new("sensors/temp")
///         .subscribe(&connected)
///         .await?;
///     let sensor = LocalMemoryPublish.pair(&connected).await.expect("the bus pairs");
///     let celsius = &[21u8][..];
///     sensor
///         .publish(OutgoingMessage::with_payload("sensors/temp", celsius), None)
///         .await
///         .map_err(PublishMessageError::into_error)?;
///
///     let mut deliveries = core::pin::pin!(readings.stream());
///     let Some(Ok(reading)) = deliveries.next().await else {
///         unreachable!("the bus is open and its stream yields no error");
///     };
///     let celsius = reading.payload()[0];
///     reading.ack().await.expect("the bus settles");
///     connected.shutdown().await?;
///     Ok(celsius)
/// }
///
/// let celsius = embassy_futures::block_on(firmware(LocalMemoryBroker::new(&BUS)));
/// assert_eq!(celsius, Ok(21));
/// ```
#[derive(Clone, Copy)]
pub struct LocalMemoryBroker<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    pub(super) bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    LocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    /// Builds a broker over `bus`. It performs no I/O and does not connect.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    ///
    /// /// The firmware's bus, built once at startup: every subscription holds four messages.
    /// fn bus() -> LocalMemoryBroker<4> {
    ///     LocalMemoryBroker::new(&BUS)
    /// }
    ///
    /// let connected = embassy_futures::block_on(bus().connect());
    /// assert!(connected.is_ok());
    /// ```
    #[must_use]
    pub const fn new(bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>) -> Self {
        Self { bus }
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for LocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryBroker")
            .field("capacity", &MESSAGES)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Broker
    for LocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        self.bus.connect();
        ready(Ok(ConnectedLocalMemoryBroker { bus: self.bus }))
    }
}

/// The connected [`LocalMemoryBroker`]: the bus open for subscriptions and publishes.
///
/// A copy shares the bus, and a shutdown through any copy closes it for all of them.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// Commands reach the actuator task by name alone.
/// async fn actuator(connected: &ConnectedLocalMemoryBroker<4>) -> Result<(), LocalMemoryError> {
///     let mut commands = connected.subscribe("actuator/cmd").await?;
///     let open = &b"open"[..];
///     connected
///         .publisher()
///         .publish(OutgoingMessage::with_payload("actuator/cmd", open), None)
///         .await
///         .map_err(PublishMessageError::into_error)?;
///     let mut deliveries = core::pin::pin!(commands.stream());
///     let Some(Ok(command)) = deliveries.next().await else {
///         unreachable!("the bus is open and its stream yields no error");
///     };
///     assert_eq!(command.payload(), b"open");
///     command.ack().await.expect("the bus settles");
///     Ok(())
/// }
///
/// embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     actuator(&connected).await?;
///     connected.shutdown().await.map(drop)
/// })?;
/// # Ok::<(), LocalMemoryError>(())
/// ```
#[derive(Clone, Copy)]
pub struct ConnectedLocalMemoryBroker<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    pub(super) bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    /// A publisher on this bus.
    ///
    /// A publisher made before a shutdown refuses every publish after it with
    /// [`LocalMemoryError::ShutDown`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    ///
    /// /// The sensor task publishes each reading it takes.
    /// async fn report(
    ///     connected: &ConnectedLocalMemoryBroker<4>,
    ///     celsius: u8,
    /// ) -> Result<(), LocalMemoryError> {
    ///     let reading = &[celsius][..];
    ///     connected
    ///         .publisher()
    ///         .publish(OutgoingMessage::with_payload("sensors/temp", reading), None)
    ///         .await
    ///         .map_err(PublishMessageError::into_error)
    /// }
    ///
    /// let published = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
    ///     report(&connected, 21).await
    /// });
    /// assert_eq!(published, Ok(()));
    /// ```
    #[must_use]
    pub const fn publisher(self) -> LocalMemoryPublisher<MESSAGES, PAYLOAD, SUBSCRIPTIONS> {
        LocalMemoryPublisher { bus: self.bus }
    }

    /// Opens a subscription to `name`, the step both subscribe paths take.
    pub(super) fn open(
        self,
        name: SlotName,
    ) -> Result<LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>, LocalMemoryError> {
        let held = self.bus.subscribe(name)?;
        Ok(LocalMemorySubscriber::new(self.bus, held))
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedLocalMemoryBroker")
            .field("capacity", &MESSAGES)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> ConnectedBroker
    for ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Error = LocalMemoryError;
    type Closed = ClosedLocalMemoryBroker;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        ready(Ok(ClosedLocalMemoryBroker {
            subscriptions_closed: self.bus.shut_down(),
        }))
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Subscribe
    for ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Subscriber = LocalMemorySubscriber<MESSAGES, PAYLOAD, SUBSCRIPTIONS>;
    // One name is both ends of the bus, so a publish under the name a subscription reads reaches
    // it, and the name is the address of its retry copies.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(
            SlotName::built(name)
                .ok_or(LocalMemoryError::NameTooLong {
                    capacity: NAME_BYTES,
                })
                .and_then(|name| self.open(name)),
        )
    }
}

/// What a [`LocalMemoryBroker`] shutdown reports: how many subscriptions it closed.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// Stops the bus and says how many readers were still attached.
/// async fn stop(connected: ConnectedLocalMemoryBroker<4>) -> Result<usize, LocalMemoryError> {
///     let closed = connected.shutdown().await?;
///     Ok(closed.subscriptions_closed())
/// }
///
/// let closed = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     let _readings = connected.subscribe("sensors/temp").await?;
///     stop(connected).await
/// });
/// assert_eq!(closed, Ok(1));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedLocalMemoryBroker {
    subscriptions_closed: usize,
}

impl ClosedLocalMemoryBroker {
    /// How many subscriptions were open when the bus shut down; their streams end once drained.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    ///
    /// let closed = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
    ///     connected.shutdown().await
    /// })?;
    /// assert_eq!(closed.subscriptions_closed(), 0);
    /// # Ok::<(), LocalMemoryError>(())
    /// ```
    #[must_use]
    pub const fn subscriptions_closed(&self) -> usize {
        self.subscriptions_closed
    }
}

#[cfg(test)]
mod tests {
    use core::future::poll_fn;
    use core::num::NonZeroUsize;
    use core::pin::pin;

    use embassy_futures::block_on;
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{
        BatchSlot, BatchStream, BatchSubscriber, BatchVec, Headers, IncomingMessage, OutgoingFor,
        ReadyHint, Settle, Subscriber, SubscriptionSource,
    };

    use ruststream_core::{AckError, Lend, OutgoingMessage, PublishMessageError, Publisher};

    use super::*;
    use crate::local::LocalMemorySource;
    use crate::local::drive::{ended, waiting};

    fn reading<'a>(name: &'a str, payload: &'a [u8]) -> OutgoingFor<'a, Lend> {
        OutgoingMessage::with_payload(name, payload)
    }

    #[test]
    fn a_publish_reaches_every_subscription_of_its_name_and_no_other() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut first = connected.subscribe("temp").await.unwrap();
            let mut second = connected.subscribe("temp").await.unwrap();
            let mut other = connected.subscribe("humidity").await.unwrap();
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();

            for subscriber in [&mut first, &mut second] {
                let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
                assert_eq!(delivery.payload(), b"21");
                delivery.ack().await.unwrap();
            }
            assert!(pin!(other.stream()).next().now_or_never().is_none());
        });
    }

    #[test]
    fn a_literal_name_and_a_built_name_reach_the_same_subscription() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut literal = LocalMemorySource::new("temp")
                .subscribe(&connected)
                .await
                .unwrap();
            let mut built = connected.subscribe("temp").await.unwrap();
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();

            let first = waiting(&mut pin!(literal.stream())).unwrap();
            let second = waiting(&mut pin!(built.stream())).unwrap();
            assert_eq!(
                (first.payload(), second.payload()),
                (&b"21"[..], &b"21"[..])
            );
        });
    }

    #[test]
    fn a_full_subscription_refuses_the_publish_for_every_subscription_of_the_name() {
        static BUS: LocalMemoryBus<1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut roomy = connected.subscribe("temp").await.unwrap();
            let _full = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let held = waiting(&mut pin!(roomy.stream())).unwrap();
            held.ack().await.unwrap();

            let refused = publisher.publish(reading("temp", b"22"), None).await;
            assert_eq!(
                refused.map_err(PublishMessageError::into_error),
                Err(LocalMemoryError::Full { capacity: 1 })
            );
            assert!(
                pin!(roomy.stream()).next().now_or_never().is_none(),
                "a refused publish reached a subscription that had room"
            );
        });
    }

    #[test]
    fn a_refused_publish_returns_its_message_whole() {
        static BUS: LocalMemoryBus<1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let _full = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();

            let mut headers = Headers::new();
            headers.insert("trace", b"a1").unwrap();
            let late = reading("temp", b"22").with_headers(headers);
            let refused = publisher.publish(late, None).await.unwrap_err();

            let (error, message) = refused.into_parts();
            let message = message.expect("the bus gives the refused message back");
            assert_eq!(error, LocalMemoryError::Full { capacity: 1 });
            assert_eq!(message.name(), "temp");
            assert_eq!(message.payload(), b"22");
            assert_eq!(message.headers().get_str("trace"), Some("a1"));
        });
    }

    #[test]
    fn a_payload_past_the_room_of_a_message_is_refused_with_its_message() {
        static BUS: LocalMemoryBus<2, 4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();

            let refused = publisher
                .publish(reading("temp", b"12345"), None)
                .await
                .unwrap_err();

            let (error, message) = refused.into_parts();
            assert_eq!(
                error,
                LocalMemoryError::PayloadTooLarge {
                    size: 5,
                    capacity: 4
                }
            );
            assert_eq!(message.expect("handed back").payload(), b"12345");
            assert!(pin!(readings.stream()).next().now_or_never().is_none());
            publisher
                .publish(reading("temp", b"1234"), None)
                .await
                .unwrap();
        });
    }

    #[test]
    fn a_delivery_in_hand_keeps_its_place_so_its_requeue_always_fits() {
        static BUS: LocalMemoryBus<1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut subscriber = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let mut stream = pin!(subscriber.stream());
            let in_hand = waiting(&mut stream).unwrap();

            assert!(matches!(
                publisher
                    .publish(reading("temp", b"22"), None)
                    .await
                    .map_err(PublishMessageError::into_error),
                Err(LocalMemoryError::Full { .. })
            ));
            in_hand.nack(true).await.unwrap();
            let again = waiting(&mut stream).unwrap();
            assert_eq!(again.payload(), b"21");
            assert_eq!(again.redelivery_count(), Some(2));
            again.ack().await.unwrap();
            publisher
                .publish(reading("temp", b"22"), None)
                .await
                .unwrap();
        });
    }

    #[test]
    fn a_dropped_subscriber_frees_its_place_and_stops_counting_against_its_name() {
        static BUS: LocalMemoryBus<1, 8, 1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let gone = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            assert_eq!(
                connected.subscribe("temp").await.unwrap_err(),
                LocalMemoryError::NoFreeSubscription { capacity: 1 }
            );
            drop(gone);
            for celsius in [b"22", b"23"] {
                publisher
                    .publish(reading("temp", celsius), None)
                    .await
                    .unwrap();
            }
            let mut again = connected.subscribe("temp").await.unwrap();
            assert!(
                pin!(again.stream()).next().now_or_never().is_none(),
                "a subscription receives what is published after it opened"
            );
        });
    }

    #[test]
    fn a_delivery_that_outlives_its_subscriber_keeps_its_place_and_settles_into_nothing() {
        static BUS: LocalMemoryBus<1, 8, 1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut gone = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let orphan = waiting(&mut pin!(gone.stream())).unwrap();
            drop(gone);

            // The orphan lies in the slot's one place, so the slot's next subscriber has no room
            // until the orphan is settled.
            let mut next = connected.subscribe("temp").await.unwrap();
            let refused = publisher.publish(reading("temp", b"22"), None).await;
            assert_eq!(
                refused.map_err(PublishMessageError::into_error),
                Err(LocalMemoryError::Full { capacity: 1 })
            );
            orphan.nack(true).await.unwrap();
            publisher
                .publish(reading("temp", b"22"), None)
                .await
                .unwrap();
            let delivery = waiting(&mut pin!(next.stream())).unwrap();
            assert_eq!(delivery.payload(), b"22");
            assert!(pin!(next.stream()).next().now_or_never().is_none());
            delivery.ack().await.unwrap();
            publisher
                .publish(reading("temp", b"23"), None)
                .await
                .unwrap();
        });
    }

    #[test]
    fn after_shutdown_publishes_and_subscriptions_are_refused_until_a_reconnect() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let broker = LocalMemoryBroker::new(&BUS);
            let connected = broker.connect().await.unwrap();
            let publisher = connected.publisher();
            let early = connected;
            let closed = connected.shutdown().await.unwrap();
            assert_eq!(closed.subscriptions_closed(), 0);

            assert_eq!(
                publisher
                    .publish(reading("temp", b"21"), None)
                    .await
                    .map_err(PublishMessageError::into_error),
                Err(LocalMemoryError::ShutDown)
            );
            assert_eq!(
                early.subscribe("temp").await.unwrap_err(),
                LocalMemoryError::ShutDown
            );

            let again = broker.connect().await.unwrap();
            let mut readings = again.subscribe("temp").await.unwrap();
            publisher
                .publish(reading("temp", b"22"), None)
                .await
                .unwrap();
            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            assert_eq!(delivery.payload(), b"22");
        });
    }

    #[test]
    fn a_shutdown_ends_each_stream_once_what_waits_is_drained() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let closed = connected.shutdown().await.unwrap();
            assert_eq!(closed.subscriptions_closed(), 1);

            let mut stream = pin!(readings.stream());
            let last = waiting(&mut stream).unwrap();
            assert_eq!(last.payload(), b"21");
            last.ack().await.unwrap();
            assert!(ended(&mut stream));
        });
    }

    #[test]
    fn a_requeue_after_the_shutdown_is_refused_as_a_broker_rejection() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let held = waiting(&mut pin!(readings.stream())).unwrap();
            connected.shutdown().await.unwrap();

            let refused = held.nack(true).await.unwrap_err();
            assert!(matches!(refused, AckError::Broker(cause) if cause.code() == 1));
        });
    }

    #[test]
    fn a_name_built_past_the_room_of_the_bus_is_refused() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let long = "a".repeat(65);
            assert_eq!(
                connected.subscribe(&long).await.unwrap_err(),
                LocalMemoryError::NameTooLong { capacity: 64 }
            );
            let kept = "a/literal/name/well/past/sixty-four/bytes/is/held/whole/by/the/bus/ok";
            assert!(
                LocalMemorySource::new(kept)
                    .subscribe(&connected)
                    .await
                    .is_ok()
            );
        });
    }

    #[test]
    fn a_retry_copy_continues_the_count_its_header_carries() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("jobs").await.unwrap();
            let mut headers = Headers::new();
            headers
                .insert(ruststream_runtime::RETRY_COUNT_HEADER, b"2")
                .unwrap();
            let copy = reading("jobs", b"job").with_headers(headers);
            connected.publisher().publish(copy, None).await.unwrap();

            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            assert_eq!(delivery.redelivery_count(), Some(3));
        });
    }

    #[test]
    fn a_dropped_delivery_comes_back_and_a_nack_without_requeue_does_not() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let mut stream = pin!(readings.stream());
            drop(waiting(&mut stream).unwrap());
            let back = waiting(&mut stream).unwrap();
            assert_eq!(back.redelivery_count(), Some(2));
            back.nack(false).await.unwrap();
            assert!(stream.next().now_or_never().is_none());
        });
    }

    #[test]
    fn a_batch_holds_what_waits_up_to_its_size() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            for celsius in [b"1", b"2", b"3"] {
                publisher
                    .publish(reading("temp", celsius), None)
                    .await
                    .unwrap();
            }
            let size = NonZeroUsize::new(2).unwrap();
            let mut batch =
                <LocalMemorySubscriber<4> as BatchSubscriber>::Batch::with_capacity(size);
            let mut batches = pin!(readings.batches(size));
            let filled = poll_fn(|cx| batches.as_mut().poll_fill(cx, &mut batch));
            assert!(filled.now_or_never().flatten().unwrap().is_ok());

            let mut taken = BatchVec::new();
            batch.hand_over(&mut taken);
            let payloads: heapless::Vec<u8, 4> =
                taken.iter().map(|message| message.payload()[0]).collect();
            assert_eq!(payloads.as_slice(), b"12");
        });
    }

    #[test]
    fn the_ready_hint_follows_the_queue_and_the_shutdown() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let hint = readings.ready_hint();
            assert!(!hint.may_be_ready());
            let publisher = connected.publisher();
            for celsius in [b"21", b"22"] {
                publisher
                    .publish(reading("temp", celsius), None)
                    .await
                    .unwrap();
            }
            assert!(hint.may_be_ready());
            let mut stream = pin!(readings.stream());
            waiting(&mut stream).unwrap().ack().await.unwrap();
            assert!(hint.may_be_ready(), "a second delivery waits");
            waiting(&mut stream).unwrap().ack().await.unwrap();
            assert!(!hint.may_be_ready(), "the queue is drained");
            connected.shutdown().await.unwrap();
            assert!(hint.may_be_ready(), "the stream ends");
        });
    }

    // The harness keeps a publish log on the heap, so the claim is made without it.
    #[cfg(not(feature = "testing"))]
    #[test]
    fn a_publish_a_delivery_a_retry_and_a_batch_allocate_nothing() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS).connect().await.unwrap();
            let mut first = connected.subscribe("temp").await.unwrap();
            let mut second = LocalMemorySource::new("temp")
                .subscribe(&connected)
                .await
                .unwrap();
            let publisher = connected.publisher();
            let size = NonZeroUsize::new(2).unwrap();
            let mut batch =
                <LocalMemorySubscriber<4> as BatchSubscriber>::Batch::with_capacity(size);
            // The runtime's buffer for the batch, which it allocates once per worker.
            #[cfg(ruststream_alloc)]
            let mut taken = BatchVec::with_capacity(size.get());
            #[cfg(not(ruststream_alloc))]
            let mut taken = BatchVec::new();

            let cost = allocation_counter::measure(|| {
                block_on(async {
                    let mut headers = Headers::new();
                    headers.insert("trace", b"a1").unwrap();
                    let message = reading("temp", b"21").with_headers(headers);
                    publisher.publish(message, None).await.unwrap();
                    let delivery = waiting(&mut pin!(first.stream())).unwrap();
                    delivery.nack(true).await.unwrap();
                    waiting(&mut pin!(first.stream()))
                        .unwrap()
                        .ack()
                        .await
                        .unwrap();

                    publisher
                        .publish(reading("temp", b"22"), None)
                        .await
                        .unwrap();
                    let mut batches = pin!(second.batches(size));
                    let filled = poll_fn(|cx| batches.as_mut().poll_fill(cx, &mut batch));
                    assert!(filled.now_or_never().flatten().unwrap().is_ok());
                    batch.hand_over(&mut taken);
                    taken.clear();
                });
            });
            assert_eq!(cost.count_total, 0, "the fixed bus allocated");
        });
    }
}
