//! The local broker, its connected and closed forms, and its publisher.

use alloc::rc::Rc;
use core::fmt;
use core::future::ready;

#[cfg(feature = "testing")]
use bytes::Bytes;
#[cfg(feature = "testing")]
use ruststream_core::RawMessage;
use ruststream_core::{
    AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer,
    LocalPublisher, OutgoingMessage, PairError, PublishPolicy, ServerSpec, Subscribe, Take,
};
#[cfg(feature = "testing")]
use ruststream_runtime::testing::{Backlog, Coordinator, InProcess, LocalTestableBroker};
use thiserror::Error;

use super::LocalMemorySubscriber;
use super::alloc_prelude::*;
use super::bus::Bus;

/// The in-process broker for local runtimes: a full broker whose queues live in the process, on
/// embassy-sync channels, with types that are not `Send`.
///
/// It is the reference implementation of the `Local*` broker contract, as
/// [`MemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.MemoryBroker.html)
/// is of the Send one. A device service uses it for a queue between parts of one firmware, and it
/// runs on any executor: its channels wake whatever task polls them, and it reads no clock.
///
/// `CAP` is the capacity of each subscription: how many messages one subscription holds, waiting
/// or in its handler's hands, before a publish to it is refused with
/// [`LocalMemoryError::Full`]. A message in hand keeps its place, so a requeue always finds room.
/// A subscription receives what is published after it opened.
///
/// The broker is a cheap handle: a clone shares the bus, and a broker connected again after a
/// shutdown opens it again.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// A sensor task and the part of the firmware that logs its readings, joined by one bus.
/// async fn firmware(broker: LocalMemoryBroker<8>) -> Result<Vec<u8>, LocalMemoryError> {
///     let connected = broker.connect().await?;
///     let mut readings = LocalMemorySource::new("sensors/temp")
///         .subscribe(&connected)
///         .await?;
///     let sensor = LocalMemoryPublish.pair(&connected).await.expect("the bus pairs");
///     sensor
///         .publish(OutgoingMessage::produced("sensors/temp", BytesMut::from(&[21u8][..])), None)
///         .await?;
///
///     let mut deliveries = core::pin::pin!(readings.stream());
///     let Some(Ok(reading)) = deliveries.next().await else {
///         unreachable!("the bus is open and its stream yields no error");
///     };
///     let celsius = reading.payload().to_vec();
///     reading.ack().await.expect("the bus settles");
///     connected.shutdown().await?;
///     Ok(celsius)
/// }
///
/// let celsius = embassy_futures::block_on(firmware(LocalMemoryBroker::new()));
/// assert_eq!(celsius, Ok(vec![21]));
/// ```
///
/// The bus belongs to the task that holds it: it is not `Send`, so it does not move to another
/// thread.
///
/// ```compile_fail,E0277
/// use ruststream_memory::local::LocalMemoryBroker;
///
/// let broker = LocalMemoryBroker::<8>::new();
/// std::thread::spawn(move || drop(broker));
/// ```
pub struct LocalMemoryBroker<const CAP: usize> {
    bus: Rc<Bus<CAP>>,
}

impl<const CAP: usize> LocalMemoryBroker<CAP> {
    /// Builds a broker with an empty bus. It performs no I/O and does not connect.
    ///
    /// A capacity of zero does not build: every publish to such a subscription would be refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// /// The firmware's bus, built once at startup: every subscription holds four messages.
    /// fn bus() -> LocalMemoryBroker<4> {
    ///     LocalMemoryBroker::new()
    /// }
    ///
    /// let connected = embassy_futures::block_on(bus().connect());
    /// assert!(connected.is_ok());
    /// ```
    ///
    /// A bus whose subscriptions hold nothing is a build error:
    ///
    /// ```compile_fail,E0080
    /// use ruststream_memory::local::LocalMemoryBroker;
    ///
    /// fn bus() -> LocalMemoryBroker<0> {
    ///     LocalMemoryBroker::new()
    /// }
    ///
    /// drop(bus());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        const {
            assert!(
                CAP > 0,
                "a LocalMemoryBroker subscription holds at least one message: give CAP a value \
                 above zero",
            );
        }
        Self {
            bus: Rc::new(Bus::new()),
        }
    }
}

impl<const CAP: usize> Default for LocalMemoryBroker<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAP: usize> Clone for LocalMemoryBroker<CAP> {
    fn clone(&self) -> Self {
        Self {
            bus: Rc::clone(&self.bus),
        }
    }
}

impl<const CAP: usize> fmt::Debug for LocalMemoryBroker<CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryBroker")
            .field("capacity", &CAP)
            .finish_non_exhaustive()
    }
}

impl<const CAP: usize> Broker for LocalMemoryBroker<CAP> {
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<CAP>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        self.bus.connect();
        ready(Ok(ConnectedLocalMemoryBroker { bus: self.bus }))
    }
}

impl<const CAP: usize> DescribeServer for LocalMemoryBroker<CAP> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("memory")
    }
}

/// The connected [`LocalMemoryBroker`]: the bus open for subscriptions and publishes.
///
/// A clone shares the bus, and a shutdown through any clone closes it for all of them.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream_memory::local::prelude::*;
///
/// /// Commands reach the actuator task by name alone.
/// async fn actuator(connected: &ConnectedLocalMemoryBroker<4>) -> Result<(), LocalMemoryError> {
///     let mut commands = connected.subscribe("actuator/cmd").await?;
///     connected
///         .publisher()
///         .publish(OutgoingMessage::produced("actuator/cmd", BytesMut::from("open")), None)
///         .await?;
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
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
///     actuator(&connected).await?;
///     connected.shutdown().await.map(drop)
/// })?;
/// # Ok::<(), LocalMemoryError>(())
/// ```
pub struct ConnectedLocalMemoryBroker<const CAP: usize> {
    pub(super) bus: Rc<Bus<CAP>>,
}

impl<const CAP: usize> ConnectedLocalMemoryBroker<CAP> {
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
    /// /// The sensor task publishes each reading it takes.
    /// async fn report(
    ///     connected: &ConnectedLocalMemoryBroker<4>,
    ///     celsius: u8,
    /// ) -> Result<(), LocalMemoryError> {
    ///     let reading = OutgoingMessage::produced("sensors/temp", BytesMut::from(&[celsius][..]));
    ///     connected.publisher().publish(reading, None).await
    /// }
    ///
    /// let published = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
    ///     report(&connected, 21).await
    /// });
    /// assert_eq!(published, Ok(()));
    /// ```
    #[must_use]
    pub fn publisher(&self) -> LocalMemoryPublisher<CAP> {
        LocalMemoryPublisher {
            bus: Rc::clone(&self.bus),
        }
    }

    /// Opens a subscription to `name`, the step both subscribe paths take.
    pub(super) fn open(&self, name: &str) -> Result<LocalMemorySubscriber<CAP>, LocalMemoryError> {
        let subscription = self.bus.subscribe(name)?;
        Ok(LocalMemorySubscriber::new(
            subscription,
            Rc::clone(&self.bus),
        ))
    }
}

impl<const CAP: usize> Clone for ConnectedLocalMemoryBroker<CAP> {
    fn clone(&self) -> Self {
        Self {
            bus: Rc::clone(&self.bus),
        }
    }
}

impl<const CAP: usize> fmt::Debug for ConnectedLocalMemoryBroker<CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedLocalMemoryBroker")
            .field("capacity", &CAP)
            .finish_non_exhaustive()
    }
}

impl<const CAP: usize> ConnectedBroker for ConnectedLocalMemoryBroker<CAP> {
    type Error = LocalMemoryError;
    type Closed = ClosedLocalMemoryBroker;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        ready(Ok(ClosedLocalMemoryBroker {
            subscriptions_closed: self.bus.shut_down(),
        }))
    }
}

impl<const CAP: usize> Subscribe for ConnectedLocalMemoryBroker<CAP> {
    type Subscriber = LocalMemorySubscriber<CAP>;
    // One name is both ends of the bus, so a publish under the name a subscription reads reaches
    // it, and the name is the address of its retry copies.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

/// What a [`LocalMemoryBroker`] shutdown reports: how many subscriptions it closed.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// Stops the bus and says how many readers were still attached.
/// async fn stop(connected: ConnectedLocalMemoryBroker<4>) -> Result<usize, LocalMemoryError> {
///     let closed = connected.shutdown().await?;
///     Ok(closed.subscriptions_closed())
/// }
///
/// let closed = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
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
    /// let closed = embassy_futures::block_on(async {
    ///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
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

/// The publish policy of the local bus: it pairs into a [`LocalMemoryPublisher`].
///
/// It is the bus's default policy, so a reply or a retry copy published by the runtime uses it.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// The policy is declared before the bus connects and paired once it has.
/// async fn sensor(policy: LocalMemoryPublish) -> Result<(), LocalMemoryError> {
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
///     let publisher = policy.pair(&connected).await.expect("the bus pairs");
///     publisher
///         .publish(OutgoingMessage::produced("sensors/temp", BytesMut::from("21")), None)
///         .await
/// }
///
/// assert_eq!(embassy_futures::block_on(sensor(LocalMemoryPublish)), Ok(()));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct LocalMemoryPublish;

impl<const CAP: usize> PublishPolicy<ConnectedLocalMemoryBroker<CAP>> for LocalMemoryPublish {
    type Live = LocalMemoryPublisher<CAP>;

    fn pair(
        self,
        connected: &ConnectedLocalMemoryBroker<CAP>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl<const CAP: usize> DefaultPublish for ConnectedLocalMemoryBroker<CAP> {
    type Policy = LocalMemoryPublish;
}

/// The live publisher of the local bus.
///
/// A publish reaches every subscription of its name or, when one of them is full, none of them.
/// The payload buffer and the headers move into the bus: a publish one subscription reads
/// allocates nothing, and each further subscription of the same name costs a clone of the
/// headers.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// A full subscription refuses the publish, and the sensor keeps the reading for later.
/// async fn report(publisher: &LocalMemoryPublisher<1>, celsius: u8) -> bool {
///     let reading = OutgoingMessage::produced("sensors/temp", BytesMut::from(&[celsius][..]));
///     match publisher.publish(reading, None).await {
///         Ok(()) => true,
///         Err(LocalMemoryError::Full { .. }) => false,
///         Err(other) => panic!("the bus is open: {other}"),
///     }
/// }
///
/// let taken = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::<1>::new().connect().await.expect("connects");
///     let _readings = connected.subscribe("sensors/temp").await.expect("subscribes");
///     let publisher = connected.publisher();
///     [report(&publisher, 21).await, report(&publisher, 22).await]
/// });
/// assert_eq!(taken, [true, false]);
/// ```
pub struct LocalMemoryPublisher<const CAP: usize> {
    bus: Rc<Bus<CAP>>,
}

impl<const CAP: usize> Clone for LocalMemoryPublisher<CAP> {
    fn clone(&self) -> Self {
        Self {
            bus: Rc::clone(&self.bus),
        }
    }
}

impl<const CAP: usize> fmt::Debug for LocalMemoryPublisher<CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryPublisher")
            .field("capacity", &CAP)
            .finish_non_exhaustive()
    }
}

impl<const CAP: usize> LocalPublisher for LocalMemoryPublisher<CAP> {
    // The bus keeps every message until its subscriptions have read it, so the publish takes the
    // buffer rather than borrowing it.
    type Payload = Take;
    type Error = LocalMemoryError;
    // The bus has no protocol field a message can differ in.
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let (name, payload, headers) = msg.into_parts();
        ready(self.bus.fanout(name, payload.freeze(), headers))
    }
}

/// An error of the local bus.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// A publish after the bus shut down is refused, never lost silently.
/// async fn late_publish() -> Result<(), LocalMemoryError> {
///     let connected = LocalMemoryBroker::<4>::new().connect().await?;
///     let publisher = connected.publisher();
///     connected.shutdown().await?;
///     publisher
///         .publish(OutgoingMessage::produced("sensors/temp", BytesMut::from("21")), None)
///         .await
/// }
///
/// assert_eq!(embassy_futures::block_on(late_publish()), Err(LocalMemoryError::ShutDown));
/// ```
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalMemoryError {
    /// The bus is shut down: it takes no publish, subscription or requeue until it connects again.
    #[error("the local memory broker is shut down")]
    ShutDown,
    /// A subscription of the name holds `capacity` messages, waiting or in hand, so the publish
    /// reached none of the subscriptions.
    #[error(
        "a subscription of {name:?} holds {capacity} messages, its capacity; the publish reached \
         no subscription"
    )]
    Full {
        /// The name published to.
        name: String,
        /// The capacity of each subscription, the broker's `CAP`.
        capacity: usize,
    },
}

// The bus lives in the process by nature, so the transition the harness connects through is the
// ordinary `connect`: the connected form a test drives is the one production gets.
#[cfg(feature = "testing")]
impl<const CAP: usize> InProcess for LocalMemoryBroker<CAP> {
    fn connect_in_process(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        self.connect()
    }
}

#[cfg(feature = "testing")]
impl<const CAP: usize> LocalTestableBroker for ConnectedLocalMemoryBroker<CAP> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.bus.harness().install(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let (name, payload, headers) = message.into_parts();
        // The harness injects strictly between connect and shutdown and settles what it
        // receives, so a refusal here is a test that outran the capacity it chose.
        if let Err(refused) = self
            .bus
            .fanout(name, Bytes::copy_from_slice(payload), headers)
        {
            panic!("the harness injected into {name:?} and the bus refused it: {refused}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus.harness().published(name)
    }

    fn backlog(&self) -> Backlog {
        Backlog::Missed
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use ruststream_core::IncomingMessage;

    use embassy_futures::block_on;
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{LocalSettle, LocalSubscriber, OutgoingFor};

    use super::*;
    use crate::local::drive::{ended, waiting};

    fn reading<'a>(name: &'a str, payload: &'static [u8]) -> OutgoingFor<'a, Take> {
        OutgoingMessage::produced(name, BytesMut::from(payload))
    }

    #[test]
    fn a_publish_reaches_every_subscription_of_its_name_and_no_other() {
        block_on(async {
            let connected = LocalMemoryBroker::<4>::new().connect().await.unwrap();
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
    fn a_full_subscription_refuses_the_publish_for_every_subscription_of_the_name() {
        block_on(async {
            let connected = LocalMemoryBroker::<1>::new().connect().await.unwrap();
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
                refused,
                Err(LocalMemoryError::Full {
                    name: "temp".into(),
                    capacity: 1,
                })
            );
            assert!(
                pin!(roomy.stream()).next().now_or_never().is_none(),
                "a refused publish reached a subscription that had room"
            );
        });
    }

    #[test]
    fn a_delivery_in_hand_keeps_its_place_so_its_requeue_always_fits() {
        block_on(async {
            let connected = LocalMemoryBroker::<1>::new().connect().await.unwrap();
            let mut subscriber = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let mut stream = pin!(subscriber.stream());
            let in_hand = waiting(&mut stream).unwrap();

            assert!(matches!(
                publisher.publish(reading("temp", b"22"), None).await,
                Err(LocalMemoryError::Full { .. })
            ));
            in_hand.nack(true).await.unwrap();
            let again = waiting(&mut stream).unwrap();
            assert_eq!(again.payload(), b"21");
            again.ack().await.unwrap();
            publisher
                .publish(reading("temp", b"22"), None)
                .await
                .unwrap();
        });
    }

    #[test]
    fn a_dropped_subscriber_stops_counting_against_its_name() {
        block_on(async {
            let connected = LocalMemoryBroker::<1>::new().connect().await.unwrap();
            let gone = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            drop(gone);
            for celsius in [b"22", b"23"] {
                publisher
                    .publish(reading("temp", celsius), None)
                    .await
                    .unwrap();
            }
        });
    }

    #[cfg(feature = "testing")]
    #[test]
    fn the_publish_log_holds_what_each_name_took_and_no_refused_publish() {
        block_on(async {
            let connected = LocalMemoryBroker::<1>::new().connect().await.unwrap();
            let _full = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            publisher
                .publish(reading("humidity", b"40"), None)
                .await
                .unwrap();
            assert!(
                publisher
                    .publish(reading("temp", b"22"), None)
                    .await
                    .is_err()
            );

            let logged = |name| -> Vec<Vec<u8>> {
                connected
                    .published(name)
                    .iter()
                    .map(|message| message.payload().to_vec())
                    .collect()
            };
            assert_eq!(logged("temp"), [b"21".to_vec()]);
            assert_eq!(logged("humidity"), [b"40".to_vec()]);
        });
    }

    #[cfg(feature = "testing")]
    #[test]
    #[should_panic(expected = "the bus refused it")]
    fn an_injection_the_bus_refuses_fails_the_test() {
        block_on(async {
            let connected = LocalMemoryBroker::<1>::new().connect().await.unwrap();
            let _full = connected.subscribe("temp").await.unwrap();
            connected.inject(OutgoingMessage::new("temp", b"21".as_slice()));
            connected.inject(OutgoingMessage::new("temp", b"22".as_slice()));
        });
    }

    #[test]
    fn after_shutdown_publishes_and_subscriptions_are_refused_until_a_reconnect() {
        block_on(async {
            let broker = LocalMemoryBroker::<4>::new();
            let connected = broker.clone().connect().await.unwrap();
            let publisher = connected.publisher();
            let early = connected.clone();
            let closed = connected.shutdown().await.unwrap();
            assert_eq!(closed.subscriptions_closed(), 0);

            assert_eq!(
                publisher.publish(reading("temp", b"21"), None).await,
                Err(LocalMemoryError::ShutDown)
            );
            assert_eq!(
                early.subscribe("temp").await.unwrap_err(),
                LocalMemoryError::ShutDown
            );

            let reconnected = broker.connect().await.unwrap();
            let mut subscriber = reconnected.subscribe("temp").await.unwrap();
            publisher
                .publish(reading("temp", b"22"), None)
                .await
                .unwrap();
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(delivery.payload(), b"22");
            delivery.ack().await.unwrap();
        });
    }

    #[test]
    fn a_shutdown_ends_each_stream_once_what_waits_is_drained() {
        block_on(async {
            let connected = LocalMemoryBroker::<4>::new().connect().await.unwrap();
            let mut subscriber = connected.subscribe("temp").await.unwrap();
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let closed = connected.shutdown().await.unwrap();
            assert_eq!(closed.subscriptions_closed(), 1);

            let mut stream = pin!(subscriber.stream());
            let drained = waiting(&mut stream).unwrap();
            assert_eq!(drained.payload(), b"21");
            drained.ack().await.unwrap();
            assert!(ended(&mut stream));
        });
    }

    #[test]
    fn a_clone_of_the_bus_its_connection_or_its_publisher_reaches_the_same_subscriptions() {
        block_on(async {
            let board = LocalMemoryBroker::<4>::default();
            let connected = board.clone().connect().await.unwrap();
            let mut subscriber = connected.clone().subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .clone()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();

            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(delivery.payload(), b"21");
            delivery.ack().await.unwrap();
        });
    }

    #[test]
    fn the_debug_forms_name_the_capacity_and_the_subscription_not_the_bus() {
        block_on(async {
            let board = LocalMemoryBroker::<4>::new();
            assert_eq!(
                format!("{board:?}"),
                "LocalMemoryBroker { capacity: 4, .. }"
            );
            let connected = board.connect().await.unwrap();
            assert_eq!(
                format!("{connected:?}"),
                "ConnectedLocalMemoryBroker { capacity: 4, .. }"
            );
            assert_eq!(
                format!("{:?}", connected.publisher()),
                "LocalMemoryPublisher { capacity: 4, .. }"
            );
            let mut subscriber = connected.subscribe("temp").await.unwrap();
            assert_eq!(
                format!("{subscriber:?}"),
                "LocalMemorySubscriber { name: \"temp\", .. }"
            );
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let delivery = waiting(&mut pin!(subscriber.stream())).unwrap();
            assert_eq!(
                format!("{delivery:?}"),
                "LocalMemoryMessage { name: \"temp\", deliveries: 1, .. }"
            );
            delivery.ack().await.unwrap();
        });
    }

    #[test]
    fn the_bus_is_described_as_a_server_in_the_process() {
        assert_eq!(
            LocalMemoryBroker::<4>::new().describe_server(),
            ServerSpec::in_process("memory")
        );
    }
}
