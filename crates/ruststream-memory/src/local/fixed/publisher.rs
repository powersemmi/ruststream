//! The publish policy of the fixed local bus and its live publisher.

use core::fmt;
use core::future::ready;

use ruststream_core::{
    DefaultPublish, Lend, OutgoingMessage, PairError, PublishMessageError, PublishOutcome,
    PublishPolicy, Publisher,
};

use super::bus::LocalMemoryBus;
use super::{ConnectedLocalMemoryBroker, LocalMemoryError};

/// The publish policy of the local bus: it pairs into a [`LocalMemoryPublisher`].
///
/// It is the bus's default policy, so a reply or a retry copy published by the runtime uses it.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// The policy is declared before the bus connects and paired once it has.
/// async fn sensor(policy: LocalMemoryPublish) -> Result<(), LocalMemoryError> {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     let publisher = policy.pair(&connected).await.expect("the bus pairs");
///     let reading = &b"21"[..];
///     publisher
///         .publish(OutgoingMessage::with_payload("sensors/temp", reading), None)
///         .await
///         .map_err(PublishMessageError::into_error)
/// }
///
/// assert_eq!(embassy_futures::block_on(sensor(LocalMemoryPublish)), Ok(()));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct LocalMemoryPublish;

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    PublishPolicy<ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>>
    for LocalMemoryPublish
{
    type Live = LocalMemoryPublisher<MESSAGES, PAYLOAD, SUBSCRIPTIONS>;

    fn pair(
        self,
        connected: &ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> DefaultPublish
    for ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    type Policy = LocalMemoryPublish;
}

/// The live publisher of the local bus.
///
/// A publish reaches every subscription of its name or, when one of them is full, none of them.
/// A message the bus does not take, because a subscription is full or the payload does not fit,
/// comes back with the refusal.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<1> = LocalMemoryBus::new();
///
/// /// A full subscription refuses the publish, and the sensor keeps the reading for later.
/// async fn report(publisher: &LocalMemoryPublisher<1>, celsius: u8) -> bool {
///     let reading = &[celsius][..];
///     let reading = OutgoingMessage::with_payload("sensors/temp", reading);
///     match publisher.publish(reading, None).await.map_err(PublishMessageError::into_error) {
///         Ok(()) => true,
///         Err(LocalMemoryError::Full { .. }) => false,
///         Err(other) => panic!("the bus is open: {other}"),
///     }
/// }
///
/// let taken = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await.expect("connects");
///     let _readings = connected.subscribe("sensors/temp").await.expect("subscribes");
///     let publisher = connected.publisher();
///     [report(&publisher, 21).await, report(&publisher, 22).await]
/// });
/// assert_eq!(taken, [true, false]);
/// ```
#[derive(Clone, Copy)]
pub struct LocalMemoryPublisher<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    pub(super) bus: &'static LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for LocalMemoryPublisher<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryPublisher")
            .field("capacity", &MESSAGES)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Publisher
    for LocalMemoryPublisher<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    // The bus copies the payload into the subscription's own room, so the publish only reads it:
    // a reply is encoded in the lane's buffer, whatever room the mount gave it.
    type Payload = Lend;
    type Error = LocalMemoryError;
    // The bus has no protocol field a message can differ in.
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = PublishOutcome<'msg, Self::Error, Lend>> {
        ready(
            self.bus
                .fanout(msg.name(), msg.payload(), msg.headers())
                .map_err(|error| PublishMessageError::with_message(error, msg)),
        )
    }
}
