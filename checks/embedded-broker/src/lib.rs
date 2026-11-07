//! A broker crate on the embedded runner whose client lives on one thread: its handles share the
//! connection through an `Rc`, so none of its types is `Send`. Its in-process mode routes through
//! the local bus, whose answers the local conformance suites hold it to.

use std::convert::Infallible;
use std::rc::Rc;

use futures::{Stream, StreamExt};
#[cfg(not(ruststream_alloc))]
use ruststream::memory::local::LocalMemoryBus;
use ruststream::memory::local::{
    ClosedLocalMemoryBroker, ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError,
    LocalMemoryMessage, LocalMemoryPublisher, LocalMemorySource, LocalMemorySubscriber,
};
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    AckError, AddressedCopies, Broker, ConnectedBroker, DefaultPublish, Headers, IncomingMessage,
    OutgoingFor, OutgoingMessage, PairError, PublishOutcome, PublishPolicy, Publisher, RawMessage,
    ReadyHint, Settle, Subscribe, Subscriber, SubscriptionSource,
};

/// How many deliveries each subscription of the bus holds.
const CAPACITY: usize = 16;

/// The connection every handle of the broker shares. A client on a single-thread network stack
/// shares it through an `Rc`, so no type that holds it is `Send`.
type Connection = Rc<ConnectedLocalMemoryBroker<CAPACITY>>;

/// The payload form the bus's publisher takes, in the variant of the build.
type Payload = <LocalMemoryPublisher<CAPACITY> as Publisher>::Payload;

/// A broker whose client lives on one thread.
pub struct RcBroker {
    bus: Rc<LocalMemoryBroker<CAPACITY>>,
}

impl RcBroker {
    /// Records the configuration: a bus of its own, which the broker's in-process mode routes
    /// through.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bus: Rc::new(local_bus()),
        }
    }
}

impl Default for RcBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(ruststream_alloc)]
fn local_bus() -> LocalMemoryBroker<CAPACITY> {
    LocalMemoryBroker::new()
}

// The bus of the device variant is storage the board declares once; a test that builds one per
// broker leaks it instead.
#[cfg(not(ruststream_alloc))]
fn local_bus() -> LocalMemoryBroker<CAPACITY> {
    LocalMemoryBroker::new(Box::leak(Box::new(LocalMemoryBus::new())))
}

impl Broker for RcBroker {
    type Error = LocalMemoryError;
    type Connected = ConnectedRcBroker;

    async fn connect(self) -> Result<ConnectedRcBroker, LocalMemoryError> {
        let connected = Rc::unwrap_or_clone(self.bus).connect().await?;
        Ok(ConnectedRcBroker {
            connection: Rc::new(connected),
        })
    }
}

impl InProcess for RcBroker {
    async fn connect_in_process(self) -> Result<ConnectedRcBroker, LocalMemoryError> {
        let connected = Rc::unwrap_or_clone(self.bus).connect_in_process().await?;
        Ok(ConnectedRcBroker {
            connection: Rc::new(connected),
        })
    }
}

/// The connected [`RcBroker`].
pub struct ConnectedRcBroker {
    connection: Connection,
}

impl ConnectedRcBroker {
    /// A publisher over this connection.
    #[must_use]
    pub fn publisher(&self) -> RcPublisher {
        RcPublisher {
            connection: Rc::clone(&self.connection),
        }
    }
}

impl ConnectedBroker for ConnectedRcBroker {
    type Error = LocalMemoryError;
    type Closed = ClosedLocalMemoryBroker;

    async fn shutdown(self) -> Result<ClosedLocalMemoryBroker, LocalMemoryError> {
        Rc::unwrap_or_clone(self.connection).shutdown().await
    }
}

impl Subscribe for ConnectedRcBroker {
    type Subscriber = RcSubscriber;
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<RcSubscriber, LocalMemoryError> {
        let inner = Subscribe::subscribe(&*self.connection, name).await?;
        Ok(RcSubscriber {
            inner,
            connection: Rc::clone(&self.connection),
        })
    }
}

impl TestableBroker for ConnectedRcBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.connection.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.connection.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.connection.published(name)
    }
}

/// The publish policy of [`RcBroker`]: every publish goes to the bus.
#[derive(Debug, Default)]
pub struct RcPublish;

impl PublishPolicy<ConnectedRcBroker> for RcPublish {
    type Live = RcPublisher;

    async fn pair(self, connected: &ConnectedRcBroker) -> Result<RcPublisher, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedRcBroker {
    type Policy = RcPublish;
}

/// The live publisher of [`RcBroker`].
#[derive(Clone)]
pub struct RcPublisher {
    connection: Connection,
}

impl Publisher for RcPublisher {
    type Payload = Payload;
    type Error = LocalMemoryError;
    type Options = ();

    async fn publish<'msg>(
        &self,
        msg: OutgoingFor<'msg, Payload>,
        options: Option<&()>,
    ) -> PublishOutcome<'msg, LocalMemoryError, Payload> {
        self.connection.publisher().publish(msg, options).await
    }
}

/// The subscription descriptor of [`RcBroker`]: one name on the bus.
pub struct RcSource {
    inner: LocalMemorySource,
}

impl RcSource {
    /// A subscription to `name`.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            inner: LocalMemorySource::new(name),
        }
    }
}

impl SubscriptionSource<ConnectedRcBroker> for RcSource {
    type Subscriber = RcSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        SubscriptionSource::<ConnectedLocalMemoryBroker<CAPACITY>>::name(&self.inner)
    }

    async fn subscribe(
        self,
        connected: &ConnectedRcBroker,
    ) -> Result<RcSubscriber, LocalMemoryError> {
        let inner = self.inner.subscribe(&*connected.connection).await?;
        Ok(RcSubscriber {
            inner,
            connection: Rc::clone(&connected.connection),
        })
    }
}

/// A subscription of [`RcBroker`].
pub struct RcSubscriber {
    inner: LocalMemorySubscriber<CAPACITY>,
    connection: Connection,
}

impl Subscriber for RcSubscriber {
    type Message = RcDelivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<RcDelivery, Infallible>> + '_ {
        let connection = &self.connection;
        self.inner.stream().map(move |delivery| {
            delivery.map(|inner| RcDelivery {
                inner,
                _connection: Rc::clone(connection),
            })
        })
    }

    fn ready_hint(&self) -> impl ReadyHint + 'static + use<> {
        self.inner.ready_hint()
    }
}

/// A delivery of [`RcBroker`].
pub struct RcDelivery {
    inner: LocalMemoryMessage<CAPACITY>,
    // Held for the delivery's life, as a client's receive buffer is.
    _connection: Connection,
}

impl IncomingMessage for RcDelivery {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &Headers {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner.redelivery_count()
    }
}

impl Settle for RcDelivery {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        self.inner.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.inner.nack(requeue)
    }
}
