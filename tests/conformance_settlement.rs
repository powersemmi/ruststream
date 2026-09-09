//! The routing suite against a stand-in whose transport cannot acknowledge.
//!
//! Several transports settle nothing: `ZeroMQ` acknowledges nothing at all, MQTT `QoS 0` and Redis
//! pub/sub have no acknowledgement to give, and a file or stdio transport has nobody to give it
//! to. Their deliveries report [`AckError::Unsupported`], and the in-process stand-in a broker
//! crate ships must answer the same way - a stand-in that succeeds instead passes a handler's
//! retry in a test and loses the message in production.
//!
//! Both halves are pinned here: the honest stand-in passes the suite, and the one claiming
//! settlements it never performs still fails it.

use std::{
    future::{Future, ready},
    marker::PhantomData,
};

use futures::{Stream, StreamExt};
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, RawMessage,
    Subscribe, Subscriber,
    conformance::harness,
    memory::{
        ClosedMemoryBroker, ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage,
        MemorySubscriber,
    },
    testing::{Coordinator, TestableBroker},
};

/// What a stand-in's deliveries answer when they are settled.
///
/// A type parameter rather than a field, so the stand-ins below cannot drift into each other's
/// answer and every scenario runs against each of them.
trait Settlement: Send + Sync + 'static {
    /// The answer `ack` and `nack` give.
    fn answer() -> Result<(), AckError>;
}

/// A transport with no acknowledgement at all, answering as `ZeroMQ`, MQTT `QoS 0` and Redis
/// pub/sub answer.
struct Refuses;

impl Settlement for Refuses {
    fn answer() -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }
}

/// The same transport claiming every settlement worked: the stand-in the suite must still catch.
struct Claims;

impl Settlement for Claims {
    fn answer() -> Result<(), AckError> {
        Ok(())
    }
}

/// A transport whose settlement genuinely fails. The suite admits an unsupported settlement, not
/// any answer at all, so this one must still fail.
struct Fails;

impl Settlement for Fails {
    fn answer() -> Result<(), AckError> {
        Err(AckError::Timeout)
    }
}

/// A stand-in transport: the in-memory broker's routing, with `S`'s answer to `ack` and `nack`.
///
/// Every stand-in behaves identically on the wire - a settled delivery is simply gone, and nothing
/// is ever redelivered - so what the suite reads is only what each one claims.
struct Standin<S> {
    broker: MemoryBroker,
    settlement: PhantomData<S>,
}

impl<S> Standin<S> {
    fn new() -> Self {
        Self {
            broker: MemoryBroker::new(),
            settlement: PhantomData,
        }
    }
}

impl<S: Settlement> Broker for Standin<S> {
    type Error = MemoryError;
    type Connected = ConnectedStandin<S>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        self.broker.connect().await.map(|inner| ConnectedStandin {
            inner,
            settlement: PhantomData,
        })
    }
}

/// The connected form of a [`Standin`], which is what the suite drives.
struct ConnectedStandin<S> {
    inner: ConnectedMemoryBroker,
    settlement: PhantomData<S>,
}

impl<S: Settlement> ConnectedBroker for ConnectedStandin<S> {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        self.inner.shutdown()
    }
}

impl<S: Settlement> TestableBroker for ConnectedStandin<S> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.inner.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.inner.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.inner.published(name)
    }
}

impl<S: Settlement> Subscribe for ConnectedStandin<S> {
    type Subscriber = StandinSubscriber<S>;

    fn subscribe(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> + Send {
        let opened = Subscribe::subscribe(&self.inner, name);
        async move {
            opened.await.map(|inner| StandinSubscriber {
                inner,
                settlement: PhantomData,
            })
        }
    }
}

/// The stand-in's subscriber: the memory subscriber's stream, one wrapper per delivery.
struct StandinSubscriber<S> {
    inner: MemorySubscriber,
    settlement: PhantomData<S>,
}

impl<S: Settlement> Subscriber for StandinSubscriber<S> {
    type Message = StandinMessage<S>;
    type Error = <MemorySubscriber as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream().map(|delivery| {
            delivery.map(|inner| StandinMessage {
                inner,
                settlement: PhantomData,
            })
        })
    }
}

/// One delivery from a stand-in, answering settlement as `S` does.
struct StandinMessage<S> {
    inner: MemoryMessage,
    settlement: PhantomData<S>,
}

impl<S: Settlement> IncomingMessage for StandinMessage<S> {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    /// Dropping the wrapped delivery here releases it without requeueing, which is what a
    /// transport that settles nothing does with every message it hands over.
    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        ready(S::answer())
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        ready(S::answer())
    }
}

/// The honest stand-in passes: every routing guarantee is checked, and the settlement it refuses
/// is accepted as an answer rather than failing the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_standin_that_cannot_acknowledge_passes_the_suite() {
    harness::run_suite(Standin::<Refuses>::new).await;
}

/// The liar is still caught, at the one scenario whose assertion IS the settlement: it claims the
/// requeue worked, and the redelivery that claim promises never arrives. The panic this test
/// expects is printed by the run; it is the suite working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack_with_requeue second: stream timed out")]
async fn a_standin_claiming_a_settlement_it_never_performs_fails_the_suite() {
    harness::run_suite(Standin::<Claims>::new).await;
}

/// A settlement that fails for any other reason is still a failed run: what the suite accepts is
/// the one answer that names a transport without acknowledgement, not every error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "ack must succeed or be unsupported, got: Timeout")]
async fn a_standin_whose_settlement_fails_still_fails_the_suite() {
    harness::run_suite(Standin::<Fails>::new).await;
}
