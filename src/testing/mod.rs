//! Testing support: the broker-author [`TestClient`] contract and, behind the `testing` feature,
//! the application-level [`TestApp`] harness.
//!
//! Two distinct tools live here:
//!
//! - [`TestClient`] (always available) is the contract a broker crate implements so its in-process
//!   routing can be exercised by the [`conformance`](crate::conformance) harness. It reproduces only
//!   Core routing, never broker-specific semantics.
//! - [`TestApp`] (the `testing` feature) drives a built [`RustStream`](crate::runtime::RustStream)
//!   application in process - no network `connect`, no server - and records what each handler
//!   received, how it settled, and what it published, for unit tests of the application itself.
//!
//! Note that [`MemoryBroker`](crate::memory::MemoryBroker) is a real broker (local in-process
//! queues), not a testing tool; the harness drives it (or any broker) through the same dispatch
//! path the production runtime uses.

use std::{error::Error as StdError, future::Future, time::Duration};

use crate::{Broker, Publisher, RawMessage, Subscriber};

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod harness;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
pub use broker::TestableBroker;
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
pub use harness::{BrokerHandle, TestApp, TestBrokers, TestError};

/// A broker test transport that runs in process, reproducing Core routing without a server.
///
/// Implementations live in the same crate as the broker they stand in for, gated by the
/// `testing` cargo feature. Test code adds the broker crate as a `dev-dependency` with that
/// feature enabled and constructs a `TestClient` to drive the system under test.
pub trait TestClient: Send {
    /// The broker type this test client emulates.
    type Broker: Broker;

    /// The subscriber type opened by [`subscribe`](Self::subscribe).
    type Subscriber: Subscriber;

    /// The publisher type returned by [`publisher`](Self::publisher).
    type Publisher: Publisher;

    /// The error type returned by test-client operations.
    type Error: StdError + Send + Sync + 'static;

    /// Starts a fresh in-memory broker instance.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the test client cannot allocate the required resources.
    fn start() -> impl Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;

    /// Returns a handle to the in-memory broker, suitable for registering with a `RustStream`.
    fn broker(&self) -> &Self::Broker;

    /// Publishes a message to the in-memory broker as if from an external producer.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the in-memory broker rejects the publish.
    fn publish(
        &self,
        name: &str,
        payload: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Opens a subscription against the in-memory broker. Used by integration tests that need
    /// to verify handler-level behaviour like ack / nack and redelivery.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the in-memory broker cannot register a new subscription.
    fn subscribe(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> + Send;

    /// Returns a publisher bound to the in-memory broker. Useful when the test needs to set
    /// headers or publish in a tight loop without allocating per-call closures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the in-memory broker cannot produce a publisher.
    fn publisher(&self) -> impl Future<Output = Result<Self::Publisher, Self::Error>> + Send;

    /// Waits until at least `count` messages have been published to `name`, returning all
    /// observed messages. Fails after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when fewer than `count` messages arrive before the timeout, or
    /// when the in-memory broker is shut down before the assertion completes.
    fn expect_published(
        &self,
        name: &str,
        count: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<RawMessage>, Self::Error>> + Send;

    /// Cleanly shuts down the in-memory broker.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when background tasks fail to terminate.
    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
