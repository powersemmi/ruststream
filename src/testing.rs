//! The [`TestClient`] trait: contract for broker-specific in-memory test doubles.
//!
//! Broker crates implement `TestClient` under their `testing` cargo feature so application
//! developers can write integration tests for their handlers without a real broker. Each
//! implementation honours the real broker's semantics (`JetStream` ack, `Kafka` offsets,
//! `RabbitMQ` exchanges and DLX, etc.).

use std::{error::Error as StdError, future::Future, time::Duration};

use crate::{Broker, RawMessage};

/// A broker test double that runs in memory while mimicking the real broker's semantics.
///
/// Implementations live in the same crate as the broker they emulate, gated by the `testing`
/// cargo feature. Test code adds the broker crate as a `dev-dependency` with that feature
/// enabled and constructs a `TestClient` to drive the system under test.
pub trait TestClient: Send {
    /// The broker type this test client emulates.
    type Broker: Broker;

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

    /// Returns a handle to the in-memory broker, suitable for passing to a `Router`.
    fn broker(&self) -> &Self::Broker;

    /// Publishes a message to the in-memory broker as if from an external producer.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the in-memory broker rejects the publish.
    fn publish(
        &self,
        topic: &str,
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
        topic: &str,
    ) -> impl Future<Output = Result<<Self::Broker as Broker>::Subscriber, Self::Error>> + Send;

    /// Returns a publisher bound to the in-memory broker. Useful when the test needs to set
    /// headers or publish in a tight loop without allocating per-call closures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the in-memory broker cannot produce a publisher.
    fn publisher(
        &self,
    ) -> impl Future<Output = Result<<Self::Broker as Broker>::Publisher, Self::Error>> + Send;

    /// Waits until at least `count` messages have been published to `topic`, returning all
    /// observed messages. Fails after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when fewer than `count` messages arrive before the timeout, or
    /// when the in-memory broker is shut down before the assertion completes.
    fn expect_published(
        &self,
        topic: &str,
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
