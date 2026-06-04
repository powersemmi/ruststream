//! Optional capability traits implemented by brokers that support specific semantics.
//!
//! Brokers implement only the capabilities they natively support. Generic runtime code that
//! depends on a capability adds it as a bound, leaving brokers that do not support it free of
//! emulation cost.

use std::{future::Future, time::Duration};

use futures::Stream;

use crate::{Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber};

/// A subscriber that natively delivers messages in batches.
///
/// Brokers that batch on the wire (`Kafka`, `JetStream` pull consumers) implement this so the
/// runtime can dispatch a whole batch through middleware in one go. Brokers without native
/// batching simply do not implement it.
pub trait BatchSubscriber: Subscriber {
    /// Container yielded by [`batches`]. Implementations choose between [`Vec`], custom
    /// iterators, or anything else that yields the underlying [`Subscriber::Message`].
    ///
    /// [`batches`]: Self::batches
    type Batch: IntoIterator<Item = <Self as Subscriber>::Message> + Send;

    /// Returns a stream of batches.
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel-safe between polls.
    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_;
}

/// A publisher that sends many messages in one call.
///
/// The default implementation awaits [`Publisher::publish`] for each message in order. Brokers
/// whose client coalesces writes (`NATS`, `Kafka` producers) override it to amortize per-call
/// overhead, for example by flushing once after enqueueing the whole batch. Generic code that
/// wants batch semantics adds this as a bound; brokers that gain nothing keep the default.
pub trait BatchPublisher: Publisher {
    /// Publishes every message in `msgs`, preserving iteration order.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on the first message the broker rejects. Messages published before
    /// the failure are not rolled back; use [`TransactionalPublisher`] for all-or-nothing
    /// semantics.
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping the returned future mid-batch may leave a prefix of `msgs`
    /// already published.
    fn publish_batch<'a, I>(&self, msgs: I) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        I: IntoIterator<Item = OutgoingMessage<'a>> + Send,
        I::IntoIter: Send,
    {
        async move {
            for msg in msgs {
                self.publish(msg).await?;
            }
            Ok(())
        }
    }
}

/// A publisher that supports broker-side transactions.
///
/// Implementations must guarantee that messages published between [`begin_transaction`] and
/// [`commit`] either all become visible to subscribers or none of them do.
///
/// [`begin_transaction`]: Self::begin_transaction
/// [`commit`]: Self::commit
pub trait TransactionalPublisher: Publisher {
    /// Begins a new transaction on this publisher.
    ///
    /// Messages published from the same publisher handle after this call are part of the
    /// transaction until [`commit`] or [`abort`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker refuses to start a transaction, for example
    /// because transactions are already in progress or the broker is misconfigured.
    ///
    /// [`commit`]: Self::commit
    /// [`abort`]: Self::abort
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Commits the active transaction, making all buffered messages visible atomically.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the commit. The transaction state after
    /// a failed commit is implementation-defined; implementors must document it.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Aborts the active transaction, discarding all buffered messages.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the abort.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A publisher that supports synchronous request / reply messaging.
///
/// Naturally implemented by `NATS` core and `NATS` `JetStream`'s `req` pattern. Brokers without
/// native reply correlation (`Kafka`, `RabbitMQ` classic queues) do not implement this; users that
/// need request / reply on those transports must emulate it themselves.
pub trait RequestReply: Publisher {
    /// The reply message type.
    type Reply: IncomingMessage;

    /// Publishes `msg` and awaits a single correlated reply, or fails after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the publish, the reply times out, or
    /// the underlying transport fails before a reply arrives.
    fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Self::Reply, Self::Error>> + Send;
}

/// Messages or publishers that carry a routing key for broker-side partitioning.
///
/// Implemented by message types whose broker assigns partitions / shards based on a key
/// (`Kafka`, `NATS` partitioned streams). The router uses this to preserve per-key ordering when
/// dispatching to handlers.
pub trait Partitioned {
    /// Returns the partition key for this item, or `None` if the broker should pick a partition.
    fn partition_key(&self) -> Option<&[u8]>;
}

/// A broker whose subscriptions are fully determined by a topic string.
///
/// This is the common case (`NATS` core subjects, the in-memory broadcast broker, `Redis` pub/sub
/// channels): no consumer group, partition, or durable-consumer configuration is needed to open a
/// subscription, so the runtime can subscribe given just a topic. Brokers whose subscriptions
/// require richer options (`Kafka` consumer groups, `JetStream` durable consumers) do not
/// implement `Subscribe`; callers describe those with a broker-specific
/// [`SubscriptionSource`](crate::SubscriptionSource) instead.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, Subscribe};
///
/// async fn open<B: Subscribe>(broker: &B) -> Result<B::Subscriber, B::Error> {
///     broker.subscribe("orders").await
/// }
/// ```
pub trait Subscribe: Broker {
    /// Opens a subscription to `topic`, producing this broker's [`Subscriber`].
    ///
    /// Called after [`Broker::connect`]; implementations may assume a live connection.
    ///
    /// # Errors
    ///
    /// Returns [`Broker::Error`] when the broker rejects the subscription or the transport fails.
    fn subscribe(
        &self,
        topic: &str,
    ) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> + Send;
}

/// How to reach a broker, for the `servers` section of an `AsyncAPI` document.
///
/// Each broker a service connects to is one `AsyncAPI` server. Construct it directly, or let a
/// broker that implements [`DescribeServer`] build it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerSpec {
    /// The host (and optional port) clients connect to, e.g. `"nats.example.com:4222"`.
    pub host: String,
    /// The messaging protocol, e.g. `"nats"`, `"kafka"`, `"amqp"`.
    pub protocol: String,
    /// An optional human description of this server.
    pub description: Option<String>,
}

impl ServerSpec {
    /// Describes a server reachable at `host` over `protocol`.
    #[must_use]
    pub fn new(host: impl Into<String>, protocol: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            protocol: protocol.into(),
            description: None,
        }
    }

    /// Builder-style setter for the server description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A broker that describes itself as an `AsyncAPI` server.
///
/// Broker crates implement this so their connection coordinates land in the generated `AsyncAPI`
/// document; wire it onto a service with
/// [`RustStream::server`](crate::runtime::RustStream::server). Brokers without a meaningful network
/// address (the in-memory test broker) simply do not implement it.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, DescribeServer, ServerSpec};
///
/// fn describe<B: DescribeServer>(broker: &B) -> ServerSpec {
///     broker.describe_server()
/// }
/// ```
pub trait DescribeServer: Broker {
    /// Returns the server coordinates for this broker.
    fn describe_server(&self) -> ServerSpec;
}
