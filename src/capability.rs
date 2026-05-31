//! Optional capability traits implemented by brokers that support specific semantics.
//!
//! Brokers implement only the capabilities they natively support. Generic runtime code that
//! depends on a capability adds it as a bound, leaving brokers that do not support it free of
//! emulation cost.

use std::{future::Future, time::Duration};

use futures::Stream;

use crate::{IncomingMessage, OutgoingMessage, Publisher, Subscriber};

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
