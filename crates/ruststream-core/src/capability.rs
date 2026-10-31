//! Optional capability traits implemented by brokers that support specific semantics.
//!
//! Brokers implement only the capabilities they natively support. Generic runtime code that
//! depends on a capability adds it as a bound, leaving brokers that do not support it free of
//! emulation cost.

use core::any::type_name;
use core::{error::Error as StdError, num::NonZeroUsize, time::Duration};

use futures_core::Stream;

use crate::subscription::copy_path::Sealed as CopyPathDeclares;
use crate::{
    ConnectedBroker, CopyPath, DeclareRetryError, HeaderMap, IncomingMessage, LocalPublisher,
    LocalSettle, LocalSubscriber, OutgoingFor, PayloadForm, Publisher, RetryDeclaration, Settle,
    Subscriber,
};

crate::__contract_twin! {
    send {
        /// A subscriber that delivers messages in batches.
        ///
        /// Every broker offers this: those that batch on the wire (`Kafka` polls, `JetStream` pull
        /// consumers, `Redis` `XREADGROUP COUNT`) translate the batch size into their client's own
        /// parameter, and those whose transport delivers one message at a time assemble batches on
        /// the client through the [`Buffered`] adapter. The batch a handler sees is exactly the
        /// batch the broker delivered: the runtime never splits or merges one.
        ///
        /// # Implementing it
        ///
        /// The mount site names one number, the batch size, and the runtime passes it to
        /// [`batches`](Self::batches) unchanged. Everything else about how a batch forms (a block
        /// timeout, a consumer group, a prefetch window) is the broker's own vocabulary on its
        /// subscription source, so a service writes the core's word first and the broker's after
        /// it: `b.include(handler.batch(nonzero!(6)).block(Duration::from_secs(5)))`.
        ///
        /// Put the capability on every subscriber a mount can reach. `#[subscriber("topic")]` opens
        /// [`Subscribe::Subscriber`], so a `&[T]` body on that form asks for `BatchSubscriber`
        /// there too; a crate that wires it onto its descriptor's subscriber alone leaves the
        /// string-literal form failing to compile.
        ///
        /// A transport that delivers one message at a time implements it anyway, through
        /// [`Buffered`]. The size is not the broker's to choose; the deadline that closes a partial
        /// batch is. Expose it on the subscription descriptor (`.max_wait(..)`) and hand it to the
        /// wrapper as the subscription opens: the wrapper's 10 ms default suits an in-process bus,
        /// and the broker crates that sit behind a network round trip settle between 10 and 50 ms.
        ///
        /// Declining the capability is legitimate where batching would break a guarantee the
        /// transport carries. A `ZeroMQ` `ROUTER` answers each peer at its own address, while a
        /// whole batch reaches its reply wiring with one context, so a batch's replies would all go
        /// to one peer. A crate that declines says so in its docs: a `&[T]` body then does not
        /// compile on that transport.
        ///
        /// [`conformance::capabilities::batches`](https://docs.rs/ruststream/latest/ruststream/conformance/capabilities/fn.batches.html)
        /// checks that a batch never comes back larger than the size it was opened with. It is not
        /// part of `harness::run_suite`: a crate calls it for this capability itself.
        ///
        /// [`Buffered`]: https://docs.rs/ruststream/latest/ruststream/struct.Buffered.html
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not deliver messages in batches",
            label = "this subscription has no batching of its own",
            note = "a handler taking `&[T]` is handed whole batches: mount it on a subscription \
                    kind that batches. A broker whose transport has no native batches gives its \
                    subscriber this capability through the `Buffered` adapter (see \
                    `BatchSubscriber` and `Buffered`)"
        )]
        pub trait BatchSubscriber: Subscriber
    }
    local {
        #[doc = include_str!("local/batch.md")]
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not deliver messages in batches",
            label = "this subscription has no batching of its own",
            note = "a subscriber whose client receives batches implements `LocalBatchSubscriber`; \
                    every `BatchSubscriber` is one through the blanket impl"
        )]
        pub trait LocalBatchSubscriber: LocalSubscriber
    }
    blanket { impl<T: ?Sized + BatchSubscriber> LocalBatchSubscriber for T }
    forward { <T as BatchSubscriber> }
    docs { BatchSubscriber }
    types {
        /// Container yielded by [`batches`]. Implementations choose between
        /// [`Vec`](alloc::vec::Vec), custom iterators, or anything else that yields the underlying
        /// [`Subscriber::Message`].
        ///
        /// [`batches`]: Self::batches
        type Batch: [
            IntoIterator<Item = <Self as @twin { Subscriber } { LocalSubscriber }>::Message>
                @send { + Send }
        ];
    }
    fns {
        /// Returns a stream of batches of at most `size` messages.
        ///
        /// `size` is the one parameter the framework passes down: it comes from the registration's
        /// [`batch(n)`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.SubscriberSettings.html#method.batch),
        /// which every batch handler names. Everything else about how batches are formed - a block
        /// timeout, a consumer group, a prefetch window - is the broker's own business, configured
        /// on its subscription source.
        ///
        /// A batch must never carry more than `size` messages; it may carry fewer (that is what a
        /// partial batch or a deadline is for). Implementations translate `size` into whatever
        /// their client already speaks (`XREADGROUP COUNT`, a pull batch size, a poll limit); one
        /// with no native batching wraps its subscriber in [`Buffered`], which honours the size on
        /// the client.
        ///
        /// [`Buffered`]: https://docs.rs/ruststream/latest/ruststream/struct.Buffered.html
        ///
        /// # Cancel safety
        ///
        /// Same guarantees as [`Subscriber::stream`]: cancel-safe between polls.
        fn batches(&mut self, size: NonZeroUsize)
            -> [
                impl Stream<
                    Item = Result<
                        Self::Batch,
                        <Self as @twin { Subscriber } { LocalSubscriber }>::Error,
                    >,
                > @send { + Send } + '_
            ];
    }
}

crate::__contract_twin! {
    send {
        /// A subscriber whose position in a replayable log can be moved.
        ///
        /// Implemented only by brokers whose transport can replay (`Kafka` seeks per partition,
        /// `Redis` streams move a group cursor, file-backed logs seek by offset); brokers without a
        /// replayable log simply do not implement it. [`Subscriber::stream`] borrows the subscriber
        /// mutably for the life of the returned stream, and the runtime holds that stream for the
        /// life of the service, so repositioning goes through a [`Seeker`] handle minted before the
        /// stream is opened, the same way publishers are handed out at build time.
        ///
        /// Positions belong to the broker: its crate declares the constructors (earliest, a
        /// timestamp, an offset) on a position type of its own, and a position captured from a
        /// delivery through [`Positioned::position`] pins the contract that seeking to it
        /// redelivers that message. For a handler body to seek, the broker carries the delivery's
        /// position and the subscription's seeker in its per-delivery context and publishes
        /// [`ContextField`](crate::ContextField) keys for them; the batch forms take the seeker
        /// from the batch context, which carries no position. The in-memory broker's
        /// `MemoryContext`, with its `Position` and `SeekHandle` keys, is the model.
        ///
        /// # Examples
        ///
        /// ```
        /// use ruststream::{Seekable, Seeker};
        ///
        /// async fn rewind<S: Seekable>(
        ///     subscriber: &S,
        ///     to: <S::Seeker as Seeker>::Position,
        /// ) -> Result<(), <S::Seeker as Seeker>::Error> {
        ///     subscriber.seeker().seek(to).await
        /// }
        /// ```
        pub trait Seekable: Subscriber
    }
    local {
        #[doc = include_str!("local/seekable.md")]
        pub trait LocalSeekable: LocalSubscriber
    }
    blanket { impl<T: ?Sized + Seekable> LocalSeekable for T }
    forward { <T as Seekable> }
    docs { Seekable }
    types {
        /// The handle usable while this subscriber's stream is running.
        type Seeker: [@twin { Seeker } { LocalSeeker }];
    }
    fns {
        /// Mints a handle for repositioning this subscription.
        fn seeker(&self) -> [Self::Seeker];
    }
}

crate::__contract_twin! {
    send {
        /// A clonable handle that repositions one subscription, minted by [`Seekable::seeker`].
        ///
        /// What one seek covers differs between brokers: a broker whose position lives on the
        /// consumer instance (`Kafka`) moves that instance only, while a broker whose position is a
        /// shared group cursor (`Redis` streams) moves the whole consumer group. Repositioning also
        /// invalidates any acknowledgement bookkeeping the broker keeps for the subscription (a
        /// contiguous-watermark commit tracker must reset). Broker implementations document both.
        pub trait Seeker: Clone + Send + Sync + 'static
    }
    local {
        #[doc = include_str!("local/seeker.md")]
        pub trait LocalSeeker: Clone + 'static
    }
    blanket { impl<T: Seeker> LocalSeeker for T }
    forward { <T as Seeker> }
    docs { Seeker }
    types {
        /// The broker's own position type (`Kafka` partition offsets, a `Redis` entry id, a byte
        /// offset), constructed by the broker crate or captured from a delivered message via
        /// [`Positioned::position`].
        type Position: [@send { Send }];

        /// The error returned when the broker rejects the reposition.
        type Error: [StdError + Send + Sync + 'static];
    }
    fns {
        /// Moves the subscription to `to`; subsequent deliveries resume from there.
        ///
        /// Once the returned future resolves, the next delivery yielded by the subscription
        /// reflects the new position. For a position captured from a delivered message the resume
        /// point is fixed by the [`Positioned`] contract: that message is delivered again. For a
        /// position built by a broker constructor (earliest, a timestamp) the resume point is
        /// defined by the broker's own position documentation.
        ///
        /// # Errors
        ///
        /// Returns `Self::Error` when the broker rejects the reposition or the transport fails.
        fn seek(&self, to: Self::Position)
            -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];
    }
}

/// A delivered message that knows its own position in the broker's replayable log.
///
/// The captured half of the seek capability, next to broker-constructed positions:
/// [`position`](Self::position) returns a value [`Seeker::seek`] accepts, with pinned
/// semantics - seeking to it redelivers this exact message. Generic replay code and the
/// conformance suite rely on that contract; positions built by broker constructors keep
/// broker-documented semantics instead.
///
/// # Examples
///
/// ```
/// use ruststream::{Positioned, Seekable, Seeker};
///
/// async fn replay_from<S>(
///     subscriber: &S,
///     msg: &S::Message,
/// ) -> Result<(), <S::Seeker as Seeker>::Error>
/// where
///     S: Seekable,
///     S::Message: Positioned<Position = <S::Seeker as Seeker>::Position>,
/// {
///     subscriber.seeker().seek(msg.position()).await
/// }
/// ```
pub trait Positioned: IncomingMessage {
    /// The position type, matching the subscription's [`Seeker::Position`].
    type Position;

    /// Returns the position of this delivery; seeking to it redelivers this message.
    fn position(&self) -> Self::Position;
}

crate::__contract_twin! {
    send {
        /// A publisher that supports broker-side transactions.
        ///
        /// Implementations must guarantee that messages published between [`begin_transaction`] and
        /// [`commit`] either all become visible to subscribers or none of them do.
        ///
        /// Misuse is an error, never a silent no-op: a [`commit`] or [`abort`] with no open
        /// transaction, and a [`begin_transaction`] while one is open, must return `Self::Error`. A
        /// caller can therefore trust that `Ok` from `commit` means "an open transaction
        /// committed", not "there was nothing to commit". The
        /// [`conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/index.html)
        /// transactional suite checks these paths.
        ///
        /// This is the borrowed kind of the two transaction capabilities: the handle carries at
        /// most one broker-side transaction, and [`begin_transaction`] takes an exclusive claim on
        /// it. Kafka-like brokers, whose client object holds exactly one transaction per producer,
        /// implement only this kind. Brokers whose transactions are client buffers can additionally
        /// implement [`OwnedTransactions`], the owned kind: every call there opens an independent
        /// buffer-owning [`Transaction`] value, so concurrent transactions on one handle are legal.
        ///
        /// [`begin_transaction`]: Self::begin_transaction
        /// [`commit`]: Self::commit
        /// [`abort`]: Self::abort
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not support broker-side transactions",
            note = "for an `Out<impl TransactionalPublisher, _>` slot, attach a policy whose live \
                    publisher is transactional (a transactional producer configuration)"
        )]
        pub trait TransactionalPublisher: Publisher
    }
    local {
        #[doc = include_str!("local/txpub.md")]
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not support broker-side transactions",
            note = "a live publisher whose client opens transactions implements \
                    `LocalTransactionalPublisher`; every `TransactionalPublisher` is one through \
                    the blanket impl"
        )]
        pub trait LocalTransactionalPublisher: LocalPublisher
    }
    blanket { impl<T: ?Sized + TransactionalPublisher> LocalTransactionalPublisher for T }
    forward { <T as TransactionalPublisher> }
    docs { TransactionalPublisher }
    fns {
        /// Begins a new transaction on this publisher.
        ///
        /// Messages published from the same publisher handle after this call are part of the
        /// transaction until [`commit`] or [`abort`] is called.
        ///
        /// # Errors
        ///
        /// Returns `Self::Error` when a transaction is already open on this handle (a second begin
        /// must not silently join or restart it), or when the broker refuses to start one. A
        /// rejected begin must leave an already-open transaction untouched.
        ///
        /// [`commit`]: Self::commit
        /// [`abort`]: Self::abort
        fn begin_transaction(&self)
            -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];

        /// Commits the active transaction, making all buffered messages visible atomically.
        ///
        /// # Errors
        ///
        /// Returns `Self::Error` when no transaction is open on this handle, or when the broker
        /// rejects the commit. After a failed commit the transaction is closed: the implementation
        /// discards or aborts the broker-side transaction rather than leaving it open, and a
        /// subsequent [`begin_transaction`](Self::begin_transaction) either starts a fresh
        /// transaction or returns an error - the handle must never wedge permanently. Messages of a
        /// failed transaction are lost to this handle; redelivery of the inputs, not resubmission
        /// of the buffer, is the recovery path.
        fn commit(&self) -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];

        /// Aborts the active transaction, discarding all buffered messages.
        ///
        /// # Errors
        ///
        /// Returns `Self::Error` when no transaction is open on this handle, or when the broker
        /// fails to abort.
        fn abort(&self) -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];
    }
}

crate::__contract_twin! {
    send {
        /// A client-buffered transaction that owns its buffer, opened by
        /// [`OwnedTransactions::transaction`].
        ///
        /// [`publish`] appends to the buffer; nothing is visible to subscribers until [`commit`]
        /// flushes the whole buffer atomically, and [`abort`] discards it. Both settle the
        /// transaction by consuming `self`, so a double commit, a commit after an abort, or a
        /// publish after settling are compile errors, not runtime checks. Dropping an unsettled
        /// transaction discards the buffer like an abort (destructors cannot run async work);
        /// implementations log a warning.
        ///
        /// This is the transaction value of the owned kind; see [`OwnedTransactions`] for the
        /// contrast with the borrowed [`TransactionalPublisher`] kind.
        ///
        /// # Examples
        ///
        /// ```
        /// use ruststream::{OutgoingMessage, Transaction};
        ///
        /// async fn settle_pair<T: Transaction>(mut txn: T) -> Result<(), T::Error> {
        ///     txn.publish(OutgoingMessage::new("orders", b"{}".as_slice()), None).await?;
        ///     txn.publish(OutgoingMessage::new("audit", b"{}".as_slice()), None).await?;
        ///     txn.commit().await
        /// }
        /// ```
        ///
        /// [`publish`]: Self::publish
        /// [`commit`]: Self::commit
        /// [`abort`]: Self::abort
        #[must_use = "a transaction does nothing until settled with commit() or abort()"]
        pub trait Transaction: Send
    }
    local {
        #[doc = include_str!("local/tx.md")]
        pub trait LocalTransaction
    }
    blanket { impl<T: Transaction> LocalTransaction for T }
    forward { <T as Transaction> }
    docs { Transaction }
    types {
        /// How this transaction consumes the payload: [`Lend`] where it reads the bytes into a
        /// frame of its own, [`Take`] where the buffer it holds until the commit keeps them.
        ///
        /// A type of its own, for the reason [`Options`](Self::Options) is one: a transaction is a
        /// publish surface of its own, and a client buffer keeps what the direct publish may only
        /// read. See [`Publisher::Payload`](crate::Publisher::Payload).
        ///
        /// [`Lend`]: crate::Lend
        /// [`Take`]: crate::Take
        type Payload: [PayloadForm];

        /// The error type returned by transaction operations.
        type Error: [StdError + Send + Sync + 'static];

        /// The broker's per-message settings inside this transaction: a type of its own, because a
        /// transaction is a publish surface of its own and may honour a different set of settings
        /// than the publisher it was opened from. Most brokers name the publisher's type here.
        ///
        /// See [`Publisher::Options`], including why the type is `Clone + 'static`. A transaction
        /// with no per-message setting writes `type Options = ();`.
        ///
        /// [`Publisher::Options`]: crate::Publisher::Options
        type Options: [Clone + Send + Sync + 'static];
    }
    fns {
        /// Publishes `msg` into the transaction: buffered, not visible before
        /// [`commit`](Self::commit).
        ///
        /// A failed publish does not settle the transaction; the caller decides between retrying
        /// and [`abort`](Self::abort). `options` is what the call site adjusted, and `None` where
        /// nothing did.
        ///
        /// # Errors
        ///
        /// Returns [`Self::Error`] when the message cannot be buffered. For a pure client buffer
        /// this is infallible in practice; implementations that stage broker-side state may reject
        /// here.
        fn publish(&mut self, msg: OutgoingFor<'_, Self::Payload>, options: Option<&Self::Options>)
            -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];

        /// Commits the transaction: the whole buffer becomes visible atomically, in publish order.
        ///
        /// # Errors
        ///
        /// Returns [`Self::Error`] when the flush fails. A failed commit has still consumed the
        /// transaction and its buffer is lost; redelivery of the inputs, not resubmission of the
        /// buffer, is the recovery path (the same rule as [`TransactionalPublisher::commit`]).
        fn commit(self) -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];

        /// Aborts the transaction, discarding the buffer.
        ///
        /// # Errors
        ///
        /// Returns [`Self::Error`] when the implementation fails to discard staged broker-side
        /// state; for a pure client buffer this is infallible in practice.
        fn abort(self) -> [impl Future<Output = Result<(), Self::Error>> @send { + Send }];

        /// The headers this transaction contributes to every message published through the publish
        /// builder, underneath whatever the call site names.
        ///
        /// The transaction-side twin of [`Publisher::base_headers`], with the same default (`None`)
        /// and the same precedence: the call site wins over the transaction, the transaction wins
        /// over nothing. A transaction opened from a handle that carries an argument passes that
        /// argument on here, so a publish behaves the same inside a transaction as outside one.
        ///
        /// # Examples
        ///
        /// ```
        /// use ruststream::{HeaderMap, OutgoingFor, Transaction};
        ///
        /// struct Tagged<T>(T, HeaderMap);
        ///
        /// impl<T: Transaction> Transaction for Tagged<T> {
        ///     type Payload = T::Payload;
        ///     type Error = T::Error;
        ///     type Options = T::Options;
        ///
        ///     async fn publish(
        ///         &mut self,
        ///         msg: OutgoingFor<'_, Self::Payload>,
        ///         options: Option<&Self::Options>,
        ///     ) -> Result<(), Self::Error> {
        ///         self.0.publish(msg, options).await
        ///     }
        ///
        ///     async fn commit(self) -> Result<(), Self::Error> {
        ///         self.0.commit().await
        ///     }
        ///
        ///     async fn abort(self) -> Result<(), Self::Error> {
        ///         self.0.abort().await
        ///     }
        ///
        ///     fn base_headers(&self) -> Option<&HeaderMap> {
        ///         Some(&self.1)
        ///     }
        /// }
        ///
        /// async fn settle<T: Transaction>(txn: T) -> Result<(), T::Error> {
        ///     let base = [("tenant", "acme")].into_iter().collect();
        ///     Tagged(txn, base).commit().await
        /// }
        /// ```
        ///
        /// [`Publisher::base_headers`]: crate::Publisher::base_headers
        fn base_headers(&self) -> [Option<&HeaderMap>] {
            None
        }
    }
}

crate::__contract_twin! {
    send {
        /// A publisher that opens caller-owned, client-buffered transactions.
        ///
        /// This is the owned kind of the two transaction capabilities, the counterpart of the
        /// borrowed [`TransactionalPublisher`]:
        ///
        /// * owned (this trait): every [`transaction`](Self::transaction) call opens its own
        ///   independent transaction, and the returned [`Transaction`] value owns the buffer.
        ///   Double-begin is unrepresentable - there is no shared "the transaction" to collide on -
        ///   and concurrent transactions on one handle are legal.
        /// * borrowed ([`TransactionalPublisher`]): the handle carries the broker's single
        ///   transaction and a begin claims it exclusively, so a second begin while one is open
        ///   errors.
        ///
        /// Implement it when the broker's transactions are client buffers flushed at commit (an
        /// AMQP confirms buffer, a Redis pipeline, the in-memory broker). Kafka-like brokers, whose
        /// client object holds exactly one transaction per producer, implement only the borrowed
        /// kind.
        ///
        /// # Examples
        ///
        /// ```
        /// use ruststream::{OutgoingMessage, OwnedTransactions, Transaction};
        ///
        /// async fn dual_write<P: OwnedTransactions>(
        ///     publisher: &P,
        /// ) -> Result<(), Box<dyn std::error::Error>> {
        ///     let mut orders = publisher.transaction().await?;
        ///     let mut audit = publisher.transaction().await?; // concurrent with `orders`
        ///     orders.publish(OutgoingMessage::new("orders", b"{}".as_slice()), None).await?;
        ///     audit.publish(OutgoingMessage::new("audit", b"{}".as_slice()), None).await?;
        ///     orders.commit().await?;
        ///     audit.commit().await?;
        ///     Ok(())
        /// }
        /// ```
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not open caller-owned transactions",
            note = "for an `Out<impl OwnedTransactions, _>` slot, attach a policy whose live \
                    publisher buffers client-side transactions; Kafka-like brokers offer only the \
                    borrowed `TransactionalPublisher` kind"
        )]
        pub trait OwnedTransactions: Publisher
    }
    local {
        #[doc = include_str!("local/owned.md")]
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not open caller-owned transactions",
            note = "a live publisher that buffers client-side transactions implements \
                    `LocalOwnedTransactions`; every `OwnedTransactions` is one through the blanket \
                    impl"
        )]
        pub trait LocalOwnedTransactions: LocalPublisher
    }
    blanket { impl<T: ?Sized + OwnedTransactions> LocalOwnedTransactions for T }
    forward { <T as OwnedTransactions> }
    docs { OwnedTransactions }
    types {
        /// The buffer-owning transaction opened by [`transaction`](Self::transaction).
        type Transaction: [@twin { Transaction } { LocalTransaction }];
    }
    fns {
        /// Opens a new transaction owned by the returned value.
        ///
        /// Every call opens its own independent transaction: settling one never affects another,
        /// and the handle keeps publishing directly ([`Publisher::publish`]) while any number of
        /// them are open.
        ///
        /// # Errors
        ///
        /// Returns [`Publisher::Error`] when the broker refuses to open a transaction; pure
        /// client-buffer implementations are infallible in practice.
        fn transaction(&self)
            -> [impl Future<Output = Result<Self::Transaction, Self::Error>> @send { + Send }];
    }
}

crate::__contract_twin! {
    send {
        /// A publisher that supports synchronous request / reply messaging.
        ///
        /// Naturally implemented by `NATS` core and `NATS` `JetStream`'s `req` pattern. Brokers
        /// without native reply correlation (`Kafka`, `RabbitMQ` classic queues) do not implement
        /// this; users that need request / reply on those transports must emulate it themselves.
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not support request / reply messaging",
            note = "for an `Out<impl RequestReply, _>` slot, attach a policy whose live publisher \
                    correlates replies natively (NATS-style); Kafka and classic queues do not"
        )]
        pub trait RequestReply: Publisher
    }
    local {
        #[doc = include_str!("local/reqrep.md")]
        #[diagnostic::on_unimplemented(
            message = "`{Self}` does not support request / reply messaging",
            note = "a live publisher whose client correlates replies natively implements \
                    `LocalRequestReply`; every `RequestReply` is one through the blanket impl"
        )]
        pub trait LocalRequestReply: LocalPublisher
    }
    blanket { impl<T: ?Sized + RequestReply> LocalRequestReply for T }
    forward { <T as RequestReply> }
    docs { RequestReply }
    types {
        /// The reply message type.
        type Reply: [@twin { Settle } { LocalSettle }];
    }
    fns {
        /// Publishes `msg` and awaits a single correlated reply, or fails after `timeout`.
        ///
        /// # Errors
        ///
        /// Returns `Self::Error` when the broker rejects the publish, the reply times out, or
        /// the underlying transport fails before a reply arrives.
        fn request(&self, msg: OutgoingFor<'_, Self::Payload>, timeout: Duration)
            -> [impl Future<Output = Result<Self::Reply, Self::Error>> @send { + Send }];
    }
}

/// Messages or publishers that carry a routing key for broker-side partitioning.
///
/// Implemented by message types whose broker assigns partitions / shards based on a key
/// (`Kafka`, `NATS` partitioned streams), so code that holds the concrete type can read the key
/// through a named capability. The runtime places no bound on this trait: keyed dispatch
/// (`workers(n, by_key)`) reads [`IncomingMessage::partition_key`], which a delivery that
/// implements this trait overrides to return the same key.
///
/// A broker's prelude leaves this trait out of its manifest: its method shares a name with the
/// defaulted [`IncomingMessage::partition_key`], which is the one the runtime reads, and a service
/// that needs this trait imports it explicitly.
pub trait Partitioned {
    /// Returns the partition key for this item, or `None` if the broker should pick a partition.
    fn partition_key(&self) -> Option<&[u8]>;
}

/// A connected broker whose subscriptions are fully determined by a name string.
///
/// A broker implements this when a name alone identifies one of its subscriptions: it maps
/// the name onto the subscription kind it treats as its default (a `NATS` core subject, a
/// `Redis` stream). Whatever that kind needs beyond a name, such as a consumer group or a
/// durable subscription name, is configured once on the broker itself; a broker left
/// without that setting rejects the subscription with an error naming the fix rather than
/// inventing a default. Kinds outside that default are described with a broker-specific
/// [`SubscriptionSource`](crate::SubscriptionSource) instead.
///
/// Implemented on the [`ConnectedBroker`](crate::ConnectedBroker) form: a subscription
/// needs a live connection. `#[subscriber("orders")]` and every mount by a bare name
/// subscribe through it, so opening the subscription and saying where a publish reaches it
/// (the [`Copies`](Self::Copies) answer) is all a broker writes for that form; the
/// in-memory broker's implementation is the reference.
///
/// # Examples
///
/// ```
/// use ruststream::Subscribe;
///
/// async fn open<C: Subscribe>(connected: &C) -> Result<C::Subscriber, C::Error> {
///     connected.subscribe("orders").await
/// }
/// ```
pub trait Subscribe: ConnectedBroker {
    /// The subscriber type opened by a by-name subscription.
    type Subscriber: LocalSubscriber;

    /// Who publishes the copies a retry of a by-name subscription is made of, and who names
    /// where they go. This is what the [`Name`](crate::Name) source reports, so it decides how
    /// `#[subscriber("orders")]` retries on this broker.
    ///
    /// Answer [`AddressedCopies`](crate::AddressedCopies) where a publish under a subscribe
    /// name reaches the subscription opened by it - a subject, a topic, a stream, a queue name
    /// usually is both ends - and the name itself is then the address. Answer
    /// [`NamedCopies`](crate::NamedCopies) where it is not: an MQTT filter and a wildcard
    /// subject read many destinations and name none, so the mount site names one. Answer
    /// [`BrokerMoves`](crate::BrokerMoves) where the broker moves a spent delivery itself.
    type Copies: CopyPath;

    /// Opens a subscription to `name`, producing this broker's
    /// [`Subscriber`](Self::Subscriber).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`](crate::ConnectedBroker::Error) when the broker
    /// rejects the subscription or the transport fails.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>>;

    /// Takes what a registration mounted by a bare name declared about its retries, before the
    /// subscription opens.
    ///
    /// Called once per registration whose source is the [`Name`](crate::Name) one, with what
    /// the mount site declared with `max_attempts(..)` and `dead_letter(..)`, at the point a
    /// descriptor is handed the same through
    /// [`SubscriptionSource::declare_retry`](crate::SubscriptionSource::declare_retry). A
    /// broker that applies a delivery limit and a dead-letter destination itself maps them onto
    /// the subscription `name` opens - a Pub/Sub dead-letter policy, an SQS redrive policy, a
    /// Pulsar `DeadLetterPolicy` - and only when both are declared, because a native
    /// dead-letter policy needs the limit and the address together.
    ///
    /// The default accepts a registration that declared nothing, and accepts any declaration
    /// where `Copies`(Self::Copies) says this process publishes the copies: the runtime
    /// applies the cap and the destination there. On a [`BrokerMoves`](crate::BrokerMoves)
    /// broker it refuses a non-empty one, because nothing in this process would apply it and a
    /// bare name carries nowhere else to declare it.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareRetryError::Unsupported`] from the default on a
    /// [`BrokerMoves`](crate::BrokerMoves) broker, and [`DeclareRetryError::Broker`] where an
    /// implementation of your own rejects the declaration. Either fails the registration at
    /// startup.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{DeclareRetryError, RetryDeclaration, Subscribe, nonzero};
    ///
    /// fn cap<C: Subscribe>(connected: &C) -> Result<(), DeclareRetryError> {
    ///     let declared = RetryDeclaration::new()
    ///         .with_max_attempts(nonzero!(5u32))
    ///         .with_dead_letter("orders.dead");
    ///     connected.declare_retry("orders", &declared)?;
    ///     Ok(())
    /// }
    /// ```
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        let _ = name;
        if declaration.declares_nothing() {
            return Ok(());
        }
        // Which arm this is comes from the copy-path type, at compile time. What cannot be
        // settled there is the name: a bare-name registration carries no descriptor for the
        // mount site to read an answer off, so a declaration this broker maps nowhere is
        // refused at startup.
        <Self::Copies as CopyPathDeclares>::declared_by_name(type_name::<Self>())
    }
}

/// A connected broker whose by-name subscriptions open a [`Subscriber`]: the form the tokio runner
/// spawns a subscription loop for.
///
/// [`Subscribe::Subscriber`] is a [`LocalSubscriber`], so a connected form whose types are not
/// `Send` implements [`Subscribe`] as it is. The tokio runner moves the subscriber to a task and
/// asks for this bound where it does; every connected broker whose subscriber is a [`Subscriber`]
/// implements it, so a broker crate writes nothing for it.
///
/// # Examples
///
/// ```
/// use ruststream::{SendSubscribe, Subscriber};
///
/// fn subscriber_of<C: SendSubscribe>(_connected: &C)
/// where
///     C::Subscriber: Subscriber,
/// {
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` opens no by-name subscription the tokio runner can spawn",
    label = "not a connected broker whose subscriber is a `Subscriber`",
    note = "the tokio runner moves a subscriber to a task, so the subscriber's futures must be \
            `Send`: implement `Subscriber` for it",
    note = "a subscriber that is not `Send` implements `LocalSubscriber` and runs on the \
            embedded runner"
)]
pub trait SendSubscribe:
    Subscribe<
    Subscriber: LocalSubscriber<Message: Settle>
                    + Subscriber<
        Message = <<Self as Subscribe>::Subscriber as LocalSubscriber>::Message,
        Error = <<Self as Subscribe>::Subscriber as LocalSubscriber>::Error,
    >,
>
{
}

impl<C> SendSubscribe for C where
    C: Subscribe<
        Subscriber: LocalSubscriber<Message: Settle>
                        + Subscriber<
            Message = <<C as Subscribe>::Subscriber as LocalSubscriber>::Message,
            Error = <<C as Subscribe>::Subscriber as LocalSubscriber>::Error,
        >,
    >
{
}

mod carries;
mod server;

pub use carries::{Carries, CarriesBatch};
pub use server::{ApiKeyLocation, DescribeServer, HttpApiKeyLocation, SecurityScheme, ServerSpec};
