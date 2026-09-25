//! The broker side of the harness: [`InProcess`], the transition that runs a production broker
//! inside the test process, and [`TestableBroker`], the view of its connected form the
//! [`TestApp`](super::TestApp) harness and the [`conformance`](crate::conformance) suite drive.

use std::any::{Any, TypeId, type_name};
use std::fmt;
use std::future::Future;
use std::time::Duration;

use crate::runtime::{BoxError, BoxFuture, InProcessConnect};
use crate::{Broker, DefaultPublish, OutgoingMessage, PublishPolicy, Publisher, RawMessage};

use super::Coordinator;

/// A broker that runs inside the test process.
///
/// [`connect_in_process`](Self::connect_in_process) is the consuming transition a test takes
/// instead of [`connect`](Broker::connect): it produces the broker's own connected form, backed
/// by an in-process transport, with no I/O. The connected form is the one production gets, so the
/// app's routes, publish policies and subscription descriptors resolve against it unchanged, and
/// [`TestApp`](super::TestApp) runs the service's production app as it is.
///
/// The transport has no configuration of its own. It reads every setting from the broker it was
/// built from (default group, prefetch, topology declaration, whatever the real broker applies),
/// so a test cannot run against settings production does not have. It never succeeds where the
/// real broker fails: a publish, a settlement or a subscription the server would reject is
/// rejected in process the same way.
///
/// A broker crate implements this under its `testing` feature, next to
/// [`TestableBroker`] on the connected form, and registers the broker with
/// [`register_testable_broker!`](crate::register_testable_broker).
/// [`MemoryBroker`](crate::memory::MemoryBroker) is the reference: it has no server, so its
/// in-process transition is its ordinary `connect`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::{InProcess, TestableBroker};
///
/// let connected = MemoryBroker::new().connect_in_process().await?;
/// assert!(connected.published("orders").is_empty());
/// # Ok(())
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no in-process mode, so the test harness cannot run it",
    label = "not an in-process broker",
    note = "a broker crate implements `InProcess` under its `testing` feature: a service enables \
            that feature in `[dev-dependencies]`, a broker author implements the trait there"
)]
pub trait InProcess: Broker<Connected: TestableBroker> {
    /// Connects to the in-process transport, consuming the configuration.
    ///
    /// # Errors
    ///
    /// Returns the broker's own error where the configuration is one the real broker refuses at
    /// `connect`.
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send;
}

/// The view of a connected broker the test tooling drives.
///
/// Implemented on the **connected form** a broker's [`InProcess`] transition produces: the
/// [`TestApp`](super::TestApp) harness connects every broker of the app in process, then drives
/// it through this, and
/// [`conformance::harness::run_suite`](crate::conformance::harness::run_suite) checks the
/// routing contract through the same surface.
///
/// The transport answers settlement the way the real transport answers it. A transport that cannot
/// acknowledge (`ZeroMQ`, MQTT `QoS 0`, Redis pub/sub, Core NATS) reports
/// [`AckError::Unsupported`](crate::AckError::Unsupported) from its deliveries in production, so
/// the in-process one reports it too: both the harness and
/// [`run_suite`](crate::conformance::harness::run_suite) accept that answer, while a transport that
/// claims success makes a handler's retry look settled in a test and lose the message in
/// production. Either way the delivery is released to [`Coordinator::consumed`] once (from its
/// `Drop`, which a refused settlement reaches like any other).
///
/// To plug into the harness, the transport also calls [`Coordinator::enqueued`] on every live
/// enqueue into a subscriber and [`Coordinator::consumed`] when a delivery is acked, nacked, or
/// dropped (so the harness can tell when the reaction has settled), and routes delayed
/// redeliveries through [`Coordinator::schedule_redelivery`].
///
/// [`ConnectedMemoryBroker`](crate::memory::ConnectedMemoryBroker) is the in-tree reference
/// implementation.
///
/// It is a separate, object-safe capability rather than a
/// [`ConnectedBroker`](crate::ConnectedBroker) supertrait: the broker traits are not
/// dyn-compatible, and the harness holds `&dyn TestableBroker`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::{InProcess, TestableBroker};
///
/// fn published<B: TestableBroker>(broker: &B, name: &str) -> usize {
///     broker.published(name).len()
/// }
///
/// let connected = MemoryBroker::new().connect_in_process().await?;
/// assert_eq!(published(&connected, "orders"), 0);
/// # Ok(())
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "the connected form `{Self}` cannot be driven by the test harness",
    label = "no `TestableBroker` view",
    note = "a broker's in-process mode implements `TestableBroker` on the connected form its \
            `InProcess` transition produces"
)]
pub trait TestableBroker: Send + Sync {
    /// Installs the harness coordinator into this broker's bus for a test run. Idempotent: a second
    /// install on the same broker is ignored.
    fn install_coordinator(&self, coordinator: Coordinator);

    /// Injects a message onto the bus as an external producer would, synchronously (no awaiting).
    /// Routes through the broker's normal fanout, so it is recorded and counted like any publish.
    ///
    /// The payload is lent whichever form the transport's own [`Publisher`](crate::Publisher)
    /// declared: an injection is a test writing bytes it holds, so the transport copies them as
    /// it stores them rather than the test building a buffer for it.
    fn inject(&self, message: OutgoingMessage<'_>);

    /// Returns every message published to `name` on this broker, in publish order. Backs the
    /// harness's `published::<T>(name)` assertions and [`expect_published`].
    fn published(&self, name: &str) -> Vec<RawMessage>;

    /// Which subscriptions of this broker a message published to `destination` is delivered to,
    /// as the broker's own routing picks them: the positions in `subscriptions` (the names the
    /// app subscribed under on this broker) of the ones it delivers to.
    ///
    /// The answer is the broker's routing, not a guess about it, and a setting the broker was
    /// configured with is read here, off the connected form. The default delivers to every
    /// subscription of the destination's name. A broker whose subscription names are patterns
    /// answers with its own rule: every matching subscription where it fans out to all of them
    /// (NATS subjects), or the one it picks where it picks one.
    ///
    /// A live [`TestApp`](super::TestApp) waits on it: a publish to this broker is owed once by
    /// each subscription this answers, and by no other.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::testing::{InProcess, TestableBroker};
    ///
    /// let connected = MemoryBroker::new().connect_in_process().await?;
    /// let subscriptions = ["orders", "orders.eu", "orders"];
    /// assert_eq!(connected.routes("orders", &subscriptions), [0, 2]);
    /// # Ok(())
    /// # }
    /// ```
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        subscriptions
            .iter()
            .enumerate()
            .filter(|(_, name)| **name == destination)
            .map(|(position, _)| position)
            .collect()
    }

    /// What a subscription opened by name receives of the messages published to that name before
    /// it opened, as the real broker answers it: [`Backlog::Missed`] for publish/subscribe,
    /// [`Backlog::Delivered`] for a queue or a log that keeps them.
    ///
    /// The in-process transport behaves as declared, and
    /// [`run_suite`](crate::conformance::harness::run_suite) checks that it does. A setting the
    /// broker was configured with is read here, off the connected form, where it decides the
    /// answer (a log read from its earliest offset, a stream consumer delivering all). The
    /// default is [`Backlog::Missed`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::testing::{Backlog, InProcess, TestableBroker};
    ///
    /// let connected = MemoryBroker::new().connect_in_process().await?;
    /// assert_eq!(connected.backlog(), Backlog::Missed);
    /// # Ok(())
    /// # }
    /// ```
    fn backlog(&self) -> Backlog {
        Backlog::Missed
    }
}

/// What a subscription opened by name receives of the messages published to that name before it
/// opened: the answer a broker gives through [`TestableBroker::backlog`].
///
/// # Examples
///
/// A queue broker's in-process transport keeps what reaches an existing queue with no consumer yet,
/// as the server does, and says so:
///
/// ```
/// use ruststream::testing::Backlog;
///
/// # struct QueueBus;
/// # impl QueueBus {
/// fn backlog(&self) -> Backlog {
///     Backlog::Delivered
/// }
/// # }
/// # assert_eq!(QueueBus.backlog(), Backlog::Delivered);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Backlog {
    /// Publish/subscribe: a subscription receives only what is published after it opens (Core
    /// NATS, Redis pub/sub, `ZeroMQ` PUB/SUB, the memory broker).
    Missed,
    /// A queue or a log that exists before the publish: what was published before the subscription
    /// opened is kept, and the subscription receives it first, in publish order (SQS, `RabbitMQ`
    /// queues declared ahead of their consumer, `JetStream` streams, Redis streams and lists, Kafka
    /// read from the earliest offset). A queue the subscription itself declares does not exist when
    /// the earlier message is published, so the server drops it and the answer is `Missed`.
    Delivered,
}

/// The harness's record of one [`InProcess`] broker type: how to recognise it in a built app, how
/// to connect it in process, and how to reach the [`TestableBroker`] view of what that produced.
///
/// Built by [`register_testable_broker!`](crate::register_testable_broker), once per broker type;
/// the harness looks each broker of the app up here by its type.
pub struct TestableRegistration {
    broker: fn() -> TypeId,
    connect: InProcessConnect,
    downcast: fn(&dyn Any) -> Option<&dyn TestableBroker>,
    live: LivePublish,
}

/// Publishes a test's input onto a connected broker, the connected form erased: what an injection
/// is in live mode, where no in-process transport takes it. Built for one broker type, which it
/// downcasts the connected form back to.
pub(crate) type LivePublish = for<'m> fn(
    &'m (dyn Any + Send + Sync),
    OutgoingMessage<'m>,
) -> BoxFuture<'m, Result<(), BoxError>>;

/// Publishes `msg` onto `connected`, the connected form of `B`, through the broker's own
/// [`DefaultPublish`] policy: the way an external producer reaches a live broker.
pub(crate) fn live_publish<'m, B>(
    connected: &'m (dyn Any + Send + Sync),
    msg: OutgoingMessage<'m>,
) -> BoxFuture<'m, Result<(), BoxError>>
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<B::Connected>>::Live: Publisher,
{
    let connected = connected.downcast_ref::<B::Connected>().ok_or_else(|| {
        Box::from(format!(
            "the live publisher of {} was handed another connected form",
            type_name::<B>(),
        )) as BoxError
    });
    Box::pin(async move {
        let connected = connected?;
        let publisher = <B::Connected as DefaultPublish>::Policy::default()
            .pair(connected)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
        let (name, payload, headers) = msg.into_parts();
        publisher
            .publish(
                OutgoingMessage::new(name, payload).with_headers(headers),
                None,
            )
            .await
            .map_err(|err| Box::new(err) as BoxError)
    })
}

impl fmt::Debug for TestableRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestableRegistration")
            .finish_non_exhaustive()
    }
}

impl TestableRegistration {
    /// The registration of the broker type `B` (the
    /// [`register_testable_broker!`](crate::register_testable_broker) macro builds this for you).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::testing::TestableRegistration;
    ///
    /// let registration = TestableRegistration::of::<MemoryBroker>();
    /// # let _ = registration;
    /// # }
    /// ```
    #[must_use]
    pub const fn of<B>() -> Self
    where
        B: InProcess + 'static,
        B::Connected: DefaultPublish,
        <<B::Connected as DefaultPublish>::Policy as PublishPolicy<B::Connected>>::Live: Publisher,
    {
        Self {
            broker: TypeId::of::<B>,
            connect: connect_erased::<B>,
            downcast: testable_view::<B>,
            live: live_publish::<B>,
        }
    }

    /// Whether this registration is the one for the broker type `broker`.
    pub(crate) fn registers(&self, broker: TypeId) -> bool {
        (self.broker)() == broker
    }

    /// The erased in-process transition of the registered broker type.
    pub(crate) fn transition(&self) -> InProcessConnect {
        self.connect
    }

    /// How a test's input reaches the registered broker type once it is connected live.
    pub(crate) fn live(&self) -> LivePublish {
        self.live
    }

    /// Resolves `any` to a `TestableBroker` if it is the connected form of this registration's
    /// broker type.
    pub(crate) fn resolve<'a>(&self, any: &'a dyn Any) -> Option<&'a dyn TestableBroker> {
        (self.downcast)(any)
    }
}

/// `B`'s in-process transition over erased values: the harness hands it the broker it found under
/// `B`'s type, and gets the connected form back the same way.
fn connect_erased<B: InProcess + 'static>(
    broker: Box<dyn Any + Send>,
) -> BoxFuture<'static, Result<Box<dyn Any + Send>, BoxError>> {
    Box::pin(async move {
        let broker = broker.downcast::<B>().map_err(|_| {
            Box::from(format!(
                "the in-process transition of {} was handed another broker",
                type_name::<B>(),
            )) as BoxError
        })?;
        let connected = broker
            .connect_in_process()
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
        Ok(Box::new(connected) as Box<dyn Any + Send>)
    })
}

/// The [`TestableBroker`] view of `B`'s connected form, when `any` is one.
fn testable_view<B: InProcess + 'static>(any: &dyn Any) -> Option<&dyn TestableBroker> {
    any.downcast_ref::<B::Connected>()
        .map(|connected| connected as &dyn TestableBroker)
}

inventory::collect!(TestableRegistration);

/// Registers a production broker type with the test harness, so
/// [`TestApp`](crate::testing::TestApp) runs every broker of that type in process.
///
/// It registers two things for the type: its [`InProcess`] transition, which the harness connects
/// it through instead of [`connect`](crate::Broker::connect), and the [`TestableBroker`] view of
/// the connected form that transition produces. Call it once at broker-crate scope, under the
/// crate's `testing` feature, with the type a service builds its app on.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # {
/// use ruststream::memory::MemoryBroker;
/// // The in-tree `MemoryBroker` is already registered; a broker crate registers its own type:
/// // ruststream::register_testable_broker!(MyBroker);
/// # let _ = MemoryBroker::new();
/// # }
/// ```
#[macro_export]
macro_rules! register_testable_broker {
    ($broker:ty) => {
        $crate::inventory::submit! {
            $crate::testing::TestableRegistration::of::<$broker>()
        }
    };
}

/// Waits until at least `count` messages have been published to `name` on `broker`.
///
/// Returns all observed messages; on timeout it returns those seen so far (so assert on the returned
/// messages, not just on length). For application tests prefer [`TestApp`](super::TestApp), which
/// drives to quiescence without polling; this helper is for tests that run a service via
/// [`run_until`](crate::runtime::RustStream::run_until) directly.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
/// use std::time::Duration;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::{InProcess, TestableBroker, expect_published};
/// use ruststream::OutgoingMessage;
///
/// let connected = MemoryBroker::new().connect_in_process().await?;
/// connected.inject(OutgoingMessage::new("out", b"x".as_slice()));
/// let seen = expect_published(&connected, "out", 1, Duration::from_secs(1)).await;
/// assert_eq!(seen.len(), 1);
/// # Ok(())
/// # }
/// ```
pub async fn expect_published<B: TestableBroker>(
    broker: &B,
    name: &str,
    count: usize,
    timeout: Duration,
) -> Vec<RawMessage> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let observed = broker.published(name);
        if observed.len() >= count || tokio::time::Instant::now() >= deadline {
            return observed;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
