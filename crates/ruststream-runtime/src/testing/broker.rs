//! The broker side of the harness: [`InProcess`], the transition that runs a production broker
//! inside the test process, and [`TestableBroker`], the view of its connected form the
//! [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) harness and the [`conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/index.html) suite drive.

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec;
use core::any::{Any, TypeId, type_name};
use core::fmt;
use core::time::Duration;

use tokio::time;
use tokio::time::Instant;

use crate::{BoxError, InProcessConnect, LocalBoxFuture, SendBroker};
use crate::{Broker, DefaultPublish, OutgoingMessage, PublishPolicy, Publisher, RawMessage};

use super::Coordinator;

/// A broker that runs inside the test process.
///
/// [`connect_in_process`](Self::connect_in_process) is the consuming transition a test
/// takes instead of [`connect`](Broker::connect): it produces the broker's own connected
/// form, backed by an in-process transport, with no I/O. The connected form is the one
/// production gets, so the app's routes, publish policies and subscription descriptors
/// resolve against it unchanged, and
/// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html)
/// runs the service's production app as it is.
///
/// The transport has no configuration of its own. It reads every setting from the broker it
/// was built from (default group, prefetch, topology declaration, whatever the real broker
/// applies), so a test cannot run against settings production does not have. It never
/// succeeds where the real broker fails: a publish, a settlement or a subscription the
/// server would reject is rejected in process the same way.
///
/// A broker crate implements this under its `testing` feature, next to
/// [`TestableBroker`] on the connected form, and registers the broker with
/// [`register_testable_broker!`](crate::register_testable_broker).
/// [`MemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.MemoryBroker.html)
/// is the reference: it has no server, so its in-process transition is its ordinary
/// `connect`.
///
/// The in-process mode lives in the production types: the connected form, its subscriber,
/// its publishers and its delivery each gain an in-process variant that exists only under
/// the `testing` feature. Without the feature there is one variant and no branch, and a
/// service's production build is unchanged, because Cargo does not enable a
/// dev-dependency's features in a production binary. A mount that compiles against the
/// broker compiles in process, and a capability is the same type in both.
///
/// The same test also runs against a stand:
/// [`TestApp::start_live`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start_live)
/// connects the broker through its ordinary `connect` and needs nothing beyond the
/// connected form and its [`DefaultPublish`] policy, through which the harness publishes a
/// test's input.
///
/// # Writing one you can trust
///
/// A service's whole test suite runs against this transport, so every difference from the
/// real one is a green test for behaviour production does not have.
///
/// - Run the
///   [`conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/index.html)
///   suites in process, not only against a server:
///   [`run_suite`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.run_suite.html),
///   then `lifecycle` and the capability suites through
///   [`InProcessBroker`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/struct.InProcessBroker.html).
///   `lifecycle` asks what an in-process transport is rarely asked: whether a publisher
///   created before the shutdown errors afterwards. A transport whose publish is a channel
///   send accepts the message instead.
/// - Settle the way the transport settles. Where the real `ack` answers
///   [`AckError::Unsupported`](crate::AckError::Unsupported), the in-process one answers
///   the same; `Ok(())` there is how a handler returning `retry()` passes in a test and
///   loses the message in production.
/// - Reproduce what the client does; do not fake what the broker does. Competing consumers,
///   group distribution, correlation, reply routing and buffering until commit are
///   client-side or routing-level, and an in-process copy of them is exact. Cluster
///   atomicity, fencing, broker-held timeouts and exactly-once are broker-side, and the
///   live mode covers them. Competing consumers matter most: handing every message of a
///   queue to every subscriber of it is a fan-out, and two workers sharing the queue then
///   each run the whole stream with no error anywhere.
/// - Keep what the real broker keeps. A queue or a log that exists before the subscription
///   delivers what reached it earlier; declare that with [`TestableBroker::backlog`], and
///   read a setting that decides it (an earliest offset, a deliver-all consumer) off the
///   connected form.
///   [`backlog_matches_server`](https://docs.rs/ruststream/latest/ruststream/conformance/in_process/fn.backlog_matches_server.html)
///   checks the declaration against the server.
/// - Read the production wiring. A check the real broker makes at subscribe or publish time
///   (a declaration it refuses, a name too long for the protocol, a header it cannot carry)
///   runs in process through the same code.
/// - Give every gap a comment naming the assertion it makes unsound and what covers it
///   instead, and pin the gap with a test asserting the handler is not reached, which fails
///   the day someone routes what the transport deliberately does not.
///
/// ```text
/// // No fencing: a second producer claiming the same transactional id is not rejected here, so a
/// // test cannot assert the first one is fenced out. `capabilities::transactions` against a real
/// // server is what covers that.
/// ```
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::future::Future;
///
/// use ruststream::Broker;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError};
/// use ruststream::testing::InProcess;
///
/// /// A broker crate built over the in-memory bus: its connected form is the bus's own, so its
/// /// in-process transition is the bus's too.
/// pub struct EdgeBroker(MemoryBroker);
///
/// impl Broker for EdgeBroker {
///     type Error = MemoryError;
///     type Connected = ConnectedMemoryBroker;
///
///     fn connect(
///         self,
///     ) -> impl Future<Output = Result<ConnectedMemoryBroker, MemoryError>> + Send {
///         self.0.connect()
///     }
/// }
///
/// impl InProcess for EdgeBroker {
///     fn connect_in_process(
///         self,
///     ) -> impl Future<Output = Result<ConnectedMemoryBroker, MemoryError>> + Send {
///         self.0.connect_in_process()
///     }
/// }
///
/// // Under the crate's `testing` feature: `TestApp::start` now runs an app on `EdgeBroker`.
/// ruststream::register_testable_broker!(EdgeBroker);
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no in-process mode, so the test harness cannot run it",
    label = "not an in-process broker",
    note = "a broker crate implements `InProcess` under its `testing` feature: a service \
                    enables that feature in `[dev-dependencies]`, a broker author implements the \
                    trait there"
)]
pub trait InProcess: Broker<Connected: LocalTestableBroker> {
    /// Connects to the in-process transport, consuming the configuration.
    ///
    /// # Errors
    ///
    /// Returns the broker's own error where the configuration is one the real broker refuses at
    /// `connect`.
    fn connect_in_process(self) -> impl Future<Output = Result<Self::Connected, Self::Error>>;
}

ruststream_core::__contract_twin! {
    send {
        /// The view of a connected broker the test tooling drives.
        ///
        /// Implemented on the **connected form** a broker's [`InProcess`] transition produces: the
        /// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html)
        /// harness connects every broker of the app in process, then drives it through this, and
        /// [`conformance::harness::run_suite`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.run_suite.html)
        /// checks the routing contract through the same surface.
        ///
        /// The transport answers settlement the way the real transport answers it. A transport that
        /// cannot acknowledge (`ZeroMQ`, MQTT `QoS 0`, Redis pub/sub, Core NATS) reports
        /// [`AckError::Unsupported`](crate::AckError::Unsupported) from its deliveries in
        /// production, so the in-process one reports it too: both the harness and
        /// [`run_suite`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.run_suite.html)
        /// accept that answer, while a transport that claims success makes a handler's retry look
        /// settled in a test and lose the message in production. Either way the delivery is
        /// released to [`Coordinator::consumed`] once (from its `Drop`, which a refused settlement
        /// reaches like any other).
        ///
        /// To plug into the harness, the transport also calls [`Coordinator::enqueued`] on every
        /// live enqueue into a subscriber and [`Coordinator::consumed`] when a delivery is acked,
        /// nacked, or dropped (so the harness can tell when the reaction has settled), and routes
        /// delayed redeliveries through [`Coordinator::schedule_redelivery`].
        ///
        /// [`ConnectedMemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.ConnectedMemoryBroker.html)
        /// is the in-tree reference implementation.
        ///
        /// It is a separate, object-safe capability rather than a
        /// [`ConnectedBroker`](crate::ConnectedBroker) supertrait: the broker traits are not
        /// dyn-compatible, and the harness holds `&dyn TestableBroker`.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// # async fn run() {
        /// use ruststream::conformance::harness;
        /// use ruststream::memory::MemoryBroker;
        ///
        /// // A broker crate's conformance test: the suite drives the connected form through this view
        /// // and checks its answers against what the transport does.
        /// harness::run_suite(MemoryBroker::new).await;
        /// # }
        /// ```
        #[diagnostic::on_unimplemented(
            message = "the connected form `{Self}` cannot be driven by the test harness",
            label = "no `TestableBroker` view",
            note = "a broker's in-process mode implements `TestableBroker` on the connected form \
                    its `InProcess` transition produces"
        )]
        pub trait TestableBroker: Send + Sync
    }
    local {
        #[doc = include_str!("local/testable.md")]
        #[diagnostic::on_unimplemented(
            message = "the connected form `{Self}` cannot be driven by the test harness",
            label = "no `LocalTestableBroker` view",
            note = "the in-process mode of a broker whose types are not `Send` implements \
                    `LocalTestableBroker` on the connected form its `InProcess` transition \
                    produces"
        )]
        pub trait LocalTestableBroker
    }
    blanket { impl<T: TestableBroker + ?Sized> LocalTestableBroker for T }
    forward { <T as TestableBroker> }
    docs { TestableBroker }
    fns {
        /// Installs the harness coordinator into this broker's bus for a test run. Idempotent: a
        /// second install on the same broker is ignored.
        fn install_coordinator(&self, coordinator: Coordinator);

        /// Injects a message onto the bus as an external producer would, synchronously (no
        /// awaiting). Routes through the broker's normal fanout, so it is recorded and counted like
        /// any publish.
        ///
        /// The payload is lent whichever form the transport's own [`Publisher`](crate::Publisher)
        /// declared: an injection is a test writing bytes it holds, so the transport copies them as
        /// it stores them rather than the test building a buffer for it.
        fn inject(&self, message: OutgoingMessage<'_>);

        /// Returns every message published to `name` on this broker, in publish order, after the
        /// [`evicted`](Self::evicted) ones. Backs the harness's `published::<T>(name)` assertions
        /// and [`expect_published`].
        fn published(&self, name: &str) -> [Vec<RawMessage>];

        /// How many of the oldest messages published to `name` this broker no longer holds, so
        /// [`published`](Self::published) starts after them.
        ///
        /// The default, zero, is the answer of a log that keeps every message. A broker whose log
        /// keeps only its newest messages answers how many it dropped: the harness pairs the
        /// options of the publishes it recorded with what the log still holds, and needs to know
        /// where the log starts.
        ///
        /// # Examples
        ///
        /// ```
        /// # struct BoundedBus { dropped: usize }
        /// # impl BoundedBus {
        /// /// A ring that keeps the newest messages of a name counts what it pushed out.
        /// fn evicted(&self, _name: &str) -> usize {
        ///     self.dropped
        /// }
        /// # }
        /// ```
        fn evicted(&self, name: &str) -> [usize] {
            let _ = name;
            0
        }

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
        /// A live
        /// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html)
        /// waits on it: a publish to this broker is owed once by each subscription this answers,
        /// and by no other.
        ///
        /// # Examples
        ///
        /// ```
        /// # struct SubjectBus;
        /// # impl SubjectBus {
        /// /// A subject broker fans a publish out to every subscription whose pattern matches it.
        /// fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        ///     subscriptions
        ///         .iter()
        ///         .enumerate()
        ///         .filter(|(_, pattern)| matches(pattern, destination))
        ///         .map(|(position, _)| position)
        ///         .collect()
        /// }
        /// # }
        /// # fn matches(pattern: &str, subject: &str) -> bool {
        /// #     pattern == subject
        /// #         || pattern.strip_suffix(".>").is_some_and(|prefix| subject.starts_with(prefix))
        /// # }
        /// ```
        fn routes(&self, destination: &str, subscriptions: &[&str]) -> [Vec<usize>] {
            subscriptions
                .iter()
                .enumerate()
                .filter(|(_, name)| **name == destination)
                .map(|(position, _)| position)
                .collect()
        }

        /// What a subscription opened by name receives of the messages published to that name
        /// before it opened, as the real broker answers it: [`Backlog::Missed`] for
        /// publish/subscribe, [`Backlog::Delivered`] for a queue or a log that keeps them.
        ///
        /// The in-process transport behaves as declared, and
        /// [`run_suite`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.run_suite.html)
        /// checks that it does. A setting the broker was configured with is read here, off the
        /// connected form, where it decides the answer (a log read from its earliest offset, a
        /// stream consumer delivering all). The default is [`Backlog::Missed`].
        ///
        /// # Examples
        ///
        /// ```
        /// use ruststream::testing::Backlog;
        ///
        /// # struct LogBus { from_earliest: bool }
        /// # impl LogBus {
        /// /// A consumer group that starts from the earliest offset reads what was published before it
        /// /// joined; one that starts from the latest does not.
        /// fn backlog(&self) -> Backlog {
        ///     if self.from_earliest { Backlog::Delivered } else { Backlog::Missed }
        /// }
        /// # }
        /// ```
        fn backlog(&self) -> [Backlog] {
            Backlog::Missed
        }
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

/// Publishes a test's input onto a connected broker, the connected form erased.
///
/// It is what an injection is in live mode, where no in-process transport takes it. Built for one
/// broker type, which it downcasts the connected form back to.
pub type LivePublish = for<'m> fn(
    &'m (dyn Any + Send + Sync),
    OutgoingMessage<'m>,
) -> LocalBoxFuture<'m, Result<(), BoxError>>;

/// Publishes `msg` onto `connected`, the connected form of `B`, through the broker's own
/// [`DefaultPublish`] policy: the way an external producer reaches a live broker.
#[doc(hidden)]
pub fn live_publish<'m, B>(
    connected: &'m (dyn Any + Send + Sync),
    msg: OutgoingMessage<'m>,
) -> LocalBoxFuture<'m, Result<(), BoxError>>
where
    B: SendBroker,
    B::Connected: DefaultPublish,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<B::Connected>>::Live: Publisher,
{
    let connected = connected.downcast_ref::<B::Connected>().ok_or_else(|| {
        BoxError::from(format!(
            "the live publisher of {} was handed another connected form",
            type_name::<B>(),
        ))
    });
    Box::pin(async move {
        let connected = connected?;
        let publisher = <B::Connected as DefaultPublish>::Policy::default()
            .pair(connected)
            .await
            .map_err(BoxError::from)?;
        let (name, payload, headers) = msg.into_parts();
        publisher
            .publish(
                OutgoingMessage::new(name, payload).with_headers(headers),
                None,
            )
            .await
            .map_err(BoxError::from)
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
        B: InProcess + SendBroker,
        B::Connected: TestableBroker + DefaultPublish,
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
    #[doc(hidden)]
    #[must_use]
    pub fn registers(&self, broker: TypeId) -> bool {
        (self.broker)() == broker
    }

    /// The erased in-process transition of the registered broker type.
    #[doc(hidden)]
    #[must_use]
    pub fn transition(&self) -> InProcessConnect {
        self.connect
    }

    /// How a test's input reaches the registered broker type once it is connected live.
    #[doc(hidden)]
    #[must_use]
    pub fn live(&self) -> LivePublish {
        self.live
    }

    /// Resolves `any` to a `TestableBroker` if it is the connected form of this registration's
    /// broker type.
    #[doc(hidden)]
    pub fn resolve<'a>(&self, any: &'a dyn Any) -> Option<&'a dyn TestableBroker> {
        (self.downcast)(any)
    }
}

/// `B`'s in-process transition over erased values: the harness hands it the broker it found under
/// `B`'s type, and gets the connected form back the same way.
fn connect_erased<B: InProcess + SendBroker>(
    broker: Box<dyn Any + Send>,
) -> LocalBoxFuture<'static, Result<Box<dyn Any + Send>, BoxError>> {
    Box::pin(async move {
        let broker = broker.downcast::<B>().map_err(|_| {
            BoxError::from(format!(
                "the in-process transition of {} was handed another broker",
                type_name::<B>(),
            ))
        })?;
        let connected = broker.connect_in_process().await.map_err(BoxError::from)?;
        let connected: Box<dyn Any + Send> = Box::new(connected);
        Ok(connected)
    })
}

/// The [`TestableBroker`] view of `B`'s connected form, when `any` is one.
fn testable_view<B>(any: &dyn Any) -> Option<&dyn TestableBroker>
where
    B: InProcess + SendBroker,
    B::Connected: TestableBroker,
{
    let connected: &dyn TestableBroker = any.downcast_ref::<B::Connected>()?;
    Some(connected)
}

inventory::collect!(TestableRegistration);

/// Registers a production broker type with the test harness, so
/// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) runs every broker of that type in process.
///
/// It registers two things for the type: its [`InProcess`] transition, which the harness connects
/// it through instead of [`connect`](crate::Broker::connect), and the [`TestableBroker`] view of
/// the connected form that transition produces. Call it once at broker-crate scope, under the
/// crate's `testing` feature, with the type a service builds its app on.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::future::Future;
///
/// use ruststream::Broker;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError};
/// use ruststream::testing::InProcess;
///
/// /// A broker crate built over the in-memory bus: its connected form is the bus's own, so its
/// /// in-process transition is the bus's too.
/// pub struct EdgeBroker(MemoryBroker);
///
/// impl Broker for EdgeBroker {
///     type Error = MemoryError;
///     type Connected = ConnectedMemoryBroker;
///
///     fn connect(
///         self,
///     ) -> impl Future<Output = Result<ConnectedMemoryBroker, MemoryError>> + Send {
///         self.0.connect()
///     }
/// }
///
/// impl InProcess for EdgeBroker {
///     fn connect_in_process(
///         self,
///     ) -> impl Future<Output = Result<ConnectedMemoryBroker, MemoryError>> + Send {
///         self.0.connect_in_process()
///     }
/// }
///
/// ruststream::register_testable_broker!(EdgeBroker);
/// # }
/// # fn main() {}
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
/// messages, not just on length). For application tests prefer [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html), which
/// drives to quiescence without polling; this helper is for tests that run a service via
/// [`run_until`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.run_until) directly.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
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
    let deadline = Instant::now() + timeout;
    loop {
        let observed = broker.published(name);
        if observed.len() >= count || Instant::now() >= deadline {
            return observed;
        }
        time::sleep(Duration::from_millis(2)).await;
    }
}
