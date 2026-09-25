//! Conformance test suite that any broker's in-process mode must pass, and the ladder checks
//! every broker must pass.
//!
//! Broker authors prove their in-process transport honours Core routing by running
//! [`run_suite`] against their production broker: each scenario starts from a fresh broker
//! produced by the caller-supplied factory, connects it through its
//! [`InProcess`] transition and drives the connected form through the broker's own
//! [`Subscribe`] / [`TestableBroker::inject`] surface - no server.
//!
//! # Examples
//!
//! The example uses [`crate::memory::MemoryBroker`], so it needs the `memory` feature; a broker
//! crate passes its own production broker here.
//!
//! ```no_run
//! # #[cfg(all(feature = "testing", feature = "memory"))]
//! # async fn run() {
//! use ruststream::{conformance::harness, memory::MemoryBroker};
//!
//! harness::run_suite(MemoryBroker::new).await;
//! # }
//! ```

use std::{fmt, future::Future, thread, time::Duration};

use super::helpers::unique_subject;
#[cfg(feature = "asyncapi")]
use crate::DescribeServer;
#[cfg(feature = "asyncapi")]
use crate::asyncapi::build_spec;
#[cfg(feature = "asyncapi")]
use crate::runtime::{AppInfo, RustStream};
use crate::{
    AckError, Broker, Connected, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage,
    Publisher, RedeliveryAddressed, Subscribe, Subscriber, SubscriptionSource,
    testing::{Backlog, InProcess, TestableBroker},
};
use bytes::Bytes;
use futures::StreamExt;
use tokio::{runtime, sync::oneshot, time::timeout};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const NEGATIVE_WAIT: Duration = Duration::from_millis(100);
/// The delay [`lifecycle`] settles with. Short, and still a real timer on every broker.
const REDELIVERY_DELAY: Duration = Duration::from_millis(200);
/// How long past the delay [`lifecycle`] waits for the redelivery: brokers whose delays have
/// second granularity round the short delay up.
const REDELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// The message type a subscriber yields.
type SubscriberMessage<S> = <S as Subscriber>::Message;

/// Runs every scenario in the suite, panicking with a descriptive message on the first failure.
///
/// `factory` is invoked once per scenario to obtain a fresh broker, so tests cannot leak state
/// between each other. It builds the production broker, the one a service builds its app on;
/// each scenario connects it through its [`InProcess`] transition and drives the connected form
/// through the broker's own [`Subscribe`] / [`TestableBroker::inject`] surface.
///
/// This is the routing contract and nothing else. A capability
/// ([`BatchSubscriber`](crate::BatchSubscriber),
/// [`RequestReply`](crate::RequestReply), [`TransactionalPublisher`](crate::TransactionalPublisher),
/// [`OwnedTransactions`](crate::OwnedTransactions), [`Seekable`](crate::Seekable)) has a suite of
/// its own in [`capabilities`](super::capabilities), which the broker calls for each capability it
/// implements: none of them can be folded in here, because the bound would either exclude every
/// broker that declines the capability or demand the in-process transport implement it. So a
/// broker running `run_suite` alone has not checked its batches - `capabilities::batches` is the
/// call that does, against the broker's own subscription source.
///
/// # A message published before the subscription opened
///
/// A publish/subscribe transport delivers a subscription only what is published after it opens,
/// while a queue or a log keeps what reached it earlier and delivers that first. The broker
/// declares which through [`TestableBroker::backlog`], and the suite checks the declared behaviour
/// both ways: the earlier message arrives first under [`Backlog::Delivered`] and never under
/// [`Backlog::Missed`], the default.
///
/// # Transports with no acknowledgement
///
/// A transport that cannot acknowledge (`ZeroMQ`, MQTT `QoS 0`, Redis pub/sub, Core NATS) reports
/// [`AckError::Unsupported`] from `ack` and `nack`, and the suite accepts that answer wherever the
/// capability suites already do - so the in-process transport can answer exactly as the real one
/// does instead of claiming a settlement production never performs. The redelivery scenario is the
/// one that cannot be checked against such a transport: `nack(requeue = true)` reporting
/// `Unsupported` is a transport that has no redelivery to observe, so the scenario ends there
/// rather than accepting any answer. Everything else stays asserted, the drop scenario included: a
/// delivery nobody can settle is still a delivery that must not come back.
///
/// The answer is read from the delivery, not from the broker, so it can differ per subscription
/// and per message the way the transport does: a requeue that is advisory under one commit mode
/// and rewinding under another, an acknowledgement a transport gives at one quality of service and
/// not at another. What the suite holds either way is the meaning of a success:
/// `Ok(())` from `nack(requeue = true)` promises the message comes back.
///
/// # Panics
///
/// Panics if any scenario fails an assertion. The panic message identifies the scenario.
pub async fn run_suite<B, F>(factory: F)
where
    B: InProcess,
    B::Connected: Subscribe,
    F: Fn() -> B,
{
    let connect = async move |broker: B| {
        broker
            .connect_in_process()
            .await
            .expect("broker must connect in process before a suite scenario")
    };
    ordering(connect(factory()).await).await;
    publish_before_subscribe(connect(factory()).await).await;
    ack_consumes_delivery(connect(factory()).await).await;
    nack_with_requeue_redelivers(connect(factory()).await).await;
    nack_without_requeue_drops(connect(factory()).await).await;
    headers_propagate(connect(factory()).await).await;
    published_log_observes_publishes(connect(factory()).await).await;
}

/// A broker whose `connect` is its [`InProcess`] transition, so the suites that take any
/// [`Broker`] run against the in-process transport.
///
/// [`lifecycle`], [`redelivery_address`] and the [`capabilities`](super::capabilities) suites
/// connect the broker they are handed with [`Broker::connect`], which is what their live pass
/// needs. Wrapping the production broker in this runs the same suite a second time without a
/// server: the connected form, and so every descriptor and publish policy, is the production one,
/// only the transport underneath is the in-process one.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::harness::{self, InProcessBroker};
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// harness::lifecycle(
///     || InProcessBroker::new(MemoryBroker::new()),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
/// )
/// .await;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct InProcessBroker<B>(B);

impl<B> InProcessBroker<B> {
    /// Wraps `broker`, the production broker a service builds its app on.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::Broker;
    /// use ruststream::conformance::harness::InProcessBroker;
    /// use ruststream::memory::MemoryBroker;
    ///
    /// // `connect` runs the broker's in-process transition: no server behind it.
    /// let connected = InProcessBroker::new(MemoryBroker::new()).connect().await?;
    /// # let _ = connected;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn new(broker: B) -> Self {
        Self(broker)
    }
}

impl<B: InProcess> Broker for InProcessBroker<B> {
    type Error = B::Error;
    type Connected = B::Connected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.0.connect_in_process()
    }
}

/// Verifies a broker honours the lifecycle ladder end to end.
///
/// The steps are: synchronous construction (no I/O in the constructor), then the consuming
/// `connect` producing the typed connected form, a subscription opened through the broker's own
/// [`SubscriptionSource`], a publish the subscription receives and acks (or reports
/// [`AckError::Unsupported`] for a broker with no ack semantics), then the consuming `shutdown`
/// producing the terminal witness. Owner-side misuse after shutdown is a compile error under the
/// ladder, so what remains checkable at runtime is the aliased-handle contract: a publisher
/// created before the shutdown must error afterwards, never silently succeed against a dead
/// connection.
///
/// The publish and the settlements come from a current-thread runtime on a thread of its own,
/// which stops before the suite goes on, the way a handler on a dedicated thread publishes and
/// settles. Where the delivery offers
/// [`nack_after`](IncomingMessage::nack_after), it is settled that way first and must come back
/// once the delay runs out: a broker that ran the delay on the settling caller's runtime has lost
/// it with that runtime. That is the rule that a broker's internal tasks run on the runtime it
/// connected on (see [`Broker`]).
///
/// A descriptor that addresses its own retry copies is held to that address by
/// [`redelivery_address`], a suite of its own:
/// a publish there must reach the subscription that reported it, because that is what the runtime
/// does with a delayed message.
///
/// The three factories keep the check broker-agnostic:
/// * `make_broker` is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
///   cannot satisfy it, which is exactly the contract: construct cheaply, connect in
///   [`Broker::connect`].
/// * `make_source` builds the broker's subscription descriptor for a subject (the macro-subscriber
///   path). The descriptor is `Clone`: it is configuration, and the mount rebuilds it per
///   registration, so a definition can be mounted on more than one broker.
/// * `make_publisher` produces a publisher from the connected form. The publisher and the
///   delivery move to the other runtime's thread, so both are `'static`.
///
/// Run it from the broker crate against a real server, and a second time in process by wrapping
/// the production broker in [`InProcessBroker`]. The subject it publishes under is unique per run
/// (see [`unique_subject`]), so a server that keeps what an earlier run left - a retained log, a
/// durable queue - does not fail the next one.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::{conformance::harness, memory::{MemoryBroker, MemorySource}};
///
/// harness::lifecycle(
///     || MemoryBroker::new(),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if construction, connection, subscription, delivery, ack,
/// the delayed redelivery, shutdown, or the aliased-handle behaviour does not follow the
/// contract.
pub async fn lifecycle<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Clone + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    SubscriberMessage<Src::Subscriber>: 'static,
    Pub: Publisher + 'static,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.lifecycle");

    let connected = make_broker()
        .connect()
        .await
        .expect("broker must connect after synchronous construction");

    let source = make_source(&subject);
    let mut subscriber = source
        .clone()
        .subscribe(&connected)
        .await
        .expect("subscription source must open against the connected form");
    let publisher = make_publisher(&connected);

    // Published from a runtime that stops before the delivery is read, the way a handler on a
    // dedicated thread publishes: what the publish left to finish runs on the broker's runtime.
    let destination = subject.clone();
    let publisher = on_foreign_runtime(async move || {
        publisher
            .publish(
                OutgoingMessage::new(&destination, b"lifecycle".as_slice()),
                None,
            )
            .await
            .expect("publish after connect failed");
        publisher
    })
    .await;

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "lifecycle").await;
    assert_eq!(
        msg.payload(),
        b"lifecycle",
        "subscription opened through SubscriptionSource must receive the publish",
    );
    let msg = if msg.supports_nack_after() {
        // Settled from a runtime that stops at once: the delay has to run out on the runtime the
        // broker connected on, or the message never comes back.
        on_foreign_runtime(async move || {
            msg.nack_after(REDELIVERY_DELAY)
                .await
                .expect("a delayed nack the delivery offers must be accepted");
        })
        .await;
        let again = timeout(REDELIVERY_DELAY + REDELIVERY_TIMEOUT, stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "lifecycle: a delayed nack settled from another runtime never came back; the \
                     broker must run its internal tasks on the runtime it connected on",
                )
            })
            .expect("lifecycle: stream ended before the delayed redelivery")
            .unwrap_or_else(|err| panic!("lifecycle: stream yielded error: {err:?}"));
        assert_eq!(
            again.payload(),
            b"lifecycle",
            "a delayed nack must redeliver the same message",
        );
        again
    } else {
        msg
    };
    // Ack must either succeed or be explicitly unsupported (a broker with no ack semantics, e.g.
    // Core NATS). Any other ack error is a real failure. Settled from another runtime as well.
    on_foreign_runtime(async move || match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    })
    .await;

    let _closed = connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    // The ladder makes owner-side misuse unrepresentable; the aliased publisher created before
    // the shutdown is the surface that must stay honest at runtime.
    assert!(
        publisher
            .publish(
                OutgoingMessage::new(&subject, b"post-shutdown".as_slice()),
                None,
            )
            .await
            .is_err(),
        "publish through a handle aliasing the closed connection must error",
    );
}

/// What a descriptor that addresses its own retry copies promises: publish to the address it
/// reports and the subscription that reported it gets the message.
///
/// The runtime publishes a `retry_after` copy exactly like this, so an address that reaches
/// nothing would lose every delayed message. Only a descriptor declaring
/// [`AddressedCopies`](crate::AddressedCopies) has one; a
/// [`NamedCopies`](crate::NamedCopies) descriptor takes its destination from the mount site and
/// has nothing to check here.
///
/// # Panics
///
/// Panics with a descriptive message if the reported address does not reach the subscription.
pub async fn redelivery_address<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: RedeliveryAddressed<Connected<B>> + Clone + Send + Sync,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.redelivery");

    let connected = make_broker()
        .connect()
        .await
        .expect("broker must connect after synchronous construction");

    let source = make_source(&subject);
    let address = source
        .redelivery_address(&connected)
        .await
        .expect("reporting a redelivery address must not fail against a live connection");
    let mut subscriber = source
        .subscribe(&connected)
        .await
        .expect("subscription source must open against the connected form");
    let publisher = make_publisher(&connected);

    publisher
        .publish(
            OutgoingMessage::new(address.as_str(), b"redelivered".as_slice()),
            None,
        )
        .await
        .expect("publish to the reported redelivery address failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "redelivery_address").await;
    assert_eq!(
        msg.payload(),
        b"redelivered",
        "a publish to the reported redelivery address must reach the subscription that reported \
         it",
    );
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    let _closed = connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// Fails when anything the broker contributes to the generated document carries `secret`.
///
/// A broker describes itself twice: once as a server coordinate
/// ([`DescribeServer`]) and once as a set of protocol bindings on its
/// subscription descriptor. Both are published, so a password that reaches either has left the
/// service. The mistake is easy to make from a configuration URL and has shipped in more than one
/// broker crate, which is why it is checked rather than only written down.
///
/// Configure `broker` and `source` the way a deployment would, with a password you pass as
/// `secret`, and this builds the document those two produce and scans it.
///
/// # Panics
///
/// Panics when `secret` appears in the server description or in any binding of the descriptor,
/// and when `secret` is empty (a scan for nothing passes for the wrong reason).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "conformance", feature = "asyncapi", feature = "memory"))]
/// # fn demo() {
/// use ruststream::conformance::harness;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// harness::describes_without_credentials(
///     &MemoryBroker::new(),
///     &MemorySource::new("orders"),
///     "hunter2",
/// );
/// # }
/// ```
#[cfg(feature = "asyncapi")]
pub fn describes_without_credentials<B, Src>(broker: &B, source: &Src, secret: &str)
where
    B: DescribeServer,
    Src: SubscriptionSource<Connected<B>>,
{
    assert!(
        !secret.is_empty(),
        "pass the password the broker was configured with; scanning for an empty string passes \
         whatever the broker does",
    );

    let app = RustStream::new(AppInfo::new("conformance", "0.0.0"))
        .server("broker", broker.describe_server());
    let document = build_spec(&app)
        .to_json()
        .expect("the generated document must serialize");
    let bindings = serde_json::to_string(&serde_json::json!({
        "channel": source.channel_bindings(),
        "operation": source.operation_bindings(),
        "message": source.message_bindings(),
    }))
    .expect("a binding body is serialized once at construction, so it serializes again here");

    for (what, text) in [
        ("the server description", &document),
        ("the subscription bindings", &bindings),
    ] {
        assert!(
            !text.contains(secret),
            "{what} carries the broker's password. A published document is shared: describe the \
             host with ServerSpec::from_url, which drops the userinfo, and keep credentials out \
             of every binding body. Got: {text}",
        );
    }
}

async fn ordering<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ordering")
        .await
        .expect("subscribe failed");

    for i in 0..10u32 {
        broker.inject(OutgoingMessage::new(
            "conformance.ordering",
            i.to_be_bytes().as_slice(),
        ));
    }

    let mut stream = std::pin::pin!(subscriber.stream());
    for expected in 0..10u32 {
        let msg = expect_next(&mut stream, "ordering").await;
        assert_eq!(
            msg.payload(),
            expected.to_be_bytes(),
            "messages must be delivered in publish order",
        );
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    broker.shutdown().await.expect("shutdown failed");
}

/// A message published before the subscription opened reaches it, or does not, exactly as the
/// broker declares through [`TestableBroker::backlog`]; the one published after reaches it either
/// way.
async fn publish_before_subscribe<C: TestableBroker + Subscribe>(broker: C) {
    let backlog = broker.backlog();
    broker.inject(OutgoingMessage::new(
        "conformance.late",
        b"before-subscribe".as_slice(),
    ));

    let mut subscriber = Subscribe::subscribe(&broker, "conformance.late")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new(
        "conformance.late",
        b"after-subscribe".as_slice(),
    ));

    let mut stream = std::pin::pin!(subscriber.stream());
    if backlog == Backlog::Delivered {
        let early = expect_next(&mut stream, "publish_before_subscribe").await;
        assert_eq!(
            early.payload(),
            b"before-subscribe",
            "the broker declares `Backlog::Delivered`: a subscription must first receive what was \
             published to its name before it opened",
        );
        settle_ack(early).await;
    }
    let msg = expect_next(&mut stream, "publish_before_subscribe").await;
    assert_eq!(
        msg.payload(),
        b"after-subscribe",
        "the broker declares `Backlog::{backlog:?}`: {}",
        match backlog {
            Backlog::Missed =>
                "a subscription must receive only messages published after it opened",
            Backlog::Delivered => "the message published after the subscription opened comes next",
        },
    );
    settle_ack(msg).await;
    broker.shutdown().await.expect("shutdown failed");
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle_ack<M: IncomingMessage>(msg: M) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
}

async fn ack_consumes_delivery<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ack")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.ack", b"one".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "ack_consumes_delivery").await;
    // A transport with no acknowledgement consumes the delivery by delivering it, so the
    // assertion below - one publish, one delivery - is the same contract either way.
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    expect_no_more(&mut stream, "ack_consumes_delivery").await;
    broker.shutdown().await.expect("shutdown failed");
}

async fn nack_with_requeue_redelivers<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.requeue")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new(
        "conformance.requeue",
        b"retry-me".as_slice(),
    ));

    let mut stream = std::pin::pin!(subscriber.stream());
    let first = expect_next(&mut stream, "nack_with_requeue first").await;
    assert_eq!(first.payload(), b"retry-me");
    // The only scenario whose assertion IS the settlement: a transport that reports the requeue
    // unsupported has no redelivery to observe, so the scenario ends instead of accepting any
    // answer - reading the redelivery of a message the transport never took back would pass a
    // broker whose retries silently lose messages.
    let requeued = match first.nack(true).await {
        Ok(()) => true,
        Err(AckError::Unsupported) => false,
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    };

    if requeued {
        let second = expect_next(&mut stream, "nack_with_requeue second").await;
        assert_eq!(
            second.payload(),
            b"retry-me",
            "nack(requeue=true) must redeliver the same payload",
        );
        match second.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    broker.shutdown().await.expect("shutdown failed");
}

async fn nack_without_requeue_drops<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.drop")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.drop", b"gone".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "nack_without_requeue").await;
    // Dropping is what a transport with no settlement does with every delivery anyway, so the
    // assertion below holds for both answers and stays checked for both.
    match msg.nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    }

    expect_no_more(&mut stream, "nack_without_requeue").await;
    broker.shutdown().await.expect("shutdown failed");
}

async fn headers_propagate<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.headers")
        .await
        .expect("subscribe failed");

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Tenant", Bytes::from_static(b"acme"));

    broker.inject(
        OutgoingMessage::new("conformance.headers", b"{}".as_slice()).with_headers(headers),
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "headers_propagate").await;
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().get("x-tenant"), Some(b"acme".as_slice()));
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    broker.shutdown().await.expect("shutdown failed");
}

async fn published_log_observes_publishes<C: TestableBroker + Subscribe>(broker: C) {
    broker.inject(OutgoingMessage::new(
        "conformance.observe",
        b"first".as_slice(),
    ));
    broker.inject(OutgoingMessage::new(
        "conformance.observe",
        b"second".as_slice(),
    ));

    let observed = broker.published("conformance.observe");
    assert_eq!(
        observed.len(),
        2,
        "the publish log must observe every publish",
    );
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    broker.shutdown().await.expect("shutdown failed");
}

/// Runs `work` on a current-thread runtime of its own, on a thread of its own, and returns once
/// that runtime has stopped.
///
/// This is where a handler on a dedicated thread publishes and settles from, so a broker that
/// spawns an internal task onto the caller's runtime loses it here: by the time this returns, the
/// runtime such a task would have landed on is gone. The caller's runtime keeps running while the
/// work does, so a broker whose I/O is driven there is not starved by the wait.
///
/// # Panics
///
/// Panics when the runtime cannot be built or when `work` panics.
pub(crate) async fn on_foreign_runtime<Output>(
    work: impl AsyncFnOnce() -> Output + Send + 'static,
) -> Output
where
    Output: Send + 'static,
{
    let (done, finished) = oneshot::channel();
    thread::Builder::new()
        .name("conformance-foreign-runtime".to_owned())
        .spawn(move || {
            let runtime = runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime for the foreign caller must build");
            let output = runtime.block_on(work());
            // Stopped before the caller resumes, so whatever the broker left on this runtime is
            // cancelled by the time the suite checks the work completed.
            drop(runtime);
            let _ = done.send(output);
        })
        .expect("the foreign caller's thread must start");
    finished
        .await
        .expect("the work on the foreign runtime panicked; its message is above")
}

pub(crate) async fn expect_next<S, M, E>(stream: &mut S, label: &str) -> M
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let item = timeout(DEFAULT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: stream timed out"));
    let item = item.unwrap_or_else(|| panic!("{label}: stream ended unexpectedly"));
    item.unwrap_or_else(|err| panic!("{label}: stream yielded error: {err:?}"))
}

pub(crate) async fn expect_no_more<S, M, E>(stream: &mut S, label: &str)
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let result = timeout(NEGATIVE_WAIT, stream.next()).await;
    assert!(
        result.is_err(),
        "{label}: expected no further deliveries within {NEGATIVE_WAIT:?}",
    );
}
