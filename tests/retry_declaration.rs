//! The retry declaration at the mount site: `max_attempts(n)` and `dead_letter(name)`.
//!
//! The subscription in these suites is shaped like a broker without delayed redelivery of its
//! own, which is what puts the runtime on the retry path: the deliveries are the in-memory
//! broker's with the native `nack_after` and the delivery count taken away, as most transports
//! ship them.
//!
//! The last suites drop the shape and mount on the in-memory broker itself, which holds a
//! delivery back for the delay and counts what it has delivered, as `JetStream`, SQS and Pub/Sub
//! do.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublish,
    MemoryPublisher, MemorySubscriber,
};
use ruststream::runtime::{
    AppInfo, ForReply, HandlerOutcome, Names, Outgoing, PublishContext, PublishTransform,
    RETRY_COUNT_HEADER, Reads, Router, RustStream, State,
};
use ruststream::testing::TestApp;
use ruststream::{
    AckError, AddressedCopies, BrokerMoves, FromRef, HeaderMap, IncomingMessage, NamedCopies,
    OutgoingMessage, Publisher, RedeliveryAddress, RedeliveryAddressed, RetryDeclaration,
    Subscribe, Subscriber, SubscriptionSource, nonzero, subscriber,
};
use serde::Serialize;

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The concrete topic a delivery came in on, as a header contract, for a wildcard subscription's
/// naming transform to read back.
#[derive(Debug, Serialize)]
struct Routed {
    #[serde(rename = "x-topic")]
    topic: &'static str,
}

/// The framework's own count as a header contract, for the harness publish that plants it on a
/// delivery no copy of this process produced.
#[derive(Debug, Serialize)]
struct Retried {
    #[serde(rename = "x-ruststream-retry-count")]
    retries: u64,
}

/// A subscription of a broker whose deliveries this process publishes copies of: one name, and a
/// transport that neither counts its deliveries nor holds one back for a delay.
#[derive(Debug, Clone)]
struct Queue {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for Queue {
    type Subscriber = ShapedSubscriber<C::Subscriber>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber(connected.subscribe(self.name).await?))
    }
}

impl<C: Subscribe> RedeliveryAddressed<C> for Queue {
    // One subject is both ends of the bus, so no lookup stands between the descriptor and the
    // answer.
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

/// The broker's subscriber, with its deliveries reshaped to report what most transports report.
struct ShapedSubscriber<S>(S);

impl<S: Subscriber> Subscriber for ShapedSubscriber<S> {
    type Message = ShapedMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(ShapedMessage))
    }
}

/// A delivery that settles like the broker's own and keeps the trait defaults for its delivery
/// count and its delayed redelivery, which is what nearly every real broker ships.
struct ShapedMessage<M>(M);

impl<M: IncomingMessage> IncomingMessage for ShapedMessage<M> {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.0.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.0.nack(requeue).await
    }
}

/// Never settles successfully: every delivery asks to come back later, so the cap is what ends
/// the sequence.
#[subscriber(Queue { name: "orders" })]
async fn never_ready(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same, without a delay: an immediate retry obeys the same cap.
#[subscriber(Queue { name: "jobs" })]
async fn never_ready_now(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// Asks to come back once, then settles: what a bare `retry_after` does on a broker with no
/// delayed redelivery of its own.
#[subscriber(Queue { name: "invoices" })]
async fn settle_on_the_copy(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Stamps every copy with the subscription the delivery it copies came from.
#[derive(Debug, Clone, Copy)]
struct StampSource;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampSource {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

/// Below the cap a copy goes back to the subscription; at the cap it goes to the declared
/// destination instead, and the message stops coming back.
#[tokio::test(start_paused = true)]
async fn the_cap_sends_the_last_delivery_to_the_dead_letter_destination() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once();

    // Two copies come back, the third delivery is the last the cap allows.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(3);
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders.dead")
        .assert_called_once()
        .with(&Order { id: 1 });

    // Nothing is left in flight: the delivery at the cap was carried away, not deferred.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(3);
}

/// A cap with no destination beside it rejects the delivery at the cap and publishes nothing,
/// which leaves the broker's own dead-letter policy in play where one is configured.
#[tokio::test(start_paused = true)]
async fn a_cap_without_a_destination_rejects_the_last_delivery() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready).max_attempts(nonzero!(2u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 2 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(2);

    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders.dead")
        .assert_not_called();
}

/// A destination with no cap beside it takes over every copy: the first retry already carries the
/// delivery away instead of sending it back.
#[tokio::test(start_paused = true)]
async fn a_destination_without_a_cap_redirects_every_copy() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready).dead_letter("orders.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 3 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders.dead")
        .assert_called_once()
        .with(&Order { id: 3 });
}

/// An immediate retry obeys the same cap on a transport that counts nothing: the runtime
/// republishes the delivery at once, so the framework's header carries the count for `retry()`
/// as it does for `retry_after`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediate_retry_counts_through_the_header() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_now)
                .max_attempts(nonzero!(3u32))
                .dead_letter("jobs.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 4 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called(3);
    tb.broker::<MemoryBroker>()
        .published::<Order>("jobs.dead")
        .assert_called_once()
        .with(&Order { id: 4 });
}

/// An immediate retry at the cap with no destination beside it is rejected, as a delayed one is:
/// the runtime publishes no copy, so the message stops coming back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediate_retry_at_the_cap_without_a_destination_is_rejected() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_now).max_attempts(nonzero!(2u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 16 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called(2);
}

/// A destination with no cap takes an immediate retry away on its first sighting, as it takes a
/// delayed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_destination_without_a_cap_takes_an_immediate_retry_away() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_now).dead_letter("jobs.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 17 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("jobs.dead")
        .assert_called_once()
        .with(&Order { id: 17 });
}

/// The declaration and the publisher are two steps of one chain: the cap still applies, and the
/// transform still reaches every copy the runtime publishes.
#[tokio::test(start_paused = true)]
async fn the_declaration_composes_with_the_named_publisher() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(2u32))
                .dead_letter("orders.dead")
                .out_retry(MemoryPublish)
                .transform(StampSource);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 6 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders.dead")
        .assert_called_once()
        .with_header("x-retried-from", "orders");
}

/// A subscription of a broker that moves a spent delivery itself: the declaration reaches the
/// descriptor before it subscribes, and the descriptor is what turns it into topology. Here the
/// topology is the queue it opens, so the subject the handler actually reads says whether the
/// declaration arrived.
#[derive(Debug, Clone)]
struct ManagedQueue {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for ManagedQueue {
    type Subscriber = C::Subscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.name
    }

    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        // What a broker with a native dead-letter policy does here: apply the declaration to the
        // queue it is about to open, and only when the limit and the destination come together.
        if declaration.max_attempts().is_some() && declaration.dead_letter().is_some() {
            self.name = "managed.capped";
        }
        self
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(self.name).await
    }
}

/// What a handler of a broker-moved subscription counts its own deliveries with, so that the
/// second one settles and a delivery that comes back is visible without retrying forever.
#[derive(Clone, FromRef)]
struct Deliveries {
    seen: Arc<AtomicU32>,
}

/// Asks for an immediate retry on its first delivery and settles on the next one.
#[subscriber(ManagedQueue { name: "managed" })]
async fn moved_retry(order: &Order, State(seen): State<Arc<AtomicU32>>) -> HandlerOutcome {
    let _ = order.id;
    if seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// An immediate retry on a subscription the broker moves itself is the broker's own requeue, cap
/// or no cap: the declaration went to the descriptor at startup, and the runtime reading it again
/// here would drop a delivery the broker was about to move.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_moved_immediate_retry_is_never_capped() {
    let seen = Arc::new(AtomicU32::new(0));
    let deliveries = Deliveries {
        seen: Arc::clone(&seen),
    };
    let app = RustStream::new(AppInfo::new("declared", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(deliveries))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(moved_retry)
                .max_attempts(nonzero!(1u32))
                .dead_letter("managed.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // The first delivery is already at the declared cap, and it still comes back.
    tb.message(&Order { id: 14 })
        .to("managed.capped")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("managed")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("managed.dead")
        .assert_not_called();
}

/// The framework's own count on a delivery, read the way the runtime reads it.
fn retry_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// A subscription of a broker whose crate honours a delay itself, by sending a copy of the
/// delivery round a wait queue: `RabbitMQ`'s `.delay(..)` scheme, where the server counts no
/// delivery of its own and the crate is what increments the framework's header on the copy.
#[derive(Debug, Clone)]
struct WaitQueue {
    name: &'static str,
}

impl SubscriptionSource<ConnectedMemoryBroker> for WaitQueue {
    type Subscriber = WaitQueueSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Self::Subscriber, MemoryError> {
        Ok(WaitQueueSubscriber {
            inner: connected.subscribe(self.name).await?,
            publisher: connected.publisher(),
            name: self.name,
        })
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for WaitQueue {
    // One subject is both ends of the bus, so no lookup stands between the descriptor and the
    // answer.
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

/// The broker's subscriber, with the handle its deliveries republish their delayed copies
/// through.
struct WaitQueueSubscriber {
    inner: MemorySubscriber,
    publisher: MemoryPublisher,
    name: &'static str,
}

impl Subscriber for WaitQueueSubscriber {
    type Message = WaitQueueMessage;
    type Error = <MemorySubscriber as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let publisher = self.publisher.clone();
        let name = self.name;
        self.inner.stream().map(move |item| {
            item.map(|inner| WaitQueueMessage {
                inner,
                publisher: publisher.clone(),
                name,
            })
        })
    }
}

/// A delivery the transport holds back itself and counts nothing for: its delay is honoured by a
/// copy that carries the framework's count forward.
struct WaitQueueMessage {
    inner: MemoryMessage,
    publisher: MemoryPublisher,
    name: &'static str,
}

impl IncomingMessage for WaitQueueMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        None
    }

    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Sends the copy round the wait queue, with the framework's header incremented, and drops
    /// the delivery it copies. The copy comes back at once: this suite is about the cap, not
    /// about the timer, and the copy is published before the original is dropped so the harness
    /// never sees the reaction go quiet between the two.
    async fn nack_after(self, _delay: Duration) -> Result<(), AckError> {
        let mut headers = self.inner.headers().clone();
        headers.insert(RETRY_COUNT_HEADER, (retry_count(&headers) + 1).to_string());
        let copy = OutgoingMessage::new(self.name, self.inner.payload()).with_headers(headers);
        self.publisher
            .publish(copy, None)
            .await
            .expect("the wait queue lost the copy");
        self.inner.nack(false).await
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

/// Never ready while the wait queue keeps bringing the message round, with a window of its own:
/// past the sixth copy it settles, so a cap that failed to hold shows up as extra deliveries
/// rather than as an endless reaction.
#[subscriber(WaitQueue { name: "returns" })]
async fn never_ready_in_the_wait_queue(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    if retry_count(ctx.headers()) < 6 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A broker that honours the delay by publishing the copy itself counts nothing on the server,
/// and the cap still holds: the framework's header travels on the copy, and the third delivery is
/// the last one the cap allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cap_applies_where_a_native_delay_counts_through_the_header() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_in_the_wait_queue)
                .max_attempts(nonzero!(3u32))
                .dead_letter("returns.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 14 })
        .to("returns")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("returns")
        .assert_called(3);
    tb.broker::<MemoryBroker>()
        .published::<Order>("returns.dead")
        .assert_called_once()
        .with(&Order { id: 14 });
}

/// A subscription that reads many destinations and addresses none of them: a filter, a wildcard,
/// a pattern. The mount site is what names where a copy goes.
#[derive(Debug, Clone)]
struct Filter {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for Filter {
    type Subscriber = ShapedSubscriber<C::Subscriber>;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber(connected.subscribe(self.name).await?))
    }
}

/// Asks to come back once, then settles.
#[subscriber(Filter { name: "sensors" })]
async fn filtered(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A descriptor that addresses its own subscription answers for the copies, and `.to(name)`
/// overrides that answer.
#[tokio::test(start_paused = true)]
async fn a_named_destination_overrides_an_addressed_descriptor() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle_on_the_copy)
                .out_retry(MemoryPublish)
                .to("invoices.retry");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 22 })
        .to("invoices")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    // The copy went to the override, so the subscription never saw it again.
    tb.broker::<MemoryBroker>()
        .subscriber("invoices")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("invoices.retry")
        .assert_called_once()
        .with(&Order { id: 22 });
}

/// Names the destination per delivery, from a header the delivery carries: what a wildcard
/// subscription needs, and what only a transform reading the delivery can do.
#[derive(Debug, Clone, Copy)]
struct ToOriginalTopic;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToOriginalTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        let topic = cx
            .headers()
            .get_str("x-topic")
            .unwrap_or_else(|| cx.name())
            .to_owned();
        out.set_name(topic);
    }
}

/// A transform on the position names where each copy goes, reading the delivery it is a copy of.
/// A router chain carries it, because its terminal is `.build()`.
#[tokio::test(start_paused = true)]
async fn a_naming_transform_sends_each_copy_to_the_delivery_s_own_topic() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include_router(
                Router::<MemoryBroker>::new()
                    .include(filtered)
                    .out_retry(MemoryPublish)
                    .transform(ToOriginalTopic)
                    .build(),
            );
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers(
            "sensors",
            &Order { id: 23 },
            &Routed {
                topic: "sensors.north",
            },
        )
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    // The copy went to the topic the delivery named, not to the filter the subscription reads.
    tb.broker::<MemoryBroker>()
        .subscriber("sensors")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("sensors.north")
        .assert_called_once()
        .with(&Order { id: 23 });
}

/// A registration on a descriptor that addresses nothing, with no destination of its own, refuses
/// to start: its copies would go nowhere. The mount chain refuses the same mistake at compile
/// time wherever its terminal is a call; a scope's guard commits when the statement ends, so this
/// is where the refusal lands there.
#[tokio::test]
async fn an_unnamed_destination_on_an_unaddressed_descriptor_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(filtered);
        });

    let failed = TestApp::start(app)
        .await
        .expect_err("a registration whose copies go nowhere must not start");
    let message = failed.to_string();
    assert!(message.contains("sensors"), "{message}");
    assert!(message.contains("NamedCopies"), "{message}");
    assert!(message.contains(".to("), "{message}");
}

/// A subscription of the reference broker itself, with nothing reshaped: the in-memory broker
/// holds the delivery back for the delay and counts what it has delivered.
#[subscriber("crates")]
async fn never_ready_in_memory(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The reference broker counts its own deliveries, so a cap declared over its native delayed
/// redelivery is spent on it: the third delivery is the last, and it goes to the declared
/// destination.
#[tokio::test(start_paused = true)]
async fn the_cap_is_spent_on_the_reference_brokers_native_redelivery() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_in_memory)
                .max_attempts(nonzero!(3u32))
                .dead_letter("crates.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 31 })
        .to("crates")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("crates")
        .assert_called(3);
    tb.broker::<MemoryBroker>()
        .published::<Order>("crates.dead")
        .assert_called_once()
        .with(&Order { id: 31 });

    // The delivery at the cap was carried away, so the broker's timer holds nothing.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("crates")
        .assert_called(3);
}

/// The same path with no destination declared: the spent delivery is terminated where it is, and
/// the broker holds nothing back.
#[tokio::test(start_paused = true)]
async fn a_cap_without_a_destination_terminates_on_the_reference_broker() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_in_memory)
                .max_attempts(nonzero!(2u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 32 })
        .to("crates")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("crates")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("crates.dead")
        .assert_not_called();
}

/// Where the transport counts, that count is the whole answer and the framework's header is not
/// added to it. A first delivery that carries a header worth three copies is still one attempt
/// against a cap of three: the delay is the broker's, and nothing is published.
#[tokio::test(start_paused = true)]
async fn a_native_count_is_the_only_count() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_in_memory)
                .max_attempts(nonzero!(3u32))
                .dead_letter("crates.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("crates", &Order { id: 34 }, &Retried { retries: 3 })
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("crates")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("crates.dead")
        .assert_not_called();
}

/// The same subscription without a delay. The broker's own requeue is the immediate retry, and
/// its count carries the attempt forward, so no copy is published and no delivery carries the
/// framework's header.
#[subscriber("tarballs")]
async fn never_ready_now_in_memory(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    assert!(
        ctx.headers().get_str(RETRY_COUNT_HEADER).is_none(),
        "the broker counts its own redeliveries, so nothing republishes the delivery",
    );
    HandlerOutcome::retry()
}

/// An immediate retry on the reference broker is its own requeue, and the cap is spent on it:
/// the count rises across redeliveries that no copy of this process carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediate_retry_is_requeued_natively_and_still_capped() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_now_in_memory)
                .max_attempts(nonzero!(3u32))
                .dead_letter("tarballs.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 33 })
        .to("tarballs")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("tarballs")
        .assert_called(3);
    tb.broker::<MemoryBroker>()
        .published::<Order>("tarballs.dead")
        .assert_called_once()
        .with(&Order { id: 33 });
}
