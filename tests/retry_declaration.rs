//! The retry declaration at the mount site: `max_attempts(n)` and `dead_letter(name)`.
//!
//! The subscription in these suites is shaped like a broker without delayed redelivery of its
//! own, which is what puts the runtime on the retry path: the deliveries are the in-memory
//! broker's with the native `nack_after` taken away. One shape reports no delivery count, as most
//! transports do, and the other reports the broker's own, as `JetStream`, SQS and Pub/Sub do.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::{Future, ready};
use std::marker::PhantomData;
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForSlot, HandlerOutcome, Outgoing, PublishTransform, RETRY_COUNT_HEADER, Reads,
    RustStream, SlotContext,
};
use ruststream::testing::TestApp;
use ruststream::{
    AckError, BrokerMoves, HeaderMap, IncomingMessage, RedeliveryAddress, RetryDeclaration,
    RuntimeCopies, Subscribe, Subscriber, SubscriptionSource, nonzero, subscriber,
};
use serde::Serialize;

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The header a [`Counted`] delivery reports its broker-side delivery count from, so a test can
/// hand a delivery the count a real transport would have given it.
const DELIVERY_COUNT: &str = "x-delivery-count";

/// The same count as a header contract, for the harness publish that plants it.
#[derive(Debug, Serialize)]
struct Delivered {
    #[serde(rename = "x-delivery-count")]
    count: u64,
}

/// What a delivery of one subscription reports about its own redeliveries: whether the transport
/// can hold it back for a delay, and how many times the broker has already delivered it.
trait DeliveryShape: Send + Sync + 'static {
    /// Whether the transport honours a delay of its own
    /// ([`IncomingMessage::supports_nack_after`](ruststream::IncomingMessage::supports_nack_after)).
    const HOLDS_BACK: bool;

    /// The broker's own delivery count, read from the header a test plants.
    fn count(headers: &HeaderMap) -> Option<u64>;
}

/// The transport counts nothing and holds nothing back, so the framework's own header carries the
/// count and the runtime publishes the copies. Most brokers.
#[derive(Debug, Clone, Copy)]
struct Uncounted;

impl DeliveryShape for Uncounted {
    const HOLDS_BACK: bool = false;

    fn count(_headers: &HeaderMap) -> Option<u64> {
        None
    }
}

/// The transport counts its own deliveries and the runtime reads that count instead.
#[derive(Debug, Clone, Copy)]
struct Counted;

impl DeliveryShape for Counted {
    const HOLDS_BACK: bool = false;

    fn count(headers: &HeaderMap) -> Option<u64> {
        headers
            .get_str(DELIVERY_COUNT)
            .and_then(|value| value.parse().ok())
    }
}

/// The transport both holds a delivery back for the delay and counts its own deliveries, the way
/// `JetStream`, SQS and Pub/Sub do.
#[derive(Debug, Clone, Copy)]
struct NativeCounted;

impl DeliveryShape for NativeCounted {
    const HOLDS_BACK: bool = true;

    fn count(headers: &HeaderMap) -> Option<u64> {
        Counted::count(headers)
    }
}

/// A subscription of a broker whose deliveries this process publishes copies of: one name, and
/// the shape of a delivery says what the transport does for itself.
#[derive(Debug, Clone)]
struct Queue<Mode> {
    name: &'static str,
    _mode: PhantomData<fn() -> Mode>,
}

impl<Mode> Queue<Mode> {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            _mode: PhantomData,
        }
    }
}

impl<C: Subscribe, Mode: DeliveryShape> SubscriptionSource<C> for Queue<Mode> {
    type Subscriber = ShapedSubscriber<C::Subscriber, Mode>;
    type Copies = RuntimeCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber {
            inner: connected.subscribe(self.name).await?,
            _mode: PhantomData,
        })
    }

    // One subject is both ends of the bus, so no lookup stands between the descriptor and the
    // answer.
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        ready(Ok(Some(RedeliveryAddress::new(self.name))))
    }
}

/// The broker's subscriber, with its deliveries reshaped to the mode the descriptor names.
struct ShapedSubscriber<S, Mode> {
    inner: S,
    _mode: PhantomData<fn() -> Mode>,
}

impl<S: Subscriber, Mode: DeliveryShape> Subscriber for ShapedSubscriber<S, Mode> {
    type Message = ShapedMessage<S::Message, Mode>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream().map(|item| {
            item.map(|inner| ShapedMessage {
                inner,
                _mode: PhantomData,
            })
        })
    }
}

/// A delivery that settles like the broker's own and reports what its mode says: the trait default
/// for `supports_nack_after`, which is what nearly every real broker ships, or the broker's own
/// delayed redelivery where the mode keeps it.
struct ShapedMessage<M, Mode> {
    inner: M,
    _mode: PhantomData<fn() -> Mode>,
}

impl<M: IncomingMessage, Mode: DeliveryShape> IncomingMessage for ShapedMessage<M, Mode> {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        Mode::count(self.inner.headers())
    }

    fn supports_nack_after(&self) -> bool {
        Mode::HOLDS_BACK && self.inner.supports_nack_after()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        self.inner.nack_after(delay).await
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

/// Never settles successfully: every delivery asks to come back later, so the cap is what ends
/// the sequence.
#[subscriber(Queue::<Uncounted>::new("orders"))]
async fn never_ready(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same, without a delay: an immediate retry obeys the same cap.
#[subscriber(Queue::<Uncounted>::new("jobs"))]
async fn never_ready_now(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// Asks to come back once, then settles: what a bare `retry_after` does on a broker with no
/// delayed redelivery of its own.
#[subscriber(Queue::<Uncounted>::new("invoices"))]
async fn settle_on_the_copy(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A delivery whose count the transport itself reports.
#[subscriber(Queue::<Counted>::new("shipments"))]
async fn never_ready_counted(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Stamps every message leaving the slot it is mounted on with that slot's name.
#[derive(Debug, Clone, Copy)]
struct StampSlot;

impl<Options> PublishTransform<ForSlot, Options> for StampSlot {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
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

/// A registration that names no retry publisher still gets one: a `retry_after` on a broker
/// without delayed redelivery yields the deferred copy, not an immediate requeue.
#[tokio::test(start_paused = true)]
async fn a_bare_retry_after_yields_the_deferred_copy() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle_on_the_copy);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 5 })
        .to("invoices")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("invoices")
        .assert_called_once();

    // The delay is real: nothing comes back before it elapses.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("invoices")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("invoices")
        .assert_called(2);
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
                .transform(StampSlot);
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
        .with_header("x-left-through", "Retry");
}

/// Where the transport counts its own deliveries, that count is what the cap reads: a delivery
/// that arrives already at the cap is carried away on its first sighting, with the framework's
/// own header absent.
#[tokio::test(start_paused = true)]
async fn the_brokers_own_delivery_count_drives_the_cap() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_counted)
                .max_attempts(nonzero!(3u32))
                .dead_letter("shipments.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("shipments", &Order { id: 7 }, &Delivered { count: 3 })
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("shipments")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("shipments.dead")
        .assert_called_once()
        .with(&Order { id: 7 });
}

/// A delivery the transport counts as its first is below the same cap, so the copy goes back to
/// the subscription: the count is read, not assumed.
#[tokio::test(start_paused = true)]
async fn a_first_delivery_the_broker_counts_stays_below_the_cap() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_counted)
                .max_attempts(nonzero!(3u32))
                .dead_letter("shipments.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("shipments", &Order { id: 8 }, &Delivered { count: 1 })
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("shipments")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("shipments.dead")
        .assert_not_called();
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

/// Settles every delivery: what the handler does is beside the point here.
#[subscriber(ManagedQueue { name: "managed" })]
async fn managed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The declaration reaches the subscription descriptor when the source resolves, so a broker that
/// applies a delivery limit and a dead-letter destination itself opens the subscription it
/// declared - and the runtime publishes nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_reaches_the_descriptor_before_it_subscribes() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(managed)
                .max_attempts(nonzero!(4u32))
                .dead_letter("managed.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 9 })
        .to("managed.capped")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("managed")
        .assert_called_once();
}

/// A subscription of a broker that holds a delivery back itself and counts its own deliveries.
#[subscriber(Queue::<NativeCounted>::new("parcels"))]
async fn never_ready_natively(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Where the transport holds the delivery back itself, the cap is still read before the delay
/// reaches it: a delivery the broker has already delivered as many times as the registration
/// allows goes to the dead-letter destination instead of coming back.
#[tokio::test(start_paused = true)]
async fn the_cap_applies_before_a_native_delayed_redelivery() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_natively)
                .max_attempts(nonzero!(3u32))
                .dead_letter("parcels.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("parcels", &Order { id: 11 }, &Delivered { count: 3 })
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("parcels.dead")
        .assert_called_once()
        .with(&Order { id: 11 });

    // The broker's own timer never got the delivery, so nothing comes back.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("parcels")
        .assert_called_once();
}

/// The same path with no destination declared: the spent delivery is rejected, and the broker's
/// own dead-letter policy is what takes it from there.
#[tokio::test(start_paused = true)]
async fn a_cap_without_a_destination_rejects_on_the_native_path() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_natively).max_attempts(nonzero!(3u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("parcels", &Order { id: 12 }, &Delivered { count: 3 })
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("parcels")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("parcels.dead")
        .assert_not_called();
}

/// Below the cap the delay is the broker's again: it holds the delivery back and brings it round
/// itself, with no copy published.
#[tokio::test(start_paused = true)]
async fn a_native_delayed_redelivery_stands_below_the_cap() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(never_ready_natively)
                .max_attempts(nonzero!(3u32))
                .dead_letter("parcels.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .publish_with_headers("parcels", &Order { id: 13 }, &Delivered { count: 1 })
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("parcels")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("parcels.dead")
        .assert_not_called();
}
