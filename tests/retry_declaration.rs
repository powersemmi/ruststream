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

use std::convert::Infallible;
use std::future::{Future, ready};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForReply, HandlerOutcome, Names, Outgoing, PublishContext, PublishTransform,
    RETRY_COUNT_HEADER, Reads, Router, RustStream, State,
};
use ruststream::testing::TestApp;
use ruststream::{
    AckError, AddressedCopies, Broker, BrokerMoves, Connected, ConnectedBroker, DeclareRetryError,
    DefaultPublish, FromRef, HeaderMap, IncomingMessage, NamedCopies, PairError, PublishPolicy,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, Subscribe, Subscriber,
    SubscriptionSource, nonzero, subscriber,
};
use serde::Serialize;

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The header a [`Counted`] delivery reports its broker-side delivery count from, so a test can
/// hand a delivery the count a real transport would have given it.
const DELIVERY_COUNT: &str = "x-delivery-count";

/// The concrete topic a delivery came in on, as a header contract, for a wildcard subscription's
/// naming transform to read back.
#[derive(Debug, Serialize)]
struct Routed {
    #[serde(rename = "x-topic")]
    topic: &'static str,
}

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
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber {
            inner: connected.subscribe(self.name).await?,
            _mode: PhantomData,
        })
    }
}

impl<C: Subscribe, Mode: DeliveryShape> RedeliveryAddressed<C> for Queue<Mode> {
    // One subject is both ends of the bus, so no lookup stands between the descriptor and the
    // answer.
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name)))
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

/// The same without a destination declared: a cap alone does not turn an immediate retry into a
/// rejection, which on a queue that deletes a rejected delivery would lose the message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_moved_immediate_retry_is_not_rejected_at_the_cap() {
    let seen = Arc::new(AtomicU32::new(0));
    let deliveries = Deliveries {
        seen: Arc::clone(&seen),
    };
    let app = RustStream::new(AppInfo::new("declared", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(deliveries))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(moved_retry).max_attempts(nonzero!(1u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // A cap with no destination beside it leaves the descriptor's own name in place.
    tb.message(&Order { id: 15 })
        .to("managed")
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

/// A subscription that reads many destinations and addresses none of them: a filter, a wildcard,
/// a pattern. The mount site is what names where a copy goes.
#[derive(Debug, Clone)]
struct Filter {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for Filter {
    type Subscriber = ShapedSubscriber<C::Subscriber, Uncounted>;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber {
            inner: connected.subscribe(self.name).await?,
            _mode: PhantomData,
        })
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

/// The destination the mount site names is where the copies go, and the handler reads them back
/// from it.
#[tokio::test(start_paused = true)]
async fn a_named_destination_carries_the_copies_of_an_unaddressed_subscription() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(filtered).out_retry(MemoryPublish).to("sensors");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 20 })
        .to("sensors")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("sensors")
        .assert_called(2);
}

/// The destination names where the copies go, not where the subscription reads: a copy of a
/// delivery on the filter goes to the one subject the mount site named.
#[tokio::test(start_paused = true)]
async fn a_named_destination_is_not_the_subscriptions_own_name() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(filtered)
                .out_retry(MemoryPublish)
                .to("sensors.retry");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 21 })
        .to("sensors")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("sensors")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("sensors.retry")
        .assert_called_once()
        .with(&Order { id: 21 });
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

/// What a broker records when a registration mounted by a bare name declares its retries: the
/// subscription, and both halves of the declaration.
type Declared = Arc<Mutex<Vec<(String, RetryDeclaration)>>>;

/// A broker that moves a spent delivery itself and maps no declaration made over a bare
/// subscription name: its `Subscribe` keeps the default.
#[derive(Debug, Clone, Copy)]
struct Moved;

/// The same broker with a mechanism of its own, as Pub/Sub, SQS and Pulsar have one: it takes the
/// declaration for the subscription the name opens, and refuses half of one.
#[derive(Debug, Clone, Copy)]
struct Mapped;

/// A broker whose retry copies this process publishes but cannot address from a name: the mount
/// site says where they go, and the runtime applies the declaration.
#[derive(Debug, Clone, Copy)]
struct Unaddressed;

/// A broker over the in-memory bus whose `Subscribe` answers differently about retries: the
/// deliveries are the bus's, and `Answer` is the only difference between one of these and the
/// next.
struct Bus<Answer> {
    inner: MemoryBroker,
    declared: Declared,
    _answer: PhantomData<fn() -> Answer>,
}

impl<Answer> Bus<Answer> {
    fn new(declared: &Declared) -> Self {
        Self {
            inner: MemoryBroker::new(),
            declared: Arc::clone(declared),
            _answer: PhantomData,
        }
    }
}

/// The connected form of [`Bus`]: the live bus, and what the broker was told about retries.
struct ConnectedBus<Answer> {
    inner: Connected<MemoryBroker>,
    declared: Declared,
    _answer: PhantomData<fn() -> Answer>,
}

impl<Answer: Send + Sync + 'static> Broker for Bus<Answer> {
    type Error = <MemoryBroker as Broker>::Error;
    type Connected = ConnectedBus<Answer>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedBus {
            inner: self.inner.connect().await?,
            declared: self.declared,
            _answer: PhantomData,
        })
    }
}

impl<Answer: Send + Sync + 'static> ConnectedBroker for ConnectedBus<Answer> {
    type Error = <Connected<MemoryBroker> as ConnectedBroker>::Error;
    type Closed = ();

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        self.inner.shutdown().await?;
        Ok(())
    }
}

/// The bus's own publisher, paired through the broker wrapped around it.
#[derive(Debug, Default, Clone, Copy)]
struct BusPublish;

impl<Answer: Send + Sync + 'static> PublishPolicy<ConnectedBus<Answer>> for BusPublish {
    type Live = <MemoryPublish as PublishPolicy<Connected<MemoryBroker>>>::Live;

    fn pair(
        self,
        connected: &ConnectedBus<Answer>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        MemoryPublish.pair(&connected.inner)
    }
}

impl<Answer: Send + Sync + 'static> DefaultPublish for ConnectedBus<Answer> {
    type Policy = BusPublish;
}

impl Subscribe for ConnectedBus<Moved> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }
}

impl Subscribe for ConnectedBus<Unaddressed> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = NamedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }
}

impl Subscribe for ConnectedBus<Mapped> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }

    /// What a broker with a native dead-letter policy does with a bare name: take the cap and the
    /// destination for the subscription this name opens, and refuse half a declaration the way a
    /// descriptor of its own would.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        if declaration.max_attempts().is_none() || declaration.dead_letter().is_none() {
            return Err(DeclareRetryError::Broker(Box::new(io::Error::other(
                format!("subscription `{name}` needs the cap and the destination together"),
            ))));
        }
        self.declared
            .lock()
            .expect("declaration mutex poisoned")
            .push((name.to_owned(), declaration.clone()));
        Ok(())
    }
}

/// Settles every delivery: these suites assert on startup, not on what the handler does.
#[subscriber("orders-workers")]
async fn moved(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Never settles successfully, and asks to come back at once, so the cap is what ends the
/// sequence.
#[subscriber("returns")]
async fn returned(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// A cap and a destination declared over a bare name on a broker that moves a spent delivery
/// itself, and maps no declaration of its own, refuse to start: nothing here would apply them,
/// and the error says where they belong instead.
#[tokio::test]
async fn a_declaration_a_bare_name_carries_nowhere_refuses_to_start() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Moved>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a declaration the broker maps nowhere must not start");
    let message = failed.to_string();
    assert!(message.contains("orders-workers"), "{message}");
    assert!(message.contains("ConnectedBus"), "{message}");
    assert!(
        message.contains(
            "moves a spent delivery itself and maps no retry declaration made over a bare \
             subscription name"
        ),
        "{message}"
    );
    assert!(
        message.contains("declare the cap and the destination on this broker's own descriptor"),
        "{message}"
    );
    assert!(
        declared
            .lock()
            .expect("declaration mutex poisoned")
            .is_empty(),
        "the refusal comes from the default, which records nothing",
    );
}

/// The same broker with a mechanism of its own starts, and both halves of the declaration reach
/// it against the subscription the name opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_maps_a_bare_name_takes_both_halves() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Mapped>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead");
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");

    let recorded = declared.lock().expect("declaration mutex poisoned").clone();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].0, "orders-workers");
    assert_eq!(recorded[0].1.max_attempts().map(NonZeroU32::get), Some(5));
    assert_eq!(recorded[0].1.dead_letter(), Some("orders.dead"));
}

/// A broker that refuses what it cannot map refuses at startup, and its own reason reaches the
/// operator beside the subscription.
#[tokio::test]
async fn a_broker_that_refuses_half_a_declaration_refuses_to_start() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Mapped>::new(&declared),
        |b| {
            b.include(moved).max_attempts(nonzero!(5u32));
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a declaration the broker rejects must not start");
    let message = failed.to_string();
    assert!(message.contains("orders-workers"), "{message}");
    assert!(
        message.contains("needs the cap and the destination together"),
        "{message}"
    );
}

/// Where the copies are this process's to publish, a bare name takes the declaration as a
/// descriptor does: the broker is asked and accepts, and the runtime applies the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_on_an_addressed_broker_keeps_the_cap() {
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(returned)
                .max_attempts(nonzero!(2u32))
                .dead_letter("returns.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 21 })
        .to("returns")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("returns")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Order>("returns.dead")
        .assert_called_once()
        .with(&Order { id: 21 });
}

/// A broker whose copies are published from here but addressed by the mount site takes the
/// declaration the same way: the default accepts it, and nothing is asked of the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_on_an_unaddressed_broker_declares_with_a_named_destination() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Unaddressed>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead")
                .out_retry(BusPublish)
                .to("orders.retry");
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");

    assert!(
        declared
            .lock()
            .expect("declaration mutex poisoned")
            .is_empty(),
        "the runtime applies this one, so the broker is told nothing",
    );
}

/// A registration that declared nothing opens on the same broker: what the default refuses is a
/// declaration nobody would apply, not the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_that_declares_nothing_opens_where_the_broker_moves_deliveries() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Moved>::new(&declared),
        |b| {
            b.include(moved);
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");
}
