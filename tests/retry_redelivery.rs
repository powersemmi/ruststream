//! Where the deferred `retry_after` fallback publishes its copy.
//!
//! The subscription in these suites is shaped like a Google Pub/Sub one: it has a name of its
//! own, its deliveries flow from a separate topic, and only a publish to that topic reaches it
//! again. No broker in this crate is built that way, so the shape is assembled here over the
//! in-memory broker: the topic is the memory subject, the subscription name is what the
//! registration is known by, and the delivery reports no native delayed redelivery, which is what
//! puts the runtime's fallback on the retry path.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::{Future, ready};
use std::time::Duration;

use common::{Order, Receipt};
use futures::{Stream, StreamExt};
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForSlot, HandlerOutcome, Out, Outgoing, PublishTransform, RETRY_COUNT_HEADER, Reads,
    Router, RustStream, SlotContext,
};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{
    AckError, ConnectedBroker, HeaderMap, IncomingMessage, OutSlot, OutgoingMessage, PairError,
    PublishPolicy, Publisher, RedeliveryAddress, Subscribe, Subscriber, SubscriptionSource,
    subscriber,
};

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// A subscription whose name is not a publish destination: deliveries come from `topic`, and a
/// publish reaches it only there.
#[derive(Debug, Clone)]
struct BoundSubscription {
    subscription: &'static str,
    topic: &'static str,
}

impl BoundSubscription {
    const fn new(subscription: &'static str, topic: &'static str) -> Self {
        Self {
            subscription,
            topic,
        }
    }
}

impl<C: Subscribe> SubscriptionSource<C> for BoundSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;

    fn name(&self) -> &str {
        self.subscription
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe(self.topic).await?))
    }

    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        // The descriptor already holds the topic, so no lookup stands between it and the answer.
        ready(Ok(Some(RedeliveryAddress::new(self.topic))))
    }
}

/// The same subscription shape from a broker crate that has not adopted the contract: it reports
/// no redelivery address at all.
#[derive(Debug, Clone)]
struct SilentSubscription {
    topic: &'static str,
}

impl SilentSubscription {
    const fn new(topic: &'static str) -> Self {
        Self { topic }
    }
}

impl<C: Subscribe> SubscriptionSource<C> for SilentSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;

    fn name(&self) -> &str {
        self.topic
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe(self.topic).await?))
    }
}

/// The broker's subscriber with its native delayed redelivery taken away, so a `retry_after`
/// takes the runtime's deferred re-publish instead of the broker's own timer.
struct UnsettledSubscriber<S>(S);

impl<S: Subscriber> Subscriber for UnsettledSubscriber<S> {
    type Message = UnsettledMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(UnsettledMessage))
    }
}

/// A delivery that acknowledges like the broker's own but keeps the trait default for
/// [`IncomingMessage::supports_nack_after`], which is what nearly every real broker ships.
struct UnsettledMessage<M>(M);

impl<M: IncomingMessage> IncomingMessage for UnsettledMessage<M> {
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

/// Defers the first delivery of every message, acks the copy that comes back.
#[subscriber(BoundSubscription::new("orders-workers", "orders"))]
async fn reconcile(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        assert_eq!(order.id, 1);
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Defers the first delivery and answers the copy, so one registration carries both a reply and
/// a deferred retry.
#[subscriber(
    BoundSubscription::new("invoices-workers", "invoices"),
    publish("receipts")
)]
async fn settle(order: &Order, ctx: &mut Context) -> Result<Receipt, HandlerOutcome> {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        Err(HandlerOutcome::retry_after(RETRY_DELAY))
    } else {
        Ok(Receipt { id: order.id })
    }
}

/// The retry position sits beside the rest of the chain: the reply keeps its own policy and
/// codec whichever order the two are named in, and the deferred copy still comes back.
#[tokio::test(start_paused = true)]
async fn the_retry_position_composes_with_the_reply_wiring() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(settle)
                .out_retry(MemoryPublish)
                .out_reply(MemoryPublish)
                .codec(JsonCodec);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .to("invoices")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("invoices-workers")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("invoices-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and be answered",
    );
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
}

/// The deferred copy goes to the address the source reported, so the handler sees the message
/// again. Published under the subscription's own name it would reach nothing, and the delayed
/// message would be lost.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_reaches_the_handler_through_the_reported_address() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile).out_retry(MemoryPublish);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("orders-workers")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));
    let delivered = delivered_bytes(&tb);

    // The delay is real: nothing comes back before it elapses.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders-workers")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
    // A position that names no codec encodes nothing either: the copy is the delivery's bytes.
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .with_raw(&delivered);
}

/// The router surface binds the position the same way: a registration grouped in a `Router` names
/// its deferred-retry publisher where it is included, and the copy comes back as it does on a
/// scope.
#[tokio::test(start_paused = true)]
async fn a_router_registration_binds_the_retry_position() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(
                Router::<MemoryBroker>::new()
                    .include(reconcile)
                    .out_retry(MemoryPublish)
                    .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// Stamps every message leaving the slot it is mounted on with that slot's name: a transform that
/// reads the slot kind, which is the kind the retry position offers.
#[derive(Debug, Clone, Copy)]
struct StampSlot;

impl PublishTransform<ForSlot> for StampSlot {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
    }
}

/// A publish policy whose publisher contributes a constant to every message, the way a broker's
/// own publisher does. What the deferred copy carries from it is what any publish through the
/// same policy carries.
#[derive(Debug, Clone, Copy)]
struct StampedPublish;

impl<C: ConnectedBroker> PublishPolicy<C> for StampedPublish
where
    MemoryPublish: PublishPolicy<C>,
{
    type Live = Stamped<<MemoryPublish as PublishPolicy<C>>::Live>;

    async fn pair(self, connected: &C) -> Result<Self::Live, PairError> {
        let mut base = HeaderMap::new();
        base.insert("x-publisher", "deferred");
        Ok(Stamped {
            inner: MemoryPublish.pair(connected).await?,
            base,
        })
    }
}

/// The live form of [`StampedPublish`].
#[derive(Debug)]
struct Stamped<P> {
    inner: P,
    base: HeaderMap,
}

impl<P: Publisher> Publisher for Stamped<P> {
    type Error = P::Error;
    type Options = P::Options;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        self.inner.publish(msg, options).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

/// The bytes of the message the test publishes, as the handler's first delivery carries them.
fn delivered_bytes(tb: &TestApp<()>) -> Vec<u8> {
    tb.broker::<MemoryBroker>()
        .subscriber("orders-workers")
        .received_raw()
        .first()
        .expect("the subscription received the first delivery")
        .to_vec()
}

/// A transform on the retry slot runs on the deferred copy, and it reads what a slot transform
/// reads: the slot's own name. Nothing else on the chain sees the copy, so this is the only place
/// a service can stamp it.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_slot_stamps_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile)
                .out_retry(MemoryPublish)
                .transform(StampSlot);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .with_header("x-left-through", "Retry");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the stamped copy must still reach the handler",
    );
}

/// The deferred copy is the delivery's own bytes: a codec named on the retry slot resolves the
/// position and encodes nothing, so a copy stamped with the retry count still decodes as the
/// message the subscription decodes with its own codec.
#[cfg(feature = "msgpack")]
#[tokio::test(start_paused = true)]
async fn a_codec_named_on_the_retry_slot_does_not_re_encode_the_copy() {
    use ruststream::codec::MsgpackCodec;

    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile)
                .out_retry(MemoryPublish)
                .codec(MsgpackCodec);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    let delivered = delivered_bytes(&tb);
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .with_raw(&delivered);
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the copy must decode as the delivery it was made from",
    );
}

/// What the bound policy's publisher contributes to every message it sends reaches the deferred
/// copy too: the copy leaves through that publisher, not past it.
#[tokio::test(start_paused = true)]
async fn the_retry_publishers_base_headers_reach_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile).out_retry(StampedPublish);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .with_header("x-publisher", "deferred");
}

/// The router chain offers the same steps as the scope: a registration grouped in a `Router` names
/// the retry slot's codec and its transform where it is included.
#[tokio::test(start_paused = true)]
async fn a_router_registration_takes_the_retry_slots_steps() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(
                Router::<MemoryBroker>::new()
                    .include(reconcile)
                    .out_retry(MemoryPublish)
                    .codec(JsonCodec)
                    .transform(StampSlot)
                    .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    let delivered = delivered_bytes(&tb);
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .with_header("x-left-through", "Retry")
        .with_raw(&delivered);
}

/// The slot a registration publishes through itself, beside the retry slot.
#[derive(OutSlot)]
#[publishes(Receipt)]
struct Audit;

/// Defers the first delivery and audits the copy: one registration carrying a slot of its own and
/// the retry slot beside it, bound in either order.
#[subscriber(BoundSubscription::new("audits-workers", "audits"))]
async fn audit(
    order: &Order,
    ctx: &mut Context,
    Out(trail): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        return HandlerOutcome::retry_after(RETRY_DELAY);
    }
    if trail
        .message(&Receipt { id: order.id })
        .to("receipts")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A handler's own slots and the retry slot are positions of one chain: both bind through
/// `.out(..)`, and the registration commits with `.build()` as any slot-carrying one does.
#[tokio::test(start_paused = true)]
async fn the_retry_slot_binds_beside_a_handlers_own_slots() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(audit)
                .out_retry(MemoryPublish)
                .transform(StampSlot)
                .out(Audit, MemoryPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 3 })
        .to("audits")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("audits")
        .with_header("x-left-through", "Retry");
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 3 });
}

/// Defers every delivery. Mounted on a source that reports no redelivery address, so the app it
/// belongs to never starts.
#[subscriber(SilentSubscription::new("payments"))]
async fn settle_later(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A registration that binds the deferred-retry position over a subscription which cannot say
/// where a redelivery is published fails to start, naming that subscription, its source and the
/// fix. The registration beside it in the same scope is addressed and bound the same way, so the
/// refusal is the one registration's, not the scope's.
#[tokio::test]
async fn a_retry_position_over_an_unaddressed_source_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile).out_retry(MemoryPublish);
            b.include(settle_later).out_retry(MemoryPublish);
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a registration that cannot address its retries must not start");
    let message = failed.to_string();
    assert!(message.contains("payments"), "{message}");
    assert!(message.contains("SilentSubscription"), "{message}");
    assert!(message.contains("MemoryBroker"), "{message}");
    assert!(message.contains("out_retry"), "{message}");
    assert!(
        !message.contains("orders-workers"),
        "the addressed registration is untouched: {message}",
    );
}

/// Without the position the same subscription starts, beside one that binds it: `retry_after`
/// then degrades to an immediate requeue, which is the documented answer for a registration with
/// neither native delayed redelivery nor a publisher to defer through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unaddressed_source_starts_when_the_registration_defers_nothing() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile).out_retry(MemoryPublish);
            b.include(settle_later);
        },
    );

    let tb = TestApp::start(app).await.expect("startup failed");
    tb.shutdown().await.expect("graceful shutdown failed");
}
