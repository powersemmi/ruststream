//! Where the deferred `retry_after` fallback publishes its copy.
//!
//! The subscription in these suites is shaped like a Google Pub/Sub one: it has a name of its
//! own, its deliveries flow from a separate topic, and only a publish to that topic reaches it
//! again. No broker in this crate is built that way, so the shape is assembled here over the
//! in-memory broker: the topic is the memory subject, the subscription name is what the
//! registration is known by, and the delivery reports no native delayed redelivery, which is what
//! puts the runtime's fallback on the retry path.
//!
//! The later suites take the same subject through the chain surfaces a registration
//! reaches it from: the guard a handler with `Out` slots gets, a `Router` chain, and the
//! declaration steps ahead of the publisher.
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
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish, MemoryPublisher,
    MemorySubscriber,
};
use ruststream::runtime::{
    AppInfo, ForReply, HandlerOutcome, MapPublisher, Out, Outgoing, PublishContext,
    PublishTransform, RETRY_COUNT_HEADER, Reads, Router, RustStream,
};
use ruststream::testing::{Outcome, TestApp, TestError};
use ruststream::{
    AckError, AddressedCopies, ConnectedBroker, HeaderMap, IncomingMessage, NamedCopies, OutSlot,
    OutgoingFor, OutgoingMessage, PairError, PublishPolicy, Publisher, RedeliveryAddress,
    RedeliveryAddressed, Subscribe, Subscriber, SubscriptionSource, nonzero, subscriber,
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
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.subscription
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe(self.topic).await?))
    }
}

impl<C: Subscribe> RedeliveryAddressed<C> for BoundSubscription {
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        // The descriptor already holds the topic, so no lookup stands between it and the answer.
        ready(Ok(RedeliveryAddress::new(self.topic)))
    }
}

/// The same subscription shape over a filter: it reads many destinations, so it addresses none of
/// them and the mount site names where a copy goes.
#[derive(Debug, Clone)]
struct FilteredSubscription {
    topic: &'static str,
}

impl FilteredSubscription {
    const fn new(topic: &'static str) -> Self {
        Self { topic }
    }
}

impl<C: Subscribe> SubscriptionSource<C> for FilteredSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;
    type Copies = NamedCopies;

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

/// Asks for the first delivery of every message back after no delay at all, acks the copy.
#[subscriber(BoundSubscription::new("refunds-workers", "refunds"))]
async fn refund(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        assert_eq!(order.id, 1);
        HandlerOutcome::retry_after(Duration::ZERO)
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
/// message would be lost. The registration names no retry publisher, so the copy leaves through
/// the one the broker's default policy pairs.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_reaches_the_handler_through_the_reported_address() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile);
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

/// A zero delay is no delay: the runtime publishes its copy at once, so the copy arrives in the
/// reaction of the publish itself and no `advance` is needed. The runtime has worker threads, so a
/// copy left to a timer task would still be on its way when the publish returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_delay_copy_arrives_without_advancing_the_clock() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(refund).out_retry(MemoryPublish);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("refunds")
        .publish()
        .await
        .expect("publish");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("refunds-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the zero-delay copy must reach the handler within the publish",
    );
}

/// Stamps every copy with the subscription its delivery came from: a transform that reads the
/// delivery being retried, which is what this position hands one.
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
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
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

/// A broker's per-message settings, as small as a broker's can be, so the retry slot has
/// something to carry. The publisher resolves the call's value against the policy's and puts the
/// answer in a header the assertion reads back.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PriorityOptions {
    priority: Option<u8>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PriorityPublish {
    priority: u8,
}

/// The live form of [`PriorityPublish`].
#[derive(Debug)]
struct PriorityPublisher<P> {
    inner: P,
    default_priority: u8,
}

impl<C: ConnectedBroker> PublishPolicy<C> for PriorityPublish
where
    MemoryPublish: PublishPolicy<C>,
{
    type Live = PriorityPublisher<<MemoryPublish as PublishPolicy<C>>::Live>;

    async fn pair(self, connected: &C) -> Result<Self::Live, PairError> {
        Ok(PriorityPublisher {
            inner: MemoryPublish.pair(connected).await?,
            default_priority: self.priority,
        })
    }
}

impl<P: Publisher> Publisher for PriorityPublisher<P> {
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = PriorityOptions;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let priority = options
            .and_then(|options| options.priority)
            .unwrap_or(self.default_priority);
        let mut headers = msg.headers().clone();
        headers.insert("priority", priority.to_string());
        let stamped = OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers);
        self.inner.publish(stamped, None).await
    }
}

/// Sends the deferred copy at a priority of its own: the copy has no call site, so a transform on
/// the retry slot is where its per-message settings come from.
#[derive(Debug, Clone, Copy)]
struct Expedite;

impl<C> PublishTransform<ForReply<C>, PriorityOptions> for Expedite {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<PriorityOptions>,
        _cx: &PublishContext<'_, C>,
    ) {
        options
            .get_or_insert_with(PriorityOptions::default)
            .priority = Some(7);
    }
}

/// The runtime publishes the deferred copy, so nothing on the chain names its per-message
/// settings; a transform on the retry slot does, and the broker sees them.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_slot_sets_the_deferred_copy_options() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile)
                .out_retry(PriorityPublish::default())
                .transform(Expedite);
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
        .with_header("priority", "7");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the copy the transform settled must still reach the handler",
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
                    .transform(StampSource)
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
        .with_header("x-retried-from", "orders-workers")
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
                .transform(StampSource)
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
        .with_header("x-retried-from", "audits-workers");
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 3 });
}

/// Defers every delivery. Mounted on a descriptor that addresses nothing, so the mount site is
/// what names where a copy goes.
#[subscriber(FilteredSubscription::new("payments"))]
async fn settle_later(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Replies and audits: one registration carrying a reply, a slot of its own and the retry
/// declaration beside them.
#[subscriber(
    BoundSubscription::new("ledger-workers", "ledger"),
    publish("ledger.out")
)]
async fn ledger(
    order: &Order,
    Out(trail): Out<impl Publisher, Audit>,
) -> Result<Receipt, HandlerOutcome> {
    if trail
        .message(&Receipt { id: order.id })
        .to("ledger.audit")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(Receipt { id: order.id })
}

/// A registration that carries `Out` slots takes the declaration and the reply policy on the
/// guard its slots gave it: the chain ends in `.build()`, and every position it named is wired.
#[tokio::test(start_paused = true)]
async fn a_slot_carrying_registration_declares_and_replies_on_one_chain() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(ledger)
                .max_attempts(nonzero!(3u32))
                .dead_letter("ledger.dead")
                .out_reply(MemoryPublish)
                .out(Audit, MemoryPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 11 })
        .to("ledger")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("ledger.out")
        .assert_called_once()
        .with(&Receipt { id: 11 });
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("ledger.audit")
        .assert_called_once()
        .with(&Receipt { id: 11 });
}

/// Where the copies go is a step of the retry position, so a registration carrying slots of its
/// own names it on the same guard, right after the publisher it belongs to.
#[tokio::test(start_paused = true)]
async fn a_slot_carrying_registration_names_where_its_copies_go() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(audit)
                .out(Audit, MemoryPublish)
                .out_retry(MemoryPublish)
                .to("audits.retry")
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 12 })
        .to("audits")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    // The copy went where the mount site said, so the subscription never saw it again and the
    // handler's own slot published nothing.
    tb.broker::<MemoryBroker>()
        .subscriber("audits-workers")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("audits.retry")
        .assert_called_once()
        .with(&Order { id: 12 });
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_not_called();
}

/// The two declaration steps read in either order, and the publisher with its destination
/// follows them: a cap of one leaves the first delivery no copy to come back as.
#[tokio::test(start_paused = true)]
async fn the_declaration_reads_in_either_order_ahead_of_the_publisher() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(settle_later)
                .dead_letter("payments.dead")
                .max_attempts(nonzero!(1u32))
                .out_retry(MemoryPublish)
                .to("payments");
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 13 })
        .to("payments")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("payments")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("payments.dead")
        .assert_called_once()
        .with(&Order { id: 13 });
}

/// The `Router` surface takes the same chain in the same order, finished by `.build()`.
#[tokio::test(start_paused = true)]
async fn a_router_chain_declares_and_names_the_destination_the_same_way() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(
                Router::<MemoryBroker>::new()
                    .include(settle_later)
                    .dead_letter("payments.dead")
                    .max_attempts(nonzero!(1u32))
                    .out_retry(MemoryPublish)
                    .to("payments")
                    .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 14 })
        .to("payments")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("payments")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Order>("payments.dead")
        .assert_called_once()
        .with(&Order { id: 14 });
}

/// A declaration and a retry publisher do not hide the registration from the router's listing,
/// and the destination it dead-letters to is listed as a channel the service publishes to.
#[test]
fn a_router_lists_a_declared_registration_with_its_dead_letter_channel() {
    let router = Router::<MemoryBroker>::new()
        .include(settle_later)
        .max_attempts(nonzero!(2u32))
        .dead_letter("payments.dead")
        .out_retry(MemoryPublish)
        .to("payments")
        .build();

    let handlers = router.handlers();

    assert_eq!(handlers.len(), 1);
    assert_eq!(handlers[0].name, "payments");
    assert!(
        handlers[0]
            .outgoing
            .iter()
            .any(|message| message.channel == "payments.dead"),
        "the dead-letter destination must be listed: {:?}",
        handlers[0].outgoing,
    );
}

/// What a broker crate layers on the chain: its policy's own settings, bound to that policy so
/// the method does not exist on a chain that named another broker's.
trait PrioritySettings: Sized {
    fn priority(self, priority: u8) -> Self;
}

impl<T: MapPublisher<Policy = PriorityPublish>> PrioritySettings for T {
    fn priority(self, priority: u8) -> Self {
        self.map_publisher(|mut policy| {
            policy.priority = priority;
            policy
        })
    }
}

/// The publisher of the copies is a policy like any other, so a broker's settings trait reaches
/// it: the copies leave at the priority the mount site named, with no transform in the way.
#[tokio::test(start_paused = true)]
async fn a_brokers_settings_trait_reaches_the_retry_publisher() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile)
                .out_retry(PriorityPublish::default())
                .priority(4);
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
        .with_header("priority", "4");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the copy the settings trait configured must still reach the handler",
    );
}

/// A retry policy that cannot pair, the way a broker's policy fails on a missing credential.
#[derive(Debug, Clone, Copy)]
struct UnpairablePublish;

impl PublishPolicy<ConnectedMemoryBroker> for UnpairablePublish {
    type Live = MemoryPublisher;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        ready(Err(PairError::from_boxed(
            "the retry topic needs a credential".into(),
        )))
    }
}

/// A retry policy that fails to pair refuses the start before its subscription opens, naming the
/// subscription and the policy's own reason: a registration whose copies would have nothing to
/// leave through never takes a delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_policy_that_fails_to_pair_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(reconcile).out_retry(UnpairablePublish);
        },
    );

    let Err(TestError::Subscribe(err)) = TestApp::start(app).await else {
        panic!("a retry policy that cannot pair must refuse to start");
    };
    assert_eq!(
        err.to_string(),
        "subscription `orders-workers`: the deferred-retry policy bound with `out_retry` failed \
         to pair: pairing a publisher failed: the retry topic needs a credential",
    );
}

/// A subscription whose descriptor fails to look up the address its copies go to, the way a
/// Pub/Sub subscription the API cannot find does.
#[derive(Debug, Clone)]
struct UnresolvedSubscription {
    subscription: &'static str,
}

impl SubscriptionSource<ConnectedMemoryBroker> for UnresolvedSubscription {
    type Subscriber = UnsettledSubscriber<MemorySubscriber>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.subscription
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Self::Subscriber, MemoryError> {
        Ok(UnsettledSubscriber(connected.subscribe("ghosts").await?))
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for UnresolvedSubscription {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Err(MemoryError::ShutDown))
    }
}

#[subscriber(UnresolvedSubscription { subscription: "ghosts-workers" })]
async fn haunt(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The broker's answer to the address lookup is the start's answer: the subscription does not
/// open on a copy path that has nowhere to go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_address_lookup_refuses_to_start_with_the_brokers_error() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(haunt);
        },
    );

    let Err(TestError::Subscribe(err)) = TestApp::start(app).await else {
        panic!("a descriptor that cannot say where its copies go must refuse to start");
    };
    assert_eq!(
        err.downcast_ref::<MemoryError>(),
        Some(&MemoryError::ShutDown),
        "{err}",
    );
}
