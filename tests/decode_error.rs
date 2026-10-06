//! A broker that reports a delivery does not decode: the runtime settles the delivery by the
//! subscription's decode policy before any handler sees it, on every input lane and for each
//! delivery of a batch.
//!
//! Every registration here stops the service on a decode failure, so the failure the harness
//! reads back names the error the broker reported.

#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod carrying;
mod common;

use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::time::Duration;

use carrying::{Row, Rows, UNREADABLE_REASON};
use common::{Order, Wire};
use futures::{Stream, StreamExt};
use ruststream::codec::CodecError;
use ruststream::memory::prelude::*;
use ruststream::memory::{
    ConnectedMemoryBroker, LogMode, MemoryMessage, MemorySeeker, MemorySubscriber,
};
use ruststream::runtime::BrokerScope;
use ruststream::testing::TestApp;
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, HeaderMap, IncomingMessage, RedeliveryAddress,
    RedeliveryAddressed, Seekable, SerializeHeadersError, Subscribe, Subscriber,
    SubscriptionSource,
};
use serde::{Deserialize, Serialize};

/// A subject of the in-memory bus read the way a database queue reads its rows: the client
/// reads each delivery itself, and one whose payload holds the word `unreadable` reports that it
/// does not decode.
#[derive(Debug, Clone)]
struct Claims {
    name: &'static str,
}

impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for Claims {
    type Subscriber = ClaimSubscriber<Log>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> Result<ClaimSubscriber<Log>, MemoryError> {
        Ok(ClaimSubscriber(
            Subscribe::subscribe(connected, self.name).await?,
        ))
    }
}

impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for Claims {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

/// The bus's own subscription, with each delivery read as a claim.
struct ClaimSubscriber<Log>(MemorySubscriber<Log>);

impl<Log: LogMode> Subscriber for ClaimSubscriber<Log> {
    type Message = Claim<Log>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Claim<Log>, Infallible>> + Send + '_ {
        self.0.stream().map(|delivery| delivery.map(Claim::read))
    }
}

impl<Log: LogMode> BatchSubscriber for ClaimSubscriber<Log> {
    type Batch = Vec<Claim<Log>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<Claim<Log>>, Infallible>> + Send + '_ {
        self.0
            .batches(size)
            .map(|page| page.map(|page| page.into_iter().map(Claim::read).collect()))
    }
}

// The bus replays its log on a seek, so a test can hand a subscription a whole page at once.
impl Seekable for ClaimSubscriber<Retaining> {
    type Seeker = MemorySeeker;

    fn seeker(&self) -> MemorySeeker {
        self.0.seeker()
    }
}

/// One delivery, and the error the client met reading it.
struct Claim<Log> {
    inner: MemoryMessage<Log>,
    error: Option<CodecError>,
}

impl<Log: LogMode> Claim<Log> {
    fn read(inner: MemoryMessage<Log>) -> Self {
        Self {
            error: carrying::reported(inner.payload()),
            inner,
        }
    }
}

impl<Log: LogMode> IncomingMessage for Claim<Log> {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn decode_error(&self) -> Option<&CodecError> {
        self.error.as_ref()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner.redelivery_count()
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack(requeue)
    }

    fn supports_nack_after(&self) -> bool {
        self.inner.supports_nack_after()
    }

    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack_after(delay)
    }
}

/// The header contract the pair input reads beside the order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Tenant {
    tenant: String,
}

/// The tenant contract as the map a publish of bytes carries: bytes declare no contract of their
/// own, so the contract travels as a map.
fn acme() -> Result<HeaderMap, SerializeHeadersError> {
    let mut headers = HeaderMap::new();
    headers.insert_typed(&Tenant {
        tenant: "acme".to_owned(),
    })?;
    Ok(headers)
}

/// An order whose payload decodes, but whose delivery the client could not read.
const UNREADABLE_ORDER: &str = r#"{"id":2,"note":"unreadable"}"#;

/// The bytes of a frame whose delivery the client could not read.
const UNREADABLE_FRAME: &str = "an unreadable frame";

/// A view over the payload that accepts any bytes, so only the broker's report can refuse one.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// What the frame handler answers with: the size it read.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
#[outgoing(name = "frames.sizes")]
struct Size(usize);

/// The event the pair handler publishes through its slot.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
#[outgoing(name = "orders.seen")]
struct Seen {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Seen)]
struct Audit;

/// Asserts the decode policy stopped the service for the error the broker reported.
fn stopped_for_the_reported_error(tb: &TestApp<()>) {
    let failure = tb
        .run_result()
        .expect_err("the decode policy stops the service");
    assert!(
        failure.to_string().contains(UNREADABLE_REASON),
        "the failure names the error the broker reported: {failure}",
    );
}

/// An app with one broker, the in-memory bus of every test that publishes as it goes.
fn app(mount: impl FnOnce(&mut BrokerScope<MemoryBroker>)) -> RustStream {
    RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(MemoryBroker::new(), mount)
}

/// A retaining bus that already holds `payloads`, each with the tenant contract: a subscription
/// opened at the start of its log replays them, so its first batch carries all of them.
async fn holding(name: &str, payloads: &[&str]) -> Result<MemoryBroker<Retaining>, Box<dyn Error>> {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for payload in payloads {
        publisher
            .message(&Wire::of(payload))
            .with_headers(acme()?)
            .to(name)
            .publish()
            .await?;
    }
    Ok(broker)
}

/// An app over a retaining bus, its batch subscription opened at the start of the log.
fn replaying(
    broker: MemoryBroker<Retaining>,
    mount: impl FnOnce(&mut BrokerScope<MemoryBroker<Retaining>>),
) -> RustStream {
    RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker, mount)
}

#[subscriber(Claims { name: "orders" }, on_failure(decode = fail_fast))]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decoded_input_is_refused_before_the_codec_runs() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app(|b| {
        b.include(accept);
    }))
    .await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(UNREADABLE_ORDER))
        .to("orders")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Claims { name: "frames" }, reply, on_failure(decode = fail_fast))]
async fn measure(frame: &Frame<'_>) -> Size {
    Size(frame.0.len())
}

/// The self-deserializing lane, mounted with a reply: the frame is never built, so nothing is
/// answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_deserializing_input_is_refused_before_it_is_built() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app(|b| {
        b.include(measure).out_reply(Publish);
    }))
    .await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(UNREADABLE_FRAME))
        .to("frames")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    tb.broker::<MemoryBroker>()
        .published::<Size>("frames.sizes")
        .assert_not_called();
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Claims { name: "orders" }, on_failure(decode = fail_fast))]
async fn audit(
    order: &Message<Tenant, Order>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    if out
        .message(&Seen { id: order.body.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The header and payload pair, mounted with an `Out` slot: both halves decode, and the broker's
/// report still keeps the body from running, so nothing leaves through the slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_input_is_refused_before_either_half_decodes() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app(|b| {
        b.include(audit).out(Audit, Publish).build();
    }))
    .await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(UNREADABLE_ORDER))
        .with_headers(acme()?)
        .to("orders")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    tb.out::<Audit>().assert_not_called();
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Rows::new("rows"), on_failure(decode = fail_fast))]
async fn greet(row: &Row) -> HandlerOutcome {
    tracing::info!(row.id, "greeted");
    HandlerOutcome::ack()
}

/// The carried lane: the delivery still lends the row it read, and the runtime never asks for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_input_is_refused_before_the_value_is_lent() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app(|b| {
        b.include(greet);
    }))
    .await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(2, "unreadable").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Claims { name: "orders" }, on_failure(decode = fail_fast))]
async fn tally(orders: &[Order]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "tallied");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decoded_batch_leaves_the_refused_delivery_out() -> Result<(), Box<dyn Error>> {
    let broker = holding("orders", &[r#"{"id":1}"#, UNREADABLE_ORDER, r#"{"id":3}"#]).await?;
    let tb = TestApp::start(replaying(broker, |b| {
        b.include(tally.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    }))
    .await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    let orders = broker.subscriber("orders");
    assert_eq!(
        orders.received::<Order>(),
        [Order { id: 1 }, Order { id: 3 }]
    );
    orders
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Claims { name: "frames" }, on_failure(decode = fail_fast))]
async fn weigh(frames: &[Frame<'_>]) -> HandlerOutcome {
    tracing::info!(count = frames.len(), "weighed");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_deserializing_batch_leaves_the_refused_delivery_out() -> Result<(), Box<dyn Error>>
{
    let broker = holding("frames", &["first", UNREADABLE_FRAME, "third"]).await?;
    let tb = TestApp::start(replaying(broker, |b| {
        b.include(weigh.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    }))
    .await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    let frames = broker.subscriber("frames");
    assert_eq!(frames.received_raw(), [&b"first"[..], &b"third"[..]]);
    frames
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Claims { name: "orders" }, on_failure(decode = fail_fast))]
async fn tally_tenants(orders: &[Message<Tenant, Order>]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "tallied per tenant");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_batch_leaves_the_refused_delivery_out() -> Result<(), Box<dyn Error>> {
    let broker = holding("orders", &[r#"{"id":1}"#, UNREADABLE_ORDER, r#"{"id":3}"#]).await?;
    let tb = TestApp::start(replaying(broker, |b| {
        b.include(
            tally_tenants
                .batch(nonzero!(8))
                .start_at(MemoryPosition::start()),
        );
    }))
    .await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    let orders = broker.subscriber("orders");
    assert_eq!(
        orders.received::<Order>(),
        [Order { id: 1 }, Order { id: 3 }]
    );
    orders
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    stopped_for_the_reported_error(&tb);
    Ok(())
}

#[subscriber(Rows::new("rows"), on_failure(decode = fail_fast))]
async fn tally_rows(rows: &[Row]) -> HandlerOutcome {
    tracing::info!(count = rows.len(), "tallied");
    HandlerOutcome::ack()
}

/// The carried batch: the page lends no row for the delivery it could not read and puts it past
/// the end of its slice, where the decode policy settles it for the error the broker reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_batch_settles_the_refused_delivery_for_its_reported_error()
-> Result<(), Box<dyn Error>> {
    let broker = holding("rows", &["1:ann", "2:unreadable", "3:cyd"]).await?;
    let tb = TestApp::start(replaying(broker, |b| {
        b.include(
            tally_rows
                .batch(nonzero!(8))
                .start_at(MemoryPosition::start()),
        );
    }))
    .await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    let rows = broker.subscriber("rows");
    assert_eq!(
        rows.received_values::<Row>(),
        [Row::new(1, "ann"), Row::new(3, "cyd")],
    );
    rows.assert_batch_sizes(&[2]).settled(HandlerOutcome::ack());
    stopped_for_the_reported_error(&tb);
    Ok(())
}
