//! Integration tests for the batch subscriber pipeline: the form the macro reads off a `&[T]`
//! payload parameter, per-element outcomes, the metadata a batch registration records, and the
//! client-side buffer a broker crate gives a transport with no batches of its own.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::Order;
use futures::Stream;
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemorySubscriber};
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, BatchSubscriber, BufferedSubscriber, RedeliveryAddress, RedeliveryAddressed,
    Seekable, Subscribe, Subscriber, SubscriptionSource,
};
use serde::{Deserialize, Serialize};

// --8<-- [start:buffered_capability]
/// What a broker crate writes when its transport has no batches of its own: the subscriber it
/// already has, wrapped in the core's client-side buffer, and `BatchSubscriber` delegated to it.
/// The deadline that closes a partial batch is the broker's own choice; the batch size is not -
/// it arrives per subscription, as the argument of `batches`.
struct TrickleSubscriber(BufferedSubscriber<MemorySubscriber<Retaining>>);

impl TrickleSubscriber {
    fn new(inner: MemorySubscriber<Retaining>) -> Self {
        Self(BufferedSubscriber::new(inner).max_wait(Duration::from_millis(5)))
    }
}

impl Subscriber for TrickleSubscriber {
    type Message = <MemorySubscriber<Retaining> as Subscriber>::Message;
    type Error = <MemorySubscriber<Retaining> as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl BatchSubscriber for TrickleSubscriber {
    type Batch = Vec<<MemorySubscriber<Retaining> as Subscriber>::Message>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.0.batches(size)
    }
}

/// Buffering does not move the subscription, so every other capability reaches through the
/// wrapper unchanged - here the seeker, which is what lets a batch subscription open at a
/// position even where the batches are assembled on the client.
impl Seekable for TrickleSubscriber {
    type Seeker = <MemorySubscriber<Retaining> as Seekable>::Seeker;

    fn seeker(&self) -> Self::Seeker {
        self.0.seeker()
    }
}

/// The broker's own subscription descriptor, opening the batching subscriber above.
#[derive(Clone)]
struct Trickle {
    name: &'static str,
}

impl SubscriptionSource<ConnectedMemoryBroker<Retaining>> for Trickle {
    type Subscriber = TrickleSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Retaining>,
    ) -> Result<TrickleSubscriber, MemoryError> {
        Ok(TrickleSubscriber::new(
            Subscribe::subscribe(connected, self.name).await?,
        ))
    }
}

// A descriptor that addresses its own copies says where they go, and one in-memory subject is
// both ends of the bus, so the name answers for itself with no lookup in between.
impl RedeliveryAddressed<ConnectedMemoryBroker<Retaining>> for Trickle {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Retaining>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}
// --8<-- [end:buffered_capability]

/// A batch handler on that broker: nothing in the mount says the batches are assembled on the
/// client, which is the point - the size is the one word the mount site has either way.
#[subscriber(Trickle { name: "trickle" })]
async fn sip(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

/// A transport with no native batches still honours the size the registration named, because the
/// adapter it delegates to is what applies it. The batches are replayed off a position the mount
/// names, so they reach the handler before the harness drives anything - which is what lets a
/// batch carry more than the one delivery an injection settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delegating_broker_honours_the_batch_size() {
    // The mount opens at a log position, so the broker keeps one.
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("trickle")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("trickle", "0.1.0")).with_broker(broker, |b| {
        b.include(sip.batch(nonzero!(2)).start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await.expect("harness start");
    tb.settle().await.expect("the replayed batches settle");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("trickle")
        .assert_batch_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown failed");
}

/// Whether order 11 has already been refused once. Held in application state, so the handler
/// reads it the way a service reads any dependency.
struct Attempts {
    retried_once: AtomicBool,
}

/// Retries order 11 on first sight; settles everything else, per element.
#[subscriber("batches")]
async fn reconcile(orders: &[Order], ctx: &mut Context<'_, (), Attempts>) -> Vec<HandlerOutcome> {
    let retried_once = &ctx.state().retried_once;
    orders
        .iter()
        .map(|o| {
            if o.id == 11 && !retried_once.swap(true, Ordering::SeqCst) {
                HandlerOutcome::retry()
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}

/// One outcome per element: only the refused element of a batch comes back, and the elements
/// around it stay settled. The run is in the log before the subscription opens, so the opening
/// replay hands the body all three at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_element_outcomes_retry_individually() {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for id in [10u32, 11, 12] {
        publisher
            .message(&Order { id })
            .to("batches")
            .publish()
            .await
            .expect("publish failed");
    }
    let app = RustStream::new(AppInfo::new("batches", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Attempts {
                retried_once: AtomicBool::new(false),
            })
        })
        .with_broker(broker, |b| {
            b.include(
                reconcile
                    .batch(nonzero!(8))
                    .start_at(MemoryPosition::start()),
            );
        });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle()
        .await
        .expect("the replayed batch and its redelivery settle");

    // Only the refused element came back, so 10 and 12 were settled on the first run; the
    // redelivered 11 is acked.
    let subscriber = tb.broker::<MemoryBroker<Retaining>>();
    let subscriber = subscriber.subscriber("batches");
    let seen: Vec<Vec<u32>> = subscriber
        .batches::<Order>()
        .iter()
        .map(|batch| batch.iter().map(|o| o.id).collect())
        .collect();
    assert_eq!(seen, vec![vec![10, 11, 12], vec![11]]);
    subscriber.settled(HandlerOutcome::ack());
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

/// The plain reply form: every batch is confirmed.
#[subscriber("requests", publish("audit"))]
async fn audit(orders: &[Order]) -> Vec<Confirmation> {
    orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect()
}

#[test]
fn batch_publishing_def_records_metadata() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include(audit.batch(nonzero!(64))).out_reply(Publish);
    });

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "requests");
    assert!(
        app.handlers()[0]
            .output_type
            .is_some_and(|t| t.contains("Confirmation")),
    );
}

/// Settles a whole batch of orders at once.
#[subscriber("orders")]
async fn bill(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[test]
fn batch_def_records_metadata() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(bill.batch(nonzero!(64)));
    });

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "orders");
    assert_eq!(
        app.handlers()[0].description.as_deref(),
        Some("Settles a whole batch of orders at once."),
    );
}

/// Typed application state read from a batch handler: the multiplier is produced at startup and
/// reaches the whole-batch handler through `ctx.state()`, the same as a single-message handler.
#[derive(Clone, Copy)]
struct Tally {
    multiplier: u32,
}

/// Scales each order by the multiplier it read off application state and republishes it, so what
/// the state contributed is visible on the wire.
#[subscriber("scale", publish("scaled"))]
async fn scale(orders: &[Order], ctx: &mut Context<'_, (), Tally>) -> Vec<Order> {
    let multiplier = ctx.state().multiplier;
    orders
        .iter()
        .map(|o| Order {
            id: o.id * multiplier,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handler_reads_typed_state() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Tally { multiplier: 10 }))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(scale.batch(nonzero!(64))).out_reply(Publish);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in 1..4u32 {
        tb.message(&Order { id })
            .to("scale")
            .publish()
            .await
            .expect("publish failed");
    }

    // Each id was multiplied by the state's multiplier (10), proving the handler read typed state.
    let scaled: Vec<Order> = tb
        .broker::<MemoryBroker>()
        .published::<Order>("scaled")
        .decoded();
    assert_eq!(
        scaled.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![10, 20, 30],
    );
}
