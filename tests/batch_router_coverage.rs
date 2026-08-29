//! Router registration kinds that the other router suites do not reach: the `handle` form (a
//! subscriber created before connect), metadata collection over every route kind, and the startup
//! failures of the two deferred publishing routes (a reply publisher that refuses to pair, a
//! source that refuses to open).
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, Receipt, wait_for};
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublish,
    MemoryPublisher, MemorySubscriber,
};
use ruststream::runtime::{
    AppInfo, Context, HandlerMetadata, HandlerResult, PublishExt, Router, RouterHandlers,
    RustStream, RustStreamError, TypedPublisher, batch,
};
use ruststream::{IncomingMessage, PairError, PublishPolicy, SubscriptionSource, subscriber};

#[subscriber("brc-in", publish("brc-out"))]
async fn brc_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber(batch("brc-batch-in"), publish("brc-out"))]
async fn brc_batch_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

static BRC_HANDLED: AtomicUsize = AtomicUsize::new(0);

/// The `handle` form attaches a handler to a subscriber created before the broker connects; the
/// registration still mounts and dispatches once the app runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_route_dispatches_through_a_prebuilt_subscriber() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let subscriber = broker.subscribe("brc-handle");

    let router = Router::<MemoryBroker>::new().handle(
        subscriber,
        |msg: &MemoryMessage, _ctx: &mut Context| {
            let payload = msg.payload().to_vec();
            async move {
                if payload == b"ping" {
                    BRC_HANDLED.fetch_add(1, Ordering::SeqCst);
                }
                HandlerResult::Ack
            }
        },
        HandlerMetadata::raw("brc-handle"),
    );

    let app = RustStream::new(AppInfo::new("brc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
    });
    let running = app.start().await.expect("startup failed");

    publisher
        .raw(b"ping")
        .to("brc-handle")
        .publish()
        .await
        .expect("publish");
    wait_for(
        || BRC_HANDLED.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Every route kind contributes its metadata, in registration order, through both the inherent
/// `handlers()` and the [`RouterHandlers`] surface a nested router is collected through.
#[test]
fn every_route_kind_reports_its_metadata_in_registration_order() {
    let broker = MemoryBroker::new();

    let router = Router::<MemoryBroker>::new()
        .handle(
            broker.subscribe("brc-meta-handle"),
            |_msg: &MemoryMessage, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::raw("brc-meta-handle"),
        )
        .include(batch(
            "brc-meta-batch",
            |_batch: &[Order], _ctx: &mut Context| async { HandlerResult::Ack },
        ))
        .include(brc_relay)
        .publisher(TypedPublisher::new(MemoryPublish))
        .include(brc_batch_relay)
        .publisher(TypedPublisher::new(MemoryPublish));

    assert!(format!("{router:?}").contains("Router"));

    let names: Vec<_> = router.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(
        names,
        [
            "brc-meta-handle",
            "brc-meta-batch",
            "brc-in",
            "brc-batch-in"
        ]
    );

    let mut nested = Vec::new();
    RouterHandlers::collect_handlers(&router, &mut nested);
    let nested: Vec<_> = nested.into_iter().map(|m| m.name).collect();
    assert_eq!(nested, names);
}

/// A publish policy the broker refuses to pair. Router-mounted publishing routes pair their reply
/// publisher at startup, so this is what a broker rejecting a producer looks like to the runtime.
struct RefusedPublish;

impl PublishPolicy<ConnectedMemoryBroker> for RefusedPublish {
    type Live = MemoryPublisher;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<MemoryPublisher, PairError>> {
        ready(Err(PairError::from_boxed(Box::from(
            "the reply publisher was refused",
        ))))
    }
}

/// A source whose subscription never opens.
#[derive(Clone)]
struct ClosedSource;

impl SubscriptionSource<ConnectedMemoryBroker> for ClosedSource {
    type Subscriber = MemorySubscriber;

    fn name(&self) -> &'static str {
        "brc-closed"
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<MemorySubscriber, MemoryError>> {
        ready(Err(MemoryError::ShutDown))
    }
}

/// The definition carries the source that never opens, so the failure is the source's own.
#[subscriber(ClosedSource {}, publish("brc-out"))]
async fn brc_closed_relay(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn assert_subscribe_error(result: Result<impl Sized, RustStreamError>, expected: &str) {
    match result {
        Ok(_) => panic!("startup must fail"),
        Err(err) => {
            assert!(
                matches!(err, RustStreamError::Subscribe(_)),
                "expected a subscription failure, got {err:?}"
            );
            let rendered = format!("{err}");
            assert!(rendered.contains(expected), "{rendered}");
        }
    }
}

/// A reply publisher that cannot pair fails startup instead of dispatching without a publisher.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_route_reports_a_refused_reply_publisher() {
    let router = Router::<MemoryBroker>::new()
        .include(brc_relay)
        .publisher(TypedPublisher::new(RefusedPublish));

    let app =
        RustStream::new(AppInfo::new("brc-pair", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include_router(router);
        });

    assert_subscribe_error(app.start().await, "the reply publisher was refused");
}

/// The same on the batch publishing route: pairing runs before the first batch, so a refusal is a
/// startup failure, not a per-batch one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_publishing_route_reports_a_refused_reply_publisher() {
    let router = Router::<MemoryBroker>::new()
        .include(brc_batch_relay)
        .publisher(TypedPublisher::new(RefusedPublish));

    let app = RustStream::new(AppInfo::new("brc-batch-pair", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(router);
        },
    );

    assert_subscribe_error(app.start().await, "the reply publisher was refused");
}

/// A source that refuses to open fails startup with the broker's own error, after the reply
/// publisher has already paired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_route_reports_a_source_that_never_opens() {
    let router = Router::<MemoryBroker>::new()
        .include(brc_closed_relay)
        .publisher(TypedPublisher::new(MemoryPublish));

    let app = RustStream::new(AppInfo::new("brc-source", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(router);
        },
    );

    assert_subscribe_error(app.start().await, "the memory broker is shut down");
}
