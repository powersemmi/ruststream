//! The `Debug` forms of the public types, which logs and panic messages print: each names its type,
//! shows the fields that identify the value (a count, a name, a timeout) and elides what it holds
//! but must not print (a borrowed scope, a closure, a connection, a payload). One test per module.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

#[cfg(any(feature = "metrics", feature = "otel"))]
use std::future::ready;
use std::pin::pin;
use std::time::Duration;

use common::{Order, Receipt};
use futures::StreamExt;
#[cfg(feature = "otel")]
use opentelemetry_sdk::metrics::{Instrument, SdkMeterProvider};
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::SdkTracerProvider;
#[cfg(feature = "metrics")]
use prometheus::Registry;
use ruststream::memory::prelude::*;
#[cfg(feature = "metrics")]
use ruststream::metrics::Metrics;
#[cfg(feature = "otel")]
use ruststream::otel::Otel;
#[cfg(any(feature = "metrics", feature = "otel"))]
use ruststream::runtime::Layer;
#[cfg(feature = "testing")]
use ruststream::testing::TestApp;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};

#[subscriber("debug.reply", reply("debug.reply.out"))]
async fn answer(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber("debug.slot")]
async fn slot(_order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let _ = out;
    HandlerOutcome::ack()
}

/// The app module: a mount guard names the terminal it commits through and elides the scope it
/// borrows, and a running app shows its subscriber and broker counts and its shutdown timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_app_types_render_their_debug_forms() {
    let app = RustStream::new(AppInfo::new("debug", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(MemoryBroker::new(), |b| {
            // A reply-only registration is complete as it stands, so dropping the guard commits.
            let mounting = b.include(answer);
            assert_eq!(format!("{mounting:?}"), "Mounting { .. }");
            drop(mounting);

            let slots = b.include(slot);
            assert_eq!(format!("{slots:?}"), "MountingSlots { .. }");
            slots.out(DefaultSlot, Publish).build();
        });

    let running = app.start().await.expect("startup failed");
    assert_eq!(
        format!("{running:?}"),
        "RunningApp { subscribers: 2, brokers: 1, shutdown_timeout: Some(5s), .. }",
    );
    running.shutdown().await.expect("shutdown failed");
}

/// The router module: a router and a half-built registration chain name themselves and elide the
/// routes, codec and layers they carry.
#[test]
fn the_router_types_render_their_debug_forms() {
    let router = Router::<MemoryBroker>::new();
    assert_eq!(format!("{router:?}"), "Router { .. }");

    let chain = router.include(slot);
    assert_eq!(format!("{chain:?}"), "RouterWith { .. }");
    let _ = chain.out(DefaultSlot, Publish).build();
}

/// The memory module: the handles show the name they are bound to and elide the shared bus, and
/// a delivery shows the name it was published to and elides its payload.
#[tokio::test]
async fn the_memory_types_render_their_debug_forms() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    assert_eq!(format!("{connected:?}"), "ConnectedMemoryBroker { .. }");
    assert_eq!(
        format!("{:?}", broker.requester()),
        "MemoryRequester { .. }"
    );

    let mut subscriber = broker.subscribe("orders");
    assert_eq!(
        format!("{subscriber:?}"),
        "MemorySubscriber { name: \"orders\", .. }",
    );
    let publisher = broker.publisher();
    assert_eq!(format!("{publisher:?}"), "MemoryPublisher { .. }");

    publisher
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    let mut stream = pin!(subscriber.stream());
    let message = stream
        .next()
        .await
        .expect("a delivery")
        .expect("a memory subscriber never errors");
    assert_eq!(
        format!("{message:?}"),
        "MemoryMessage { name: Some(\"orders\"), .. }",
    );
    message.ack().await.expect("ack");
    connected.shutdown().await.expect("shutdown");
}

/// The testing module: the harness shows its mode and how many brokers and subscribers it
/// drives, the brokers handed to a mirror state show their count, and a broker handle shows the
/// label it addresses; none prints the transports behind them.
#[cfg(feature = "testing")]
#[tokio::test]
async fn the_testing_types_render_their_debug_forms() {
    let app = RustStream::new(AppInfo::new("debug", "0.1.0")).with_broker_labeled(
        "east",
        MemoryBroker::new(),
        |b| {
            b.include(slot).out(DefaultSlot, Publish).build();
        },
    );
    let tb = TestApp::with_state(app, |brokers| {
        assert_eq!(format!("{brokers:?}"), "TestBrokers { brokers: 1, .. }");
    })
    .await
    .expect("startup failed");
    assert_eq!(
        format!("{tb:?}"),
        "TestApp { mode: InProcess, brokers: 1, subscribers: 1, .. }",
    );
    assert_eq!(
        format!("{:?}", tb.broker_named("east")),
        "BrokerHandle { broker: \"east\", .. }",
    );
    tb.shutdown().await.expect("shutdown failed");
}

/// The metrics module: the collector, its layers and a wrapped handler name themselves and elide
/// the registry, the collectors and the inner handler.
#[cfg(feature = "metrics")]
#[test]
fn the_metrics_types_render_their_debug_forms() {
    let metrics = Metrics::with_registry(Registry::new()).expect("a fresh registry");
    assert_eq!(format!("{metrics:?}"), "Metrics { .. }");
    let consume = metrics.consume_layer();
    assert_eq!(format!("{consume:?}"), "MetricsLayer { .. }");
    assert_eq!(
        format!("{:?}", metrics.publish_layer()),
        "MetricsPublish { .. }"
    );
    let handler = consume.layer(|_: &Order, _: &mut Context<'_>| ready(HandlerOutcome::ack()));
    assert_eq!(format!("{handler:?}"), "MetricsHandler { .. }");
}

/// The otel module: the integration, its layers and a wrapped handler name themselves and elide
/// the providers, the instruments and the inner handler; the builder counts its views and elides
/// the view closures.
#[cfg(feature = "otel")]
#[test]
fn the_otel_types_render_their_debug_forms() {
    let builder = Otel::builder().view(|_: &Instrument| None);
    assert!(
        format!("{builder:?}").contains("views: Views { len: 1 }"),
        "{builder:?}"
    );
    let otel = builder.attach(
        SdkTracerProvider::builder().build(),
        SdkMeterProvider::builder().build(),
    );
    assert_eq!(format!("{otel:?}"), "Otel { .. }");
    let consume = otel.consume_layer();
    assert_eq!(format!("{consume:?}"), "OtelConsumeLayer { .. }");
    assert_eq!(
        format!("{:?}", otel.publish_layer()),
        "OtelPublishLayer { .. }"
    );
    let handler = consume.layer(|_: &Order, _: &mut Context<'_>| ready(HandlerOutcome::ack()));
    assert_eq!(format!("{handler:?}"), "OtelConsumeHandler { .. }");
}
