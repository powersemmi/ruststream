//! Integration tests for `RustStream` lifecycle and dispatch, using `MemoryBroker`.
//!
//! What a handler saw rides the harness; the suite whose subject IS the running app, the shutdown
//! drain, keeps `run_until` and says so.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::{
    convert::Infallible,
    future::{Future, ready},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::prelude::*;
use ruststream::runtime::{
    BlanketLayer, Handler, HandlerMetadata, Input, Layer, MessageWire, SerializedWire,
    SoloDeserialized,
};
use ruststream::testing::TestApp;
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
use tokio::sync::Notify;

/// The payload view the byte-level bodies below take. The bodies never look at the bytes - the
/// subject here is the wiring - but the view is what puts them on the codec-free lane.
// The field is what makes the type a payload view; no body in this file reads it.
#[allow(dead_code)]
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// The wire the suites inject their unstructured payloads through, with the impls
/// `#[derive(Serialized)]` and `#[derive(Outgoing)]` write: the byte-level bodies below never
/// look at the bytes, and the serialized wire is what keeps a codec off them.
struct Wire(&'static [u8]);

impl Serialized for Wire {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(self.0))
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

/// Acks whatever raw delivery reaches it; the subject of the suites below is the wiring that
/// gets a delivery here, not what the body does with it.
struct TakeFrames;

impl<'p> Handle<Frame<'p>> for TakeFrames {
    fn handle(
        &self,
        _frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

/// A test layer that counts every invocation, to prove the global stack wraps handlers.
#[derive(Clone)]
struct CountLayer(Arc<AtomicU32>);

struct CountHandler<H> {
    inner: H,
    count: Arc<AtomicU32>,
}

impl<H> Layer<H> for CountLayer {
    type Handler = CountHandler<H>;

    fn layer(&self, inner: H) -> CountHandler<H> {
        CountHandler {
            inner,
            count: Arc::clone(&self.0),
        }
    }
}

impl<M, C, S, H> Handler<M, C, S> for CountHandler<H>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(msg, ctx).await
    }
}

// Lets CountLayer be an app-global layer that reaches router handlers via include_router.
impl BlanketLayer for CountLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        CountHandler {
            inner: handler,
            count: Arc::clone(&self.0),
        }
    }
}

// The subject IS the running app's teardown: the drain has to hold `run()` open, which only the
// spawned form can show.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_post_settle_continuations() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // The continuation signals `parked` once it is running, then blocks on `release` until the
    // test lets it proceed, and finally marks `drained`. The drain on shutdown must wait for it.
    let parked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let drained = Arc::new(AtomicU32::new(0));

    let on_parked = Arc::clone(&parked);
    let gate = Arc::clone(&release);
    let flag = Arc::clone(&drained);
    let handler = move |_msg: &_, _ctx: &mut Context| {
        let on_parked = Arc::clone(&on_parked);
        let gate = Arc::clone(&gate);
        let flag = Arc::clone(&flag);
        async move {
            HandlerOutcome::ack().and_after(async move {
                // Signal once the continuation is in flight, then block: the drain must await it.
                on_parked.notify_one();
                gate.notified().await;
                flag.store(1, Ordering::SeqCst);
            })
        }
    };

    let app = RustStream::new(AppInfo::new("drain", "0.1.0")).with_broker(broker, |b| {
        let subscriber = b.broker().subscribe("work");
        b.handle(subscriber, handler, HandlerMetadata::raw("work"));
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .message(&Wire(b"go"))
        .to("work")
        .publish()
        .await
        .unwrap();

    // Wait until the continuation is spawned and blocked (the message is already acked).
    parked.notified().await;

    // Begin shutdown while the continuation is still in flight, then release it: the drain holds
    // run() open until the continuation completes.
    shutdown.notify_one();
    release.notify_one();
    run.await.unwrap().unwrap();

    // run() returned only after the in-flight continuation finished.
    assert_eq!(drained.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_layer_reaches_router_handlers() {
    let layer_hits = Arc::new(AtomicU32::new(0));

    // The app-global stack must reach handlers mounted through include_router.
    let router = Router::<MemoryBroker>::new().include(subscriber("events", TakeFrames).build());

    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .layer(CountLayer(Arc::clone(&layer_hits)))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    deliver_one(&tb).await;

    // A layer's own invocation count is not something the harness records, so the layer keeps
    // its counter; what the handler saw is read off the harness above.
    assert_eq!(
        layer_hits.load(Ordering::SeqCst),
        1,
        "global layer did not reach the router handler exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_layer_wraps_handlers() {
    let layer_hits = Arc::new(AtomicU32::new(0));

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(CountLayer(Arc::clone(&layer_hits)))
        .with_broker(MemoryBroker::new(), |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
                HandlerMetadata::raw("orders"),
            );
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Wire(b"x"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(layer_hits.load(Ordering::SeqCst), 1);
}

/// Forwards every delivery to the publisher it was built with: the captured publisher belongs to
/// another broker, which is what the suite below asserts on.
struct Bridge(MemoryPublisher);

impl<'p> Handle<Frame<'p>> for Bridge {
    async fn handle(
        &self,
        _frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = self
            .0
            .message(&Wire(b"reply"))
            .to("responses")
            .publish()
            .await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_broker_publish_via_captured_publisher() {
    let egress = MemoryBroker::new();
    // Capture the egress broker's own publisher into a handler on the ingress broker - typed, no
    // registry.
    let egress_pub = egress.publisher();

    let app = RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Bridge(egress_pub.clone())).build());
        })
        .with_broker_labeled("egress", egress, |b| {
            let subscriber = b.broker().subscribe("responses");
            b.handle(
                subscriber,
                move |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
                HandlerMetadata::raw("responses"),
            );
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker_named("ingress")
        .message(&Wire(b"x"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    // The whole cascade settles before the injection returns, egress included.
    tb.broker_named("egress")
        .subscriber("responses")
        .assert_called_once()
        .with_raw(b"reply")
        .settled(HandlerOutcome::ack());
}

/// Injects one frame on the `events` channel and asserts the mount under test received it.
async fn deliver_one<S: Send + Sync + 'static>(tb: &TestApp<S>) {
    tb.message(&Wire(b"ping"))
        .to("events")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("events")
        .assert_called_once()
        .with_raw(b"ping")
        .settled(HandlerOutcome::ack());
}
