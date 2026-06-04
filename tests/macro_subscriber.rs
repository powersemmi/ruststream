//! Integration test for the `#[subscriber]` attribute macro.
#![cfg(feature = "macros")]

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemorySubscriber};
use ruststream::runtime::{
    AppInfo, HandlerResult, Outgoing, PublishLayer, PublishMiddleware, PublishNext, RustStream,
    TypedPublisher,
};
use ruststream::{Message, OutgoingMessage, Publisher, SubscriptionSource, subscriber};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
    total: f64,
}

static HANDLED: AtomicU32 = AtomicU32::new(0);

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    HANDLED.fetch_add(order.id, Ordering::SeqCst);
    HandlerResult::Ack
}

/// A broker-specific subscription descriptor (stand-in for e.g. a Redis stream): mounted with
/// `include_on`, not by name. Proves a macro def works on an arbitrary `SubscriptionSource`.
struct StreamSource {
    name: String,
}

impl StreamSource {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

impl SubscriptionSource<MemoryBroker> for StreamSource {
    type Subscriber = MemorySubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, broker: &MemoryBroker) -> Result<MemorySubscriber, Infallible> {
        Ok(broker.subscribe(&self.name))
    }
}

static HANDLED_ON_STREAM: AtomicU32 = AtomicU32::new(0);

#[subscriber("ignored-on-the-include_on-path")]
async fn on_stream(order: &Order) -> HandlerResult {
    HANDLED_ON_STREAM.fetch_add(order.id, Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_def_mounts_on_arbitrary_source() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include_on(
            StreamSource {
                name: "events.stream".to_owned(),
            },
            on_stream,
            JsonCodec,
        );
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 4, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            // The source subscribed to "events.stream", not the macro's name.
            let _ = publisher
                .publish(OutgoingMessage::new("events.stream", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if HANDLED_ON_STREAM.load(Ordering::SeqCst) >= 4 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "include_on handler did not run");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static HANDLED_CTOR: AtomicU32 = AtomicU32::new(0);

// The descriptor lives in the decorator: the macro pulls the `StreamSource` type out of the
// constructor path and `include` mounts on `def.source()`, with the broker checked at compile time.
#[subscriber(StreamSource::new("ctor.stream"))]
async fn on_ctor(order: &Order) -> HandlerResult {
    HANDLED_CTOR.fetch_add(order.id, Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_descriptor_in_decorator() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // No source at the call site - it came from the macro argument.
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(on_ctor, JsonCodec));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 6, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("ctor.stream", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if HANDLED_CTOR.load(Ordering::SeqCst) >= 6 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "descriptor-in-decorator handler did not run"
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// An order placed by a customer.
#[derive(Message)]
#[allow(dead_code)]
struct DescribedOrder {
    id: u32,
}

#[test]
fn derive_message_metadata() {
    assert_eq!(DescribedOrder::NAME, "DescribedOrder");
    assert_eq!(
        DescribedOrder::DESCRIPTION,
        Some("An order placed by a customer."),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_subscriber_dispatches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(handle, JsonCodec));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 5, total: 1.0 }).unwrap();
    // include subscribes inside run() (after connect); retry until the subscription is live.
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("orders", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if HANDLED.load(Ordering::SeqCst) >= 5 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "macro handler did not run");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static HANDLED_DEFAULT: AtomicU32 = AtomicU32::new(0);

#[subscriber("orders-default")]
async fn handle_default(order: &Order) -> HandlerResult {
    HANDLED_DEFAULT.fetch_add(order.id, Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_default_codec_drops_per_call_codec() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // with_broker_codec sets the scope default, so include takes no codec argument.
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0"))
            .with_broker_codec(broker, JsonCodec, |b| b.include(handle_default));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 9, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("orders-default", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if HANDLED_DEFAULT.load(Ordering::SeqCst) >= 9 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "scope-default-codec handler did not run");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// A static (zero-cost) publish transform baked onto the `TypedPublisher`.
struct StaticEnvelope;

impl PublishLayer for StaticEnvelope {
    fn apply(&self, out: &mut Outgoing) {
        out.headers_mut().insert("x-static", b"1".to_vec());
    }
}

#[derive(Serialize, Deserialize)]
struct Ping {
    n: u32,
}

static STATIC_SEEN: AtomicU32 = AtomicU32::new(0);

#[subscriber("ping-in", publish("ping-out"))]
async fn relay(p: &Ping) -> Ping {
    Ping { n: p.n }
}

#[subscriber("ping-out")]
async fn check(p: &Ping, ctx: &mut Context) -> HandlerResult {
    if ctx.headers().get("x-static").is_some() {
        STATIC_SEEN.store(p.n, Ordering::SeqCst);
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_publish_layer_transforms_reply() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    // The static layer is composed onto the publisher at compile time - no dyn dispatch.
    let egress_pub = TypedPublisher::new(egress.publisher(), JsonCodec).layer(StaticEnvelope);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(ingress, |b| {
            b.include_publishing(relay, JsonCodec, egress_pub);
        })
        .with_broker(egress, |b| b.include(check, JsonCodec));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Ping { n: 7 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("ping-in", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if STATIC_SEEN.load(Ordering::SeqCst) == 7 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "static publish layer header did not reach the consumer",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[derive(Serialize, Deserialize)]
struct Request {
    n: u32,
}

#[derive(Serialize, Deserialize)]
struct Response {
    doubled: u32,
}

static REPLY_DOUBLED: AtomicU32 = AtomicU32::new(0);
static REPLY_TAGGED: AtomicU32 = AtomicU32::new(0);

/// A publish middleware that tags every outgoing reply with a header (envelope-style).
struct Tagger;

impl PublishMiddleware for Tagger {
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing,
        next: PublishNext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
    > {
        Box::pin(async move {
            out.headers_mut().insert("x-envelope", b"1".to_vec());
            next.run(out).await
        })
    }
}

#[subscriber("requests", publish("responses"))]
async fn reply(req: &Request) -> Response {
    Response { doubled: req.n * 2 }
}

#[subscriber("responses")]
async fn capture(resp: &Response, ctx: &mut Context) -> HandlerResult {
    if ctx.headers().get("x-envelope").is_some() {
        REPLY_TAGGED.store(1, Ordering::SeqCst);
    }
    REPLY_DOUBLED.store(resp.doubled, Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_publisher_replies_cross_broker() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    // The reply is published cross-broker: egress's publisher + reply codec; name from the macro.
    let egress_pub = TypedPublisher::new(egress.publisher(), JsonCodec);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(Tagger)
        .with_broker(ingress, |b| {
            b.include_publishing(reply, JsonCodec, egress_pub);
        })
        .with_broker(egress, |b| b.include(capture, JsonCodec));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Request { n: 21 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("requests", &payload))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if REPLY_DOUBLED.load(Ordering::SeqCst) == 42 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "reply was not published to egress");
    assert_eq!(
        REPLY_TAGGED.load(Ordering::SeqCst),
        1,
        "publish middleware header did not reach the consumer",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
