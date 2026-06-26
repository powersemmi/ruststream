//! Integration test for the `#[subscriber]` attribute macro.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        LazyLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use common::handler_signal;
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
static HANDLED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    HANDLED.fetch_add(order.id, Ordering::SeqCst);
    HANDLED_NOTIFY.notify_one();
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

    /// A fluent option returning `Self`, so a `StreamSource::new(..).at(..)` chain can sit directly
    /// in the `#[subscriber(..)]` decorator.
    fn at(mut self, name: &str) -> Self {
        name.clone_into(&mut self.name);
        self
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
static HANDLED_ON_STREAM_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("ignored-on-the-include_on-path")]
async fn on_stream(order: &Order) -> HandlerResult {
    HANDLED_ON_STREAM.fetch_add(order.id, Ordering::SeqCst);
    HANDLED_ON_STREAM_NOTIFY.notify_one();
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
        );
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 4, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // The source subscribed to "events.stream", not the macro's name.
            let _ = publisher
                .publish(OutgoingMessage::new("events.stream", &payload))
                .await;
            handler_signal(&HANDLED_ON_STREAM_NOTIFY).await;
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
static HANDLED_CTOR_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

// The descriptor lives in the decorator: the macro pulls the `StreamSource` type out of the
// constructor path and `include` mounts on `def.source()`, with the broker checked at compile time.
#[subscriber(StreamSource::new("ctor.stream"))]
async fn on_ctor(order: &Order) -> HandlerResult {
    HANDLED_CTOR.fetch_add(order.id, Ordering::SeqCst);
    HANDLED_CTOR_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_descriptor_in_decorator() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // No source at the call site - it came from the macro argument.
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(on_ctor));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 6, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("ctor.stream", &payload))
                .await;
            handler_signal(&HANDLED_CTOR_NOTIFY).await;
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

static HANDLED_CHAIN: AtomicU32 = AtomicU32::new(0);
static HANDLED_CHAIN_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

// A builder chain in the decorator: the macro follows the receivers down to `StreamSource::new`
// for the type, and emits the whole chain as the source constructor.
#[subscriber(StreamSource::new("placeholder").at("chain.stream"))]
async fn on_chain(order: &Order) -> HandlerResult {
    HANDLED_CHAIN.fetch_add(order.id, Ordering::SeqCst);
    HANDLED_CHAIN_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_builder_chain_in_decorator() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(on_chain));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 7, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // The `at(..)` option won: the subscription binds to "chain.stream".
            let _ = publisher
                .publish(OutgoingMessage::new("chain.stream", &payload))
                .await;
            handler_signal(&HANDLED_CHAIN_NOTIFY).await;
            if HANDLED_CHAIN.load(Ordering::SeqCst) >= 7 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "builder-chain-in-decorator handler did not run"
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

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(handle));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Order { id: 5, total: 1.0 }).unwrap();
    // include subscribes inside run() (after connect); retry until the subscription is live.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("orders", &payload))
                .await;
            handler_signal(&HANDLED_NOTIFY).await;
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
static HANDLED_DEFAULT_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("orders-default")]
async fn handle_default(order: &Order) -> HandlerResult {
    HANDLED_DEFAULT.fetch_add(order.id, Ordering::SeqCst);
    HANDLED_DEFAULT_NOTIFY.notify_one();
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
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("orders-default", &payload))
                .await;
            handler_signal(&HANDLED_DEFAULT_NOTIFY).await;
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

impl<C> PublishLayer<C> for StaticEnvelope {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &ruststream::runtime::PublishContext<'_, C>) {
        out.headers_mut().insert("x-static", b"1".to_vec());
    }
}

#[derive(Serialize, Deserialize)]
struct Ping {
    n: u32,
}

static STATIC_SEEN: AtomicU32 = AtomicU32::new(0);
static STATIC_SEEN_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("ping-in", publish("ping-out"))]
async fn relay(p: &Ping) -> Ping {
    Ping { n: p.n }
}

#[subscriber("ping-out")]
async fn check(p: &Ping, ctx: &mut Context) -> HandlerResult {
    if ctx.headers().get("x-static").is_some() {
        STATIC_SEEN.store(p.n, Ordering::SeqCst);
    }
    STATIC_SEEN_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_publish_layer_transforms_reply() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    // The static layer is composed onto the publisher at compile time - no dyn dispatch.
    let egress_pub = TypedPublisher::new(egress.publisher()).layer(StaticEnvelope);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(ingress, |b| {
            b.include_publishing(relay, egress_pub);
        })
        .with_broker(egress, |b| b.include(check));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Ping { n: 7 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("ping-in", &payload))
                .await;
            handler_signal(&STATIC_SEEN_NOTIFY).await;
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
static REPLY_DOUBLED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static REPLY_TAGGED: AtomicU32 = AtomicU32::new(0);

/// A publish middleware that tags every outgoing reply with a header (envelope-style).
#[derive(Clone)]
struct Tagger;

impl PublishMiddleware for Tagger {
    fn on_publish<'a, N: ruststream::runtime::PublishPipeline>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N>,
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
    REPLY_DOUBLED_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_publisher_replies_cross_broker() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    // The reply is published cross-broker: egress's publisher + reply codec; name from the macro.
    let egress_pub = TypedPublisher::new(egress.publisher());

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(Tagger)
        .with_broker(ingress, |b| {
            b.include_publishing(reply, egress_pub);
        })
        .with_broker(egress, |b| b.include(capture));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Request { n: 21 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("requests", &payload))
                .await;
            handler_signal(&REPLY_DOUBLED_NOTIFY).await;
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

#[derive(Serialize, Deserialize)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

static CONFIRM_REJECTED: AtomicU32 = AtomicU32::new(0);
static CONFIRM_REJECTED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static CONFIRM_ACCEPTED: AtomicU32 = AtomicU32::new(0);
static CONFIRM_ACCEPTED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("confirm-in", publish("confirm-out"))]
async fn confirm(order: &Order) -> Result<Confirmation, HandlerResult> {
    if order.id == 0 {
        CONFIRM_REJECTED.fetch_add(1, Ordering::SeqCst);
        CONFIRM_REJECTED_NOTIFY.notify_one();
        return Err(HandlerResult::drop());
    }
    Ok(Confirmation {
        id: order.id,
        accepted: true,
    })
}

#[subscriber("confirm-out")]
async fn confirm_sink(c: &Confirmation) -> HandlerResult {
    if c.accepted {
        CONFIRM_ACCEPTED.store(c.id, Ordering::SeqCst);
        CONFIRM_ACCEPTED_NOTIFY.notify_one();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_result_form_controls_ack_and_publish() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let replies = TypedPublisher::new(broker.publisher());

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include_publishing(confirm, replies);
        b.include(confirm_sink);
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Err(HandlerResult) skips the publish entirely.
    let rejected = serde_json::to_vec(&Order { id: 0, total: 0.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress
                .publish(OutgoingMessage::new("confirm-in", &rejected))
                .await;
            handler_signal(&CONFIRM_REJECTED_NOTIFY).await;
            if CONFIRM_REJECTED.load(Ordering::SeqCst) >= 3 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "Err branch of the publishing handler did not run",
    );
    assert_eq!(
        CONFIRM_ACCEPTED.load(Ordering::SeqCst),
        0,
        "Err(..) must not publish a reply",
    );

    // Ok(reply) publishes and acks.
    let accepted = serde_json::to_vec(&Order { id: 6, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress
                .publish(OutgoingMessage::new("confirm-in", &accepted))
                .await;
            handler_signal(&CONFIRM_ACCEPTED_NOTIFY).await;
            if CONFIRM_ACCEPTED.load(Ordering::SeqCst) == 6 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Ok branch did not publish the reply");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// App state read from the publishing handler's optional `&mut Context` parameter.
#[derive(Clone, Copy)]
struct Bump(u32);

static CTX_REPLY: AtomicU32 = AtomicU32::new(0);
static CTX_REPLY_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("ctx-in", publish("ctx-out"))]
async fn ctx_reply(req: &Request, ctx: &mut Context<'_, (), Bump>) -> Response {
    let bump = ctx.state().0;
    Response {
        doubled: req.n + bump,
    }
}

#[subscriber("ctx-out")]
async fn ctx_sink(resp: &Response) -> HandlerResult {
    CTX_REPLY.store(resp.doubled, Ordering::SeqCst);
    CTX_REPLY_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_handler_reads_context_state() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let replies = TypedPublisher::new(broker.publisher());

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, Infallible>(Bump(100)) })
        .with_broker(broker, |b| {
            b.include_publishing(ctx_reply, replies);
            b.include(ctx_sink);
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = serde_json::to_vec(&Request { n: 1 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress
                .publish(OutgoingMessage::new("ctx-in", &payload))
                .await;
            handler_signal(&CTX_REPLY_NOTIFY).await;
            if CTX_REPLY.load(Ordering::SeqCst) == 101 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "publishing handler did not read app state from the context",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static ATTEMPTS_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Asks for a delayed redelivery on first sight, then acks: the not-ready-yet pattern.
#[subscriber("deferred")]
async fn eventually(order: &Order) -> HandlerResult {
    let _ = order;
    let prev = ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    ATTEMPTS_NOTIFY.notify_one();
    if prev == 0 {
        return HandlerResult::retry_after(Duration::from_millis(10));
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_redelivers_through_the_dispatcher() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(eventually));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // One publish is enough: the second attempt must come from the delayed redelivery.
    let payload = serde_json::to_vec(&Order { id: 5, total: 1.0 }).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ATTEMPTS.load(Ordering::SeqCst) == 0 {
                let _ = publisher
                    .publish(OutgoingMessage::new("deferred", &payload))
                    .await;
            }
            handler_signal(&ATTEMPTS_NOTIFY).await;
            if ATTEMPTS.load(Ordering::SeqCst) >= 2 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "the delayed nack never came back through the dispatcher",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
