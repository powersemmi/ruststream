//! The typed per-delivery context reaches the publish path: a static `PublishTransform` reads the
//! originating delivery (issue #103) and stamps the reply, propagating a correlation id.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{
    AppInfo, Outgoing, PublishContext, PublishDynLayer, PublishDynNext, PublishDynStack,
    PublishLayer, PublishNext, PublishPipeline, PublishTransform, RustStream, TypedPublisher,
    for_batch,
};
use ruststream::{
    BuildContext, Field, Headers, IncomingMessage, OutgoingMessage, Publisher, subscriber,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Req {
    n: u32,
}

#[derive(Serialize, Deserialize)]
struct Resp {
    n: u32,
}

/// A broker context built from the incoming message: it lifts the correlation id off the headers so
/// the handler (and the publish layer) can read it by key instead of re-parsing the headers.
#[derive(Default)]
struct TraceCtx {
    correlation: Option<String>,
}

impl BuildContext<MemoryMessage> for TraceCtx {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            correlation: msg.headers().correlation_id().map(str::to_owned),
        }
    }
}

/// The compile-time key that reads [`TraceCtx::correlation`].
#[derive(Clone, Copy)]
struct Correlation;

impl Field<TraceCtx> for Correlation {
    type Value<'a> = Option<&'a str>;
    fn get(self, c: &TraceCtx) -> Option<&str> {
        c.correlation.as_deref()
    }
}

/// A static, zero-cost publish transform: stamps the originating delivery's correlation id onto the
/// reply, read off the typed context through [`PublishContext`].
struct PropagateCorrelation;

impl PublishTransform<TraceCtx> for PropagateCorrelation {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, TraceCtx>) {
        if let Some(id) = cx.context(Correlation) {
            out.headers_mut()
                .insert("correlation-id", id.as_bytes().to_vec());
        }
    }
}

#[subscriber("in", publish("out"))]
async fn echo(req: &Req, _ctx: &mut Context<'_, TraceCtx>) -> Resp {
    Resp { n: req.n }
}

static CAPTURED: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));
static GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("out")]
async fn capture(_resp: &Resp, ctx: &mut Context<'_>) {
    *CAPTURED.lock().expect("poisoned") = ctx.headers().correlation_id().map(str::to_owned);
    GOT.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_context_propagates_to_the_reply() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();
    let egress_pub = TypedPublisher::new(egress.publisher()).transform(PropagateCorrelation);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(ingress, |b| {
            b.include_publishing(echo, egress_pub);
        })
        .with_broker(egress, |b| {
            b.include(capture);
        });

    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Req { n: 7 }).expect("encode");
    let mut headers = Headers::new();
    headers.insert("correlation-id", "trace-abc");
    ingress_pub
        .publish(OutgoingMessage::new("in", &payload).with_headers(headers))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), GOT.notified())
        .await
        .expect("reply never captured");

    assert_eq!(
        CAPTURED.lock().expect("poisoned").as_deref(),
        Some("trace-abc"),
        "the reply should carry the delivery's correlation id, stamped by the publish layer"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

/// A batch-only transform: marks every batched reply, never a single-message one.
struct MarkBatched;

impl<C> PublishTransform<C> for MarkBatched {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-batched", b"1".to_vec());
    }
}

#[subscriber(batch("batch-in"), publish("batch-out"))]
async fn batch_echo(reqs: &[Req]) -> Vec<Resp> {
    reqs.iter().map(|r| Resp { n: r.n }).collect()
}

static BATCHED: LazyLock<Mutex<Option<bool>>> = LazyLock::new(|| Mutex::new(None));
static BATCH_GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("batch-out")]
async fn batch_capture(_resp: &Resp, ctx: &mut Context<'_>) {
    *BATCHED.lock().expect("poisoned") = Some(ctx.headers().get("x-batched").is_some());
    BATCH_GOT.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_layer_runs_only_on_batched_replies() {
    let broker = MemoryBroker::new();
    let ingress_pub = broker.publisher();
    // The same `MarkBatched` transform, reused on the batch path through `for_batch`; the
    // single-message mounts would reject a publisher carrying it.
    let reply_pub = TypedPublisher::new(broker.publisher()).batch_transform(for_batch(MarkBatched));

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include_batch_publishing(batch_echo, reply_pub);
        b.include(batch_capture);
    });

    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Req { n: 1 }).expect("encode");
    ingress_pub
        .publish(OutgoingMessage::new("batch-in", &payload))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), BATCH_GOT.notified())
        .await
        .expect("batched reply never captured");

    assert_eq!(
        *BATCHED.lock().expect("poisoned"),
        Some(true),
        "the batch layer should stamp every batched reply"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

/// A dynamic, runtime-built publish middleware: stamps a header, then continues.
struct StampDyn;

impl PublishDynLayer for StampDyn {
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishDynNext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
    > {
        Box::pin(async move {
            out.headers_mut().insert("x-dyn", b"1".to_vec());
            next.run(out).await
        })
    }
}

#[subscriber("dyn-in", publish("dyn-out"))]
async fn dyn_echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

static DYN_SEEN: LazyLock<Mutex<Option<bool>>> = LazyLock::new(|| Mutex::new(None));
static DYN_GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("dyn-out")]
async fn dyn_capture(_resp: &Resp, ctx: &mut Context<'_>) {
    *DYN_SEEN.lock().expect("poisoned") = Some(ctx.headers().get("x-dyn").is_some());
    DYN_GOT.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dyn_stack_runs_a_runtime_built_middleware() {
    let broker = MemoryBroker::new();
    let ingress_pub = broker.publisher();
    // The middleware set is decided at runtime and inserted as one static layer.
    let stack = PublishDynStack::new([Arc::new(StampDyn) as Arc<dyn PublishDynLayer>]);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(stack)
        .with_broker(broker, |b| {
            let reply_pub = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(dyn_echo, reply_pub);
            b.include(dyn_capture);
        });

    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Req { n: 3 }).expect("encode");
    ingress_pub
        .publish(OutgoingMessage::new("dyn-in", &payload))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), DYN_GOT.notified())
        .await
        .expect("reply never captured");

    assert_eq!(
        *DYN_SEEN.lock().expect("poisoned"),
        Some(true),
        "the dynamic stack middleware should run and stamp the reply"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

// Two app-wide publish middleware, each appending its letter to an "order" header, pin the
// documented composition: the LAST `publish_layer` added runs OUTERMOST (so it appends first).
type PubFut<'a> =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>;

fn append_order(out: &mut Outgoing<'_>, letter: &str) {
    let mut order = out
        .headers()
        .get_str("order")
        .unwrap_or_default()
        .to_owned();
    order.push_str(letter);
    out.headers_mut().insert("order", order.into_bytes());
}

#[derive(Clone)]
struct AppendA;

impl PublishLayer for AppendA {
    fn on_publish<'a, N: PublishPipeline>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N>,
    ) -> PubFut<'a> {
        append_order(out, "A");
        next.run(out)
    }
}

#[derive(Clone)]
struct AppendB;

impl PublishLayer for AppendB {
    fn on_publish<'a, N: PublishPipeline>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N>,
    ) -> PubFut<'a> {
        append_order(out, "B");
        next.run(out)
    }
}

#[subscriber("ord-in", publish("ord-out"))]
async fn ord_echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

static ORDER: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));
static ORDER_GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("ord-out")]
async fn ord_capture(_resp: &Resp, ctx: &mut Context<'_>) {
    *ORDER.lock().expect("poisoned") = ctx.headers().get_str("order").map(str::to_owned);
    ORDER_GOT.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_layer_last_added_runs_outermost() {
    let broker = MemoryBroker::new();
    let ingress_pub = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(AppendA)
        .publish_layer(AppendB)
        .with_broker(broker, |b| {
            b.include_publishing(ord_echo, TypedPublisher::new(b.broker().publisher()));
            b.include(ord_capture);
        });

    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Req { n: 1 }).expect("encode");
    ingress_pub
        .publish(OutgoingMessage::new("ord-in", &payload))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), ORDER_GOT.notified())
        .await
        .expect("reply never captured");

    // B was added last, so it wraps A and appends first: "BA".
    assert_eq!(
        ORDER.lock().expect("poisoned").as_deref(),
        Some("BA"),
        "the last publish_layer added must run outermost"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}
