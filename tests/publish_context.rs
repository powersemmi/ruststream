//! The typed per-delivery context reaches the publish path: a static `PublishTransform` reads the
//! originating delivery and stamps the reply, propagating a correlation id.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use common::{Req, Resp};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{
    Outgoing, PublishContext, PublishDynLayer, PublishDynNext, PublishDynStack, PublishLayer,
    PublishNext, PublishPipeline, PublishTransform, for_batch,
};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, Field};

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

#[subscriber("out")]
async fn capture(_resp: &Resp) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_context_propagates_to_the_reply() {
    let egress = MemoryBroker::new().bindable();
    let egress_pub = egress.bind(Publish);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("egress", egress, |b| {
            b.include(capture);
        })
        .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
            b.include(echo)
                .publisher(egress_pub)
                .transform(PropagateCorrelation);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "trace-abc");
    tb.broker_named("ingress")
        .message(&Req { n: 7 })
        .with_headers(headers)
        .to("in")
        .publish()
        .await
        .expect("publish");

    // The reply carries the delivery's correlation id, stamped by the publish layer.
    tb.broker_named("egress")
        .published::<Resp>("out")
        .assert_called_once()
        .with(&Resp { n: 7 })
        .with_header("correlation-id", b"trace-abc");
    tb.broker_named("egress")
        .subscriber("out")
        .assert_called_once()
        .with(&Resp { n: 7 });
}

/// A batch-only transform: marks every batched reply, never a single-message one.
struct MarkBatched;

impl<C> PublishTransform<C> for MarkBatched {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-batched", b"1".to_vec());
    }
}

#[subscriber("batch-in", publish("batch-out"))]
async fn batch_echo(reqs: &[Req]) -> Vec<Resp> {
    reqs.iter().map(|r| Resp { n: r.n }).collect()
}

#[subscriber("batch-out")]
async fn batch_capture(_resp: &Resp) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_layer_runs_only_on_batched_replies() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The same `MarkBatched` transform, reused on the batch path through `for_batch`; the
        // single-message mounts would reject a wiring carrying it.
        b.include(
            batch_echo
                .batch(nonzero!(8))
                .publisher(Publish)
                .batch_transform(for_batch(MarkBatched))
                .build(),
        );
        b.include(batch_capture);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Req { n: 1 })
        .to("batch-in")
        .publish()
        .await
        .expect("publish");

    // The batch layer stamps every batched reply.
    tb.broker::<MemoryBroker>()
        .published::<Resp>("batch-out")
        .assert_called_once()
        .with(&Resp { n: 1 })
        .with_header("x-batched", b"1");
    tb.broker::<MemoryBroker>()
        .subscriber("batch-out")
        .assert_called_once()
        .with(&Resp { n: 1 });
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

#[subscriber("dyn-out")]
async fn dyn_capture(_resp: &Resp) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dyn_stack_runs_a_runtime_built_middleware() {
    // The middleware set is decided at runtime and inserted as one static layer.
    let stack = PublishDynStack::new([Arc::new(StampDyn) as Arc<dyn PublishDynLayer>]);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(stack)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(dyn_echo);
            b.include(dyn_capture);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Req { n: 3 })
        .to("dyn-in")
        .publish()
        .await
        .expect("publish");

    // The dynamic stack middleware runs and stamps the reply.
    tb.broker::<MemoryBroker>()
        .published::<Resp>("dyn-out")
        .assert_called_once()
        .with(&Resp { n: 3 })
        .with_header("x-dyn", b"1");
    tb.broker::<MemoryBroker>()
        .subscriber("dyn-out")
        .assert_called_once()
        .with(&Resp { n: 3 });
}

// Two app-wide publish middleware, each appending its letter to an "order" header, pin the
// documented composition: the LAST `publish_layer` added runs OUTERMOST (so it appends first).
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
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        append_order(out, "A");
        next.run(out)
    }
}

#[derive(Clone)]
struct AppendB;

impl PublishLayer for AppendB {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        append_order(out, "B");
        next.run(out)
    }
}

#[subscriber("ord-in", publish("ord-out"))]
async fn ord_echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

#[subscriber("ord-out")]
async fn ord_capture(_resp: &Resp) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_layer_last_added_runs_outermost() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(AppendA)
        .publish_layer(AppendB)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ord_echo);
            b.include(ord_capture);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Req { n: 1 })
        .to("ord-in")
        .publish()
        .await
        .expect("publish");

    // B was added last, so it wraps A and appends first: "BA".
    tb.broker::<MemoryBroker>()
        .published::<Resp>("ord-out")
        .assert_called_once()
        .with_header("order", b"BA");
    tb.broker::<MemoryBroker>()
        .subscriber("ord-out")
        .assert_called_once()
        .with(&Resp { n: 1 });
}
