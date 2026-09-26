//! The typed per-delivery context reaches the publish path: a static `PublishTransform` reads the
//! originating delivery and stamps the reply, propagating a correlation id. The app-wide publish
//! layers wrap what the transforms produced.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::Future;

use common::{Req, Resp};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{
    ContextKind, ForReply, Outgoing, PublishContext, PublishLayer, PublishNext, PublishPipeline,
    PublishTransform, Reads,
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

impl<Options> PublishTransform<ForReply<TraceCtx>, Options> for PropagateCorrelation {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, TraceCtx>,
    ) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_context_propagates_to_the_reply() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(echo)
            .out(Reply, Publish)
            .transform(PropagateCorrelation);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "trace-abc");
    tb.message(&Req { n: 7 })
        .with_headers(headers)
        .to("in")
        .publish()
        .await
        .expect("publish");

    // The reply carries the delivery's correlation id, stamped by the publish transform.
    tb.broker::<MemoryBroker>()
        .published::<Resp>("out")
        .assert_called_once()
        .with(&Resp { n: 7 })
        .with_header("correlation-id", b"trace-abc");
}

/// Writes into the delivery's working copy of the headers before it answers.
#[subscriber("working-in", publish("working-out"))]
async fn enrich(req: &Req, ctx: &mut Context<'_>) -> Resp {
    ctx.headers_mut().insert("x-working", "1");
    Resp { n: req.n }
}

/// The working copy on the context belongs to the delivery: neither it nor the delivery's own
/// headers reach the reply, which carries only what its publish path writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_context_headers_never_reach_the_reply() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(enrich).out(Reply, Publish);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("x-incoming", "1");
    tb.message(&Req { n: 2 })
        .with_headers(headers)
        .to("working-in")
        .publish()
        .await
        .expect("publish");

    let reply = tb
        .broker::<MemoryBroker>()
        .published::<Resp>("working-out")
        .assert_called_once()
        .with(&Resp { n: 2 });
    assert!(
        reply.messages()[0].headers().is_empty(),
        "the reply carried headers it never published: {:?}",
        reply.messages()[0].headers(),
    );
}

// Two app-wide publish middleware and one reply transform, each appending its letter to an
// "order" header, pin the documented composition: the position's transforms first, then the
// app-wide layers, the LAST `publish_layer` added running OUTERMOST (so it appends first).
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

/// The reply position's own transform.
struct AppendT;

impl<K: ContextKind, Options> PublishTransform<K, Options> for AppendT {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        append_order(out, "T");
    }
}

#[subscriber("ord-in", publish("ord-out"))]
async fn ord_echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transforms_run_first_and_the_last_publish_layer_added_runs_outermost() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(AppendA)
        .publish_layer(AppendB)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ord_echo).out(Reply, Publish).transform(AppendT);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Req { n: 1 })
        .to("ord-in")
        .publish()
        .await
        .expect("publish");

    // The transform wrote first; B was added last, so it wraps A and appends before it: "TBA".
    tb.broker::<MemoryBroker>()
        .published::<Resp>("ord-out")
        .assert_called_once()
        .with(&Resp { n: 1 })
        .with_header("order", b"TBA");
}
