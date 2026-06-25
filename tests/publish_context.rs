//! The typed per-delivery context reaches the publish path: a static `PublishLayer` reads the
//! originating delivery (issue #103) and stamps the reply, propagating a correlation id.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{
    AppInfo, Outgoing, PublishContext, PublishLayer, RustStream, TypedPublisher,
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

impl PublishLayer<TraceCtx> for PropagateCorrelation {
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
    let egress_pub = TypedPublisher::new(egress.publisher()).layer(PropagateCorrelation);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(ingress, |b| {
            b.include_publishing(echo, egress_pub);
        })
        .with_broker(egress, |b| {
            b.include(capture);
        });

    let shutdown = Arc::new(Notify::new());
    let signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { signal.notified().await }));

    let payload = serde_json::to_vec(&Req { n: 7 }).expect("encode");
    // Re-publish until the reaction lands: the first sends can race subscription startup, and the
    // in-memory broker drops a message with no subscriber yet (see the metrics publish test).
    let captured = tokio::time::timeout(Duration::from_secs(2), async {
        let notified = GOT.notified();
        tokio::pin!(notified);
        loop {
            let mut headers = Headers::new();
            headers.insert("correlation-id", "trace-abc");
            ingress_pub
                .publish(OutgoingMessage::new("in", &payload).with_headers(headers))
                .await
                .expect("publish");
            tokio::select! {
                () = &mut notified => break,
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await;
    assert!(captured.is_ok(), "reply never captured");

    assert_eq!(
        CAPTURED.lock().expect("poisoned").as_deref(),
        Some("trace-abc"),
        "the reply should carry the delivery's correlation id, stamped by the publish layer"
    );

    shutdown.notify_one();
    run.await.expect("join").expect("run");
}
