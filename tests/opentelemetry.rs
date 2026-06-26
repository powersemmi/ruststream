//! End-to-end W3C trace context propagation: the consume layer stamps the consumer span and the
//! publish layer carries it onto the reply (the `opentelemetry` feature, built on #103).
#![cfg(all(
    feature = "opentelemetry",
    feature = "macros",
    feature = "memory",
    feature = "json"
))]

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::opentelemetry::{OpenTelemetry, TraceContext};
use ruststream::runtime::{AppInfo, Context, RustStream, TypedPublisher};
use ruststream::{Headers, OutgoingMessage, Publisher, subscriber};
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

#[subscriber("in", publish("out"))]
async fn echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

static CAPTURED: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));
static GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("out")]
async fn capture(_resp: &Resp, ctx: &mut Context<'_>) {
    *CAPTURED.lock().expect("poisoned") = ctx.headers().get_str("traceparent").map(str::to_owned);
    GOT.notify_one();
}

/// Drives the app until the reply lands, re-publishing to defeat the startup race, and returns the
/// captured reply `traceparent`.
async fn run_and_capture(incoming: Option<&'static str>) -> TraceContext {
    *CAPTURED.lock().expect("poisoned") = None;
    let otel = OpenTelemetry::new();
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let reply_pub = TypedPublisher::new(broker.publisher()).layer(otel.propagation());

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .with_broker(broker, |b| {
            b.include_publishing(echo, reply_pub);
            b.include(capture);
        });

    let shutdown = Arc::new(Notify::new());
    let signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { signal.notified().await }));

    let payload = serde_json::to_vec(&Req { n: 1 }).expect("encode");
    let captured = tokio::time::timeout(Duration::from_secs(2), async {
        let notified = GOT.notified();
        tokio::pin!(notified);
        loop {
            let mut headers = Headers::new();
            if let Some(tp) = incoming {
                headers.insert("traceparent", tp);
            }
            ingress
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

    shutdown.notify_one();
    run.await.expect("join").expect("run");

    let header = CAPTURED
        .lock()
        .expect("poisoned")
        .clone()
        .expect("reply carried a traceparent");
    TraceContext::parse(&header).expect("reply traceparent is valid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_trace_continues_onto_the_reply() {
    let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let parsed = TraceContext::parse(incoming).unwrap();

    let reply = run_and_capture(Some(incoming)).await;

    assert_eq!(
        reply.trace_id(),
        parsed.trace_id(),
        "the reply stays in the incoming trace"
    );
    assert_ne!(
        reply.span_id(),
        parsed.span_id(),
        "the reply's parent is the consumer span, not the upstream one"
    );
    assert!(reply.sampled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trace_is_started_when_none_arrives() {
    let reply = run_and_capture(None).await;
    // A fresh, sampled root trace was started for the untraced delivery.
    assert!(reply.sampled());
    assert_eq!(reply.trace_id().len(), 32);
}
