//! End-to-end W3C trace context propagation: the consume layer stamps the consumer span and the
//! publish layer carries it onto the reply.
#![cfg(all(
    feature = "otel",
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Req, Resp};
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use ruststream::memory::prelude::*;
use ruststream::otel::OpenTelemetry;
use ruststream::testing::TestApp;

#[subscriber("in", publish("out"))]
async fn echo(req: &Req) -> Resp {
    Resp { n: req.n }
}

/// The far end of the reply channel: it proves the stamped reply is deliverable, while the
/// header itself is read off the broker's publish log.
#[subscriber("out")]
async fn capture(resp: &Resp) -> HandlerOutcome {
    let _ = resp.n;
    HandlerOutcome::ack()
}

/// Parses a `traceparent` header value through the SDK propagator, the way a downstream service
/// would, and returns the span context it names.
fn parse_traceparent(header: &str) -> SpanContext {
    struct Single<'a>(&'a str);
    impl Extractor for Single<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            (key == "traceparent").then_some(self.0)
        }
        fn keys(&self) -> Vec<&str> {
            vec!["traceparent"]
        }
    }
    TraceContextPropagator::new()
        .extract_with_context(&OtelContext::new(), &Single(header))
        .span()
        .span_context()
        .clone()
}

/// Drives one request through the app (`start()` resolves with subscriptions already open, so a
/// single publish lands) and returns the captured reply `traceparent`, parsed.
async fn run_and_capture(incoming: Option<&'static str>) -> SpanContext {
    // --8<-- [start:wiring]
    let otel = OpenTelemetry::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        // The consume layer opens a span per delivery and records the consumer's trace context.
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            // The reply wiring propagates the delivery's trace context onto each reply.
            b.include(echo)
                .out(Reply, Publish)
                .transform(otel.propagation());
            b.include(capture);
        });
    // --8<-- [end:wiring]

    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    if let Some(tp) = incoming {
        headers.insert("traceparent", tp);
    }
    tb.message(&Req { n: 1 })
        .with_headers(headers)
        .to("in")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("out")
        .assert_called_once()
        .with(&Resp { n: 1 });

    let published = tb.broker::<MemoryBroker>().published::<Resp>("out");
    let header = published
        .messages()
        .first()
        .expect("the reply was published")
        .headers()
        .get_str("traceparent")
        .expect("reply carried a traceparent")
        .to_owned();
    let reply = parse_traceparent(&header);
    assert!(reply.is_valid(), "reply traceparent is valid");
    reply
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_trace_continues_onto_the_reply() {
    let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let parsed = parse_traceparent(incoming);

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
    assert!(reply.is_sampled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trace_is_started_when_none_arrives() {
    let reply = run_and_capture(None).await;
    // A fresh, sampled root trace was started for the untraced delivery (`run_and_capture`
    // already asserted the ids are valid, that is, non-zero).
    assert!(reply.is_sampled());
}
