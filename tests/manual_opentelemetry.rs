//! The macro-free counterpart of `tests/opentelemetry.rs`: end-to-end W3C trace context
//! propagation with the two subscribers written out.
//!
//! The consume layer and the publish transform are runtime wiring, so they attach to a
//! hand-written definition exactly as they do to a declared one.
#![cfg(all(feature = "otel", feature = "memory", feature = "json"))]

mod common;

use std::future::{Future, ready};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use common::{Req, Resp};
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::otel::OpenTelemetry;
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, Declared, Decoded, Handler, OutgoingMessageMetadata, PublishingCall, PublishingDef,
    Settle, SubscriberBuilder, SubscriberDef, forms,
};
use tokio::sync::Notify;

/// The request half: a reply definition whose destination is fixed by the definition itself.
struct Echo;

impl Declared for Echo {
    type Form = forms::Publishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("in"))
    }
}

impl PublishingDef for Echo {
    type Input = Decoded<Req>;
    type Injections = ();
    type Reply = Resp;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("in")
    }

    fn reply_name(&self) -> &'static str {
        "out"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            "out",
            std::any::type_name::<Resp>(),
        )]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Echo {
    fn call(
        &self,
        req: &Req,
        _injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<Resp, HandlerResult>> + Send {
        ready(Ok(Resp { n: req.n }))
    }
}

static CAPTURED: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));
static GOT: LazyLock<Notify> = LazyLock::new(Notify::new);

/// The far end: records the reply's `traceparent`, which is the whole subject of the file.
struct Capture;

impl<State: Send + Sync> Handler<Resp, (), State> for Capture {
    fn handle(
        &self,
        _resp: &Resp,
        ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Settle> + Send {
        *CAPTURED.lock().expect("poisoned") =
            ctx.headers().get_str("traceparent").map(str::to_owned);
        GOT.notify_one();
        ready(HandlerResult::Ack.into())
    }
}

impl Declared for Capture {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("out"))
    }
}

impl SubscriberDef for Capture {
    type Input = Decoded<Resp>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("out")
    }

    fn into_handler(self) -> Self {
        self
    }
}

/// Serializes the two tests: they share the `CAPTURED` slot and the `in` / `out` channels, so they
/// must not run concurrently (cargo runs a file's tests in parallel by default).
static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

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
    let _serial = SERIAL.lock().await;
    *CAPTURED.lock().expect("poisoned") = None;
    // --8<-- [start:wiring]
    let otel = OpenTelemetry::new();
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    // The reply wiring propagates the delivery's trace context onto each reply.
    let reply_pub = TypedPublisher::new(MemoryPublish).transform(otel.propagation());

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        // The consume layer opens a span per delivery and records the consumer's trace context.
        .layer(otel.consume_layer())
        .with_broker(broker, |b| {
            b.include(Echo).publisher(reply_pub);
            b.include(Capture);
        });
    // --8<-- [end:wiring]

    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Req { n: 1 }).expect("encode");
    let mut headers = HeaderMap::new();
    if let Some(tp) = incoming {
        headers.insert("traceparent", tp);
    }
    ingress
        .raw(&payload)
        .with_headers(headers)
        .to("in")
        .publish()
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), GOT.notified())
        .await
        .expect("reply never captured");

    running.shutdown().await.expect("graceful shutdown failed");

    let header = CAPTURED
        .lock()
        .expect("poisoned")
        .clone()
        .expect("reply carried a traceparent");
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
