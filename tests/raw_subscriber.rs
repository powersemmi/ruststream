//! Integration tests for the raw form, which the macro reads off a `Deserialized` payload
//! parameter: the handler receives each delivery's payload bytes untouched, with no codec
//! anywhere on the path.
//!
//! The codec-free path itself is additionally pinned by a feature-stripped compile:
//! `cargo check --no-default-features --features macros,memory,testing --test raw_subscriber`
//! builds this file with every codec-gated test compiled out.
#![cfg(all(feature = "macros", feature = "memory", feature = "testing"))]

use std::convert::Infallible;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryMessage, MemoryPublisher};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField, OutgoingMessage, PairError};

/// Deliberately not valid JSON (or UTF-8): a decode step anywhere on the path would fail it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

/// The named payload view every byte-lane handler below takes: the delivery's bytes, borrowed
/// straight out of the broker's buffer.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The named reply the byte-lane handlers return: its bytes leave on the wire as they are, with
/// no codec in between.
#[derive(Serialized)]
struct Export(Vec<u8>);

/// The named wire this suite injects its deliberately unstructured payloads through. Publishing
/// is typed, and `Serialized` is what keeps the injection on the codec-free lane the handlers
/// under test read from.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

impl Wire {
    /// The wire form of `bytes`, for the call sites that hold a slice or a literal.
    fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }
}

// --- the plain form: the handler sees the exact published bytes ---

// --8<-- [start:raw]
#[subscriber("frames")]
async fn on_frame(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}
// --8<-- [end:raw]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_handler_receives_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(on_frame));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("frames")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        // The recorded payload is what the handler was called with, so this is the assertion
        // that the bytes reached it untouched.
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
}

// --- the byte reply form: a Serialized reply republishes the returned bytes as-is ---

// --8<-- [start:raw_reply]
#[subscriber("relay-in", publish("relay-out"))]
async fn relay(frame: &Frame<'_>) -> Export {
    let mut reply = frame.0.to_vec();
    reply.reverse();
    Export(reply)
}
// --8<-- [end:raw_reply]

#[subscriber("relay-out")]
async fn relay_capture(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_round_trips_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay).publisher(Publish);
        b.include(relay_capture);
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("relay-in")
        .publish()
        .await
        .expect("publish");

    let mut expected = FRAME.to_vec();
    expected.reverse();
    tb.broker::<MemoryBroker>()
        .subscriber("relay-in")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-out")
        .assert_called_once()
        .with_raw(&expected)
        .settled(HandlerOutcome::ack());
}

// --- without .publisher(..) the reply commits with the broker's default publish policy ---

#[subscriber("relay-default-in", publish("relay-default-out"))]
async fn relay_default(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

#[subscriber("relay-default-out")]
async fn relay_default_capture(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_defaults_to_the_brokers_publish_policy() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay_default);
        b.include(relay_default_capture);
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("relay-default-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("relay-default-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
}

// --- the Result form: Err skips the publish and settles by the returned HandlerOutcome ---

#[subscriber("relay-checked-in", publish("relay-checked-out"))]
async fn relay_checked(frame: &Frame<'_>) -> Result<Export, HandlerOutcome> {
    if frame.0.is_empty() {
        return Err(HandlerOutcome::drop());
    }
    Ok(Export(frame.0.to_vec()))
}

#[subscriber("relay-checked-out")]
async fn relay_checked_capture(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_result_form_controls_the_publish() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay_checked).publisher(Publish);
        b.include(relay_checked_capture);
    });

    let tb = TestApp::start(app).await.expect("start");

    // The Err arm: nothing is published and the delivery settles by the returned result.
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b""))
        .to("relay-checked-in")
        .publish()
        .await
        .expect("publish empty");
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-in")
        .assert_called_once()
        .settled(HandlerOutcome::drop());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-out")
        .assert_called(0);

    // The Ok arm publishes the bytes as-is.
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("relay-checked-in")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-in")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
}

// --- a failed reply publish nacks the delivery with requeue, like the typed reply form ---

/// A policy whose live publisher fails its first publish, then delegates to the real one:
/// exercises the reply-publish failure path without tearing a broker down.
struct FlakyPublish(Arc<AtomicBool>);

struct FlakyPublisher {
    inner: MemoryPublisher,
    fail_next: Arc<AtomicBool>,
}

impl Publisher for FlakyPublisher {
    type Error = MemoryError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), MemoryError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(MemoryError::ShutDown);
        }
        self.inner.publish(msg).await
    }
}

impl PublishPolicy<ConnectedMemoryBroker> for FlakyPublish {
    type Live = FlakyPublisher;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<FlakyPublisher, PairError>> {
        ready(Ok(FlakyPublisher {
            inner: connected.publisher(),
            fail_next: self.0,
        }))
    }
}

#[subscriber("relay-flaky-in", publish("relay-flaky-out"))]
async fn relay_flaky(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

#[subscriber("relay-flaky-out")]
async fn relay_flaky_capture(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_raw_reply_publish_nacks_and_redelivers() {
    let fail_next = Arc::new(AtomicBool::new(true));
    let publisher_flag = Arc::clone(&fail_next);
    let app =
        RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), move |b| {
            b.include(relay_flaky)
                .publisher(FlakyPublish(publisher_flag));
            b.include(relay_flaky_capture);
        });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("relay-flaky-in")
        .publish()
        .await
        .expect("publish");

    // The first delivery's reply publish fails, so it nacks with requeue; the redelivery
    // publishes and acks. The reply reaches the capture exactly once.
    tb.broker::<MemoryBroker>()
        .subscriber("relay-flaky-in")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-flaky-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
    assert!(
        !fail_next.load(Ordering::SeqCst),
        "the flaky publisher consumed its failure"
    );
}

// --- a Serialized reply with a TYPED input: decode with the scope codec, reply bytes as-is ---

#[cfg(feature = "json")]
mod typed_in {
    use serde::Deserialize;

    use super::{
        AppInfo, Export, FRAME, Frame, HandlerOutcome, MemoryBroker, Publish, RustStream, TestApp,
        Wire, subscriber,
    };

    #[derive(Debug, Deserialize)]
    struct Wrap {
        id: u32,
    }

    // --8<-- [start:raw_reply_typed]
    /// The gateway shape: a structured message in, a self-produced wire format out.
    #[subscriber("gateway-in", publish("gateway-out"))]
    async fn gateway(wrap: &Wrap) -> Export {
        Export(wrap.id.to_be_bytes().to_vec())
    }
    // --8<-- [end:raw_reply_typed]

    /// The Result form keeps ack control: an odd id skips the publish and drops.
    #[subscriber("gateway-checked-in", publish("gateway-checked-out"))]
    async fn gateway_checked(wrap: &Wrap) -> Result<Export, HandlerOutcome> {
        if wrap.id % 2 == 1 {
            return Err(HandlerOutcome::drop());
        }
        Ok(Export(wrap.id.to_be_bytes().to_vec()))
    }

    #[subscriber("gateway-out")]
    async fn gateway_capture(frame: &Frame<'_>) -> HandlerOutcome {
        assert_eq!(frame.0, 7_u32.to_be_bytes(), "the reply bytes arrive as-is");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_input_replies_raw_bytes() {
        let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(gateway).publisher(Publish);
                b.include(gateway_capture);
            },
        );

        let tb = TestApp::start(app).await.expect("start");
        tb.broker::<MemoryBroker>()
            .publish("gateway-in", &serde_json::json!({"id": 7}))
            .await
            .expect("publish");

        tb.broker::<MemoryBroker>()
            .subscriber("gateway-in")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-out")
            .assert_called_once()
            .with_raw(7_u32.to_be_bytes().as_slice())
            .settled(HandlerOutcome::ack());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_input_decode_and_result_control_apply() {
        let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                // The default publish policy commits without an explicit .publisher call.
                b.include(gateway_checked);
            },
        );

        let tb = TestApp::start(app).await.expect("start");
        // Not valid JSON: the typed input side keeps the decode failure policy, unlike raw.
        tb.broker::<MemoryBroker>()
            .message(&Wire::of(FRAME))
            .to("gateway-checked-in")
            .publish()
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-checked-in")
            .assert_last_failed_to_decode();

        // An odd id decodes but the handler skips the publish via Err(drop()).
        tb.broker::<MemoryBroker>()
            .publish("gateway-checked-in", &serde_json::json!({"id": 3}))
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-checked-in")
            .settled(HandlerOutcome::drop());
        // A skipped reply must not publish.
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-checked-out")
            .assert_not_called();
    }
}

// --- extractors and the ctx parameter keep working next to the raw payload ---

#[derive(FromRef)]
struct CountState {
    bytes_seen: Arc<AtomicUsize>,
}

#[subscriber("frames-state")]
async fn with_state(
    frame: &Frame<'_>,
    ctx: &mut Context,
    State(bytes_seen): State<Arc<AtomicUsize>>,
) -> HandlerOutcome {
    assert_eq!(ctx.name(), "frames-state");
    bytes_seen.fetch_add(frame.0.len(), Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_extractor_and_ctx_resolve_alongside_raw() {
    let bytes_seen = Arc::new(AtomicUsize::new(0));
    let state_bytes = bytes_seen.clone();
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .on_startup(move |()| async move {
            Ok::<_, Infallible>(CountState {
                bytes_seen: state_bytes,
            })
        })
        .with_broker(MemoryBroker::new(), |b| b.include(with_state));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("frames-state")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames-state")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(
        bytes_seen.load(Ordering::Relaxed),
        FRAME.len(),
        "the State extractor handed the counter to the raw handler"
    );
}

// --- a Ctx<K> extractor projects the broker context under raw, exactly as in the typed form ---

/// A broker-style per-delivery context built from the message, standing in for an offset /
/// delivery tag a real broker would expose.
struct FrameMeta {
    payload_len: usize,
}

impl BuildContext<MemoryMessage> for FrameMeta {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            payload_len: msg.payload().len(),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct FrameLen;

impl ContextField for FrameLen {
    type Context = FrameMeta;
    type Value = usize;
    fn read(self, src: &FrameMeta) -> usize {
        src.payload_len
    }
}

/// Where the handler records the length its context field carried: a context value never leaves
/// the handler, so it comes back through application state.
#[derive(Clone)]
struct SeenLen(Arc<AtomicUsize>);

#[derive(FromRef)]
struct MeasuredState {
    seen: SeenLen,
}

#[subscriber("frames-meta")]
async fn measured(
    _frame: &Frame<'_>,
    Ctx(len): Ctx<FrameLen>,
    State(seen): State<SeenLen>,
) -> HandlerOutcome {
    seen.0.store(len, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractor_projects_the_context_under_raw() {
    let seen_len = Arc::new(AtomicUsize::new(0));
    let state_seen = SeenLen(Arc::clone(&seen_len));
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, Infallible>(MeasuredState { seen: state_seen }) },
        )
        .with_broker(MemoryBroker::new(), |b| b.include(measured));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("frames-meta")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames-meta")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(seen_len.load(Ordering::Relaxed), FRAME.len());
}

// --- workers(..) and on_failure(panic = ..) keep working on the raw form ---

#[subscriber("frames-workers", workers(2), on_failure(panic = drop))]
async fn tolerant(frame: &Frame<'_>) -> HandlerOutcome {
    assert_ne!(frame.0, b"boom", "poison frame");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workers_and_panic_policy_apply_to_raw() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(tolerant));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"boom"))
        .to("frames-workers")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("frames-workers")
        .assert_called_once()
        .panicked();

    // The panic policy dropped the poison frame; the app keeps serving.
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"ok"))
        .to("frames-workers")
        .publish()
        .await
        .expect("publish after panic");
    tb.broker::<MemoryBroker>()
        .subscriber("frames-workers")
        .assert_called(2)
        .with_raw(b"ok")
        .settled(HandlerOutcome::ack());
}

// --- a Router mounts raw definitions through the form-dispatched include ---

#[subscriber("routed-raw")]
async fn routed(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_mounts_raw_definitions() {
    let router = Router::<MemoryBroker>::new().include(routed);
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("routed-raw")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("routed-raw")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
}

#[subscriber("routed-relay-in", publish("routed-relay-out"))]
async fn routed_relay(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

/// The byte-reply form on a router, next to the scope-mounted one above: both mounts resolve
/// the decode codec against the input kind, so neither asks for a codec the byte path does not
/// use. This file compiles with no codec feature at all, which is what pins that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_mounts_a_byte_reply_definition() {
    let router = Router::<MemoryBroker>::new()
        .include(routed_relay)
        .publisher(Publish)
        .build();
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
        .to("routed-relay-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Export>("routed-relay-out")
        .assert_called_once()
        .with_raw(FRAME);
}

// --- under a scope codec, raw mounts ignore it while typed neighbours decode with it ---

#[cfg(feature = "json")]
mod scope_codec {
    use super::*;

    use ruststream::Outgoing;
    use ruststream::codec::JsonCodec;
    use serde::{Deserialize, Serialize};

    #[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
    struct Order {
        id: u32,
    }

    #[subscriber("mixed-raw")]
    async fn raw_side(frame: &Frame<'_>) -> HandlerOutcome {
        let _ = frame.0;
        HandlerOutcome::ack()
    }

    #[subscriber("mixed-typed")]
    async fn typed_side(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raw_ignores_the_scope_codec() {
        let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            JsonCodec,
            |b| {
                b.include(raw_side);
                b.include(typed_side);
            },
        );

        let tb = TestApp::start(app).await.expect("start");
        // Bytes no JSON decoder would accept reach the raw handler untouched...
        tb.broker::<MemoryBroker>()
            .message(&Wire::of(FRAME))
            .to("mixed-raw")
            .publish()
            .await
            .expect("publish raw");
        tb.broker::<MemoryBroker>()
            .subscriber("mixed-raw")
            .assert_called_once()
            .with_raw(FRAME)
            .settled(HandlerOutcome::ack());

        // ...while the typed neighbour still decodes with the scope codec.
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 9 })
            .to("mixed-typed")
            .publish()
            .await
            .expect("publish typed");
        tb.broker::<MemoryBroker>()
            .subscriber("mixed-typed")
            .assert_called_once()
            .with(&Order { id: 9 })
            .settled(HandlerOutcome::ack());
    }
}

// --- AsyncAPI: a raw subscriber has no input schema, but its channel is still listed ---

#[cfg(feature = "asyncapi")]
mod asyncapi_listing {
    use super::*;

    use ruststream::asyncapi::build_spec;

    /// Consumes raw frames.
    #[subscriber("frames-doc")]
    async fn documented(_frame: &Frame<'_>) {}

    #[test]
    fn raw_channel_is_listed_without_a_schema() {
        let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include(documented));

        let spec = build_spec(&app);
        let json = spec.to_json().expect("serialize spec");
        assert!(
            json.contains("\"frames-doc\""),
            "the raw channel is listed: {json}"
        );
        assert!(
            json.contains("Consumes raw frames."),
            "the doc-comment description flows into the spec: {json}"
        );
    }
}
