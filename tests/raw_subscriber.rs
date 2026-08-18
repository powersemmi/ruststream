//! Integration tests for the raw `#[subscriber(.., raw)]` form: the handler receives each
//! delivery's payload bytes untouched, with no codec anywhere on the path.
//!
//! The codec-free path itself is additionally pinned by a feature-stripped compile:
//! `cargo check --no-default-features --features macros,memory,testing --test raw_subscriber`
//! builds this file with every codec-gated test compiled out.
#![cfg(all(feature = "macros", feature = "memory", feature = "testing"))]

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublish, MemoryPublisher,
};
use ruststream::runtime::{AppInfo, Ctx, HandlerResult, Router, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{
    BuildContext, ContextField, FromRef, IncomingMessage, OutgoingMessage, PairError,
    PublishPolicy, Publisher, subscriber,
};

/// Deliberately not valid JSON (or UTF-8): a decode step anywhere on the path would fail it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

// --- the plain form: the handler sees the exact published bytes ---

static FRAMES: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

// --8<-- [start:raw]
#[subscriber("frames", raw)]
async fn on_frame(frame: &[u8]) -> HandlerResult {
    FRAMES.lock().expect("frame log").push(frame.to_vec());
    HandlerResult::Ack
}
// --8<-- [end:raw]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_handler_receives_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(on_frame));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("frames", FRAME)
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    assert_eq!(
        FRAMES.lock().expect("frame log").as_slice(),
        &[FRAME.to_vec()],
        "the handler saw the published bytes untouched"
    );
}

// --- the reply form: raw, publish_raw("dest") republishes the returned bytes as-is ---

// --8<-- [start:raw_reply]
#[subscriber("relay-in", raw, publish_raw("relay-out"))]
async fn relay(frame: &[u8]) -> Vec<u8> {
    let mut reply = frame.to_vec();
    reply.reverse();
    reply
}
// --8<-- [end:raw_reply]

#[subscriber("relay-out", raw)]
async fn relay_capture(_frame: &[u8]) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_round_trips_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay).publisher(MemoryPublish);
        b.include(relay_capture);
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("relay-in", FRAME)
        .await
        .expect("publish");

    let mut expected = FRAME.to_vec();
    expected.reverse();
    tb.broker::<MemoryBroker>()
        .subscriber("relay-in")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .subscriber("relay-out")
        .assert_called_once()
        .with_raw(&expected)
        .settled(HandlerResult::Ack);
}

// --- without .publisher(..) the reply commits with the broker's default publish policy ---

#[subscriber("relay-default-in", raw, publish_raw("relay-default-out"))]
async fn relay_default(frame: &[u8]) -> Vec<u8> {
    frame.to_vec()
}

#[subscriber("relay-default-out", raw)]
async fn relay_default_capture(_frame: &[u8]) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_defaults_to_the_brokers_publish_policy() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay_default);
        b.include(relay_default_capture);
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("relay-default-in", FRAME)
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("relay-default-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
}

// --- the Result form: Err skips the publish and settles by the returned HandlerResult ---

#[subscriber("relay-checked-in", raw, publish_raw("relay-checked-out"))]
async fn relay_checked(frame: &[u8]) -> Result<Vec<u8>, HandlerResult> {
    if frame.is_empty() {
        return Err(HandlerResult::drop());
    }
    Ok(frame.to_vec())
}

#[subscriber("relay-checked-out", raw)]
async fn relay_checked_capture(_frame: &[u8]) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_result_form_controls_the_publish() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(relay_checked).publisher(MemoryPublish);
        b.include(relay_checked_capture);
    });

    let tb = TestApp::start(app).await.expect("start");

    // The Err arm: nothing is published and the delivery settles by the returned result.
    tb.broker::<MemoryBroker>()
        .publish_raw("relay-checked-in", b"")
        .await
        .expect("publish empty");
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-in")
        .assert_called_once()
        .settled(HandlerResult::drop());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-out")
        .assert_called(0);

    // The Ok arm publishes the bytes as-is.
    tb.broker::<MemoryBroker>()
        .publish_raw("relay-checked-in", FRAME)
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-in")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .subscriber("relay-checked-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
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

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<FlakyPublisher, PairError> {
        Ok(FlakyPublisher {
            inner: connected.publisher(),
            fail_next: self.0,
        })
    }
}

#[subscriber("relay-flaky-in", raw, publish_raw("relay-flaky-out"))]
async fn relay_flaky(frame: &[u8]) -> Vec<u8> {
    frame.to_vec()
}

#[subscriber("relay-flaky-out", raw)]
async fn relay_flaky_capture(_frame: &[u8]) -> HandlerResult {
    HandlerResult::Ack
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
        .publish_raw("relay-flaky-in", FRAME)
        .await
        .expect("publish");

    // The first delivery's reply publish fails, so it nacks with requeue; the redelivery
    // publishes and acks. The reply reaches the capture exactly once.
    tb.broker::<MemoryBroker>()
        .subscriber("relay-flaky-in")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .subscriber("relay-flaky-out")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    assert!(
        !fail_next.load(Ordering::SeqCst),
        "the flaky publisher consumed its failure"
    );
}

// --- publish_raw with a TYPED input: decode with the scope codec, reply bytes as-is ---

#[cfg(feature = "json")]
mod typed_in {
    use serde::Deserialize;

    use super::{
        AppInfo, FRAME, HandlerResult, MemoryBroker, MemoryPublish, RustStream, TestApp, subscriber,
    };

    #[derive(Debug, Deserialize)]
    struct Wrap {
        id: u32,
    }

    // --8<-- [start:raw_reply_typed]
    /// The gateway shape: a structured message in, a self-produced wire format out.
    #[subscriber("gateway-in", publish_raw("gateway-out"))]
    async fn gateway(wrap: &Wrap) -> Vec<u8> {
        wrap.id.to_be_bytes().to_vec()
    }
    // --8<-- [end:raw_reply_typed]

    /// The Result form keeps ack control: an odd id skips the publish and drops.
    #[subscriber("gateway-checked-in", publish_raw("gateway-checked-out"))]
    async fn gateway_checked(wrap: &Wrap) -> Result<Vec<u8>, HandlerResult> {
        if wrap.id % 2 == 1 {
            return Err(HandlerResult::drop());
        }
        Ok(wrap.id.to_be_bytes().to_vec())
    }

    #[subscriber("gateway-out", raw)]
    async fn gateway_capture(frame: &[u8]) -> HandlerResult {
        assert_eq!(frame, 7_u32.to_be_bytes(), "the reply bytes arrive as-is");
        HandlerResult::Ack
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_input_replies_raw_bytes() {
        let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(gateway).publisher(MemoryPublish);
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
            .settled(HandlerResult::Ack);
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-out")
            .assert_called_once()
            .with_raw(7_u32.to_be_bytes().as_slice())
            .settled(HandlerResult::Ack);
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
            .publish_raw("gateway-checked-in", FRAME)
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
            .settled(HandlerResult::drop());
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

#[subscriber("frames-state", raw)]
async fn with_state(
    frame: &[u8],
    ctx: &mut Context,
    State(bytes_seen): State<Arc<AtomicUsize>>,
) -> HandlerResult {
    assert_eq!(ctx.name(), "frames-state");
    bytes_seen.fetch_add(frame.len(), Ordering::Relaxed);
    HandlerResult::Ack
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
        .publish_raw("frames-state", FRAME)
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames-state")
        .assert_called_once()
        .settled(HandlerResult::Ack);
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

static SEEN_LEN: AtomicUsize = AtomicUsize::new(0);

#[subscriber("frames-meta", raw)]
async fn measured(_frame: &[u8], Ctx(len): Ctx<FrameLen>) -> HandlerResult {
    SEEN_LEN.store(len, Ordering::Relaxed);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractor_projects_the_context_under_raw() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(measured));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("frames-meta", FRAME)
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames-meta")
        .assert_called_once()
        .settled(HandlerResult::Ack);
    assert_eq!(SEEN_LEN.load(Ordering::Relaxed), FRAME.len());
}

// --- workers(..) and on_failure(panic = ..) keep working on the raw form ---

#[subscriber("frames-workers", raw, workers(2), on_failure(panic = drop))]
async fn tolerant(frame: &[u8]) -> HandlerResult {
    assert_ne!(frame, b"boom", "poison frame");
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workers_and_panic_policy_apply_to_raw() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(tolerant));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("frames-workers", b"boom")
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("frames-workers")
        .assert_called_once()
        .panicked();

    // The panic policy dropped the poison frame; the app keeps serving.
    tb.broker::<MemoryBroker>()
        .publish_raw("frames-workers", b"ok")
        .await
        .expect("publish after panic");
    tb.broker::<MemoryBroker>()
        .subscriber("frames-workers")
        .assert_called(2)
        .with_raw(b"ok")
        .settled(HandlerResult::Ack);
}

// --- a Router mounts raw definitions through include_raw ---

static ROUTED: AtomicUsize = AtomicUsize::new(0);

#[subscriber("routed-raw", raw)]
async fn routed(frame: &[u8]) -> HandlerResult {
    ROUTED.fetch_add(frame.len(), Ordering::Relaxed);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_mounts_raw_definitions() {
    let router = Router::<MemoryBroker>::new().include(routed);
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish_raw("routed-raw", FRAME)
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("routed-raw")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    assert_eq!(ROUTED.load(Ordering::Relaxed), FRAME.len());
}

// --- under a scope codec, raw mounts ignore it while typed neighbours decode with it ---

#[cfg(feature = "json")]
mod scope_codec {
    use super::*;

    use ruststream::codec::JsonCodec;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Order {
        id: u32,
    }

    static RAW_BYTES: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
    static TYPED_ID: AtomicUsize = AtomicUsize::new(0);

    #[subscriber("mixed-raw", raw)]
    async fn raw_side(frame: &[u8]) -> HandlerResult {
        RAW_BYTES.lock().expect("raw log").push(frame.to_vec());
        HandlerResult::Ack
    }

    #[subscriber("mixed-typed")]
    async fn typed_side(order: &Order) -> HandlerResult {
        TYPED_ID.store(order.id as usize, Ordering::Relaxed);
        HandlerResult::Ack
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
            .publish_raw("mixed-raw", FRAME)
            .await
            .expect("publish raw");
        tb.broker::<MemoryBroker>()
            .subscriber("mixed-raw")
            .assert_called_once()
            .with_raw(FRAME)
            .settled(HandlerResult::Ack);
        assert_eq!(
            RAW_BYTES.lock().expect("raw log").as_slice(),
            &[FRAME.to_vec()]
        );

        // ...while the typed neighbour still decodes with the scope codec.
        tb.broker::<MemoryBroker>()
            .publish("mixed-typed", &Order { id: 9 })
            .await
            .expect("publish typed");
        tb.broker::<MemoryBroker>()
            .subscriber("mixed-typed")
            .assert_called_once()
            .with(&Order { id: 9 })
            .settled(HandlerResult::Ack);
        assert_eq!(TYPED_ID.load(Ordering::Relaxed), 9);
    }
}

// --- AsyncAPI: a raw subscriber has no input schema, but its channel is still listed ---

#[cfg(feature = "asyncapi")]
mod asyncapi_listing {
    use super::*;

    use ruststream::asyncapi::build_spec;

    /// Consumes raw frames.
    #[subscriber("frames-doc", raw)]
    async fn documented(_frame: &[u8]) {}

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
