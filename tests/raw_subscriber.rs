//! Integration tests for the raw `#[subscriber(.., raw)]` form: the handler receives each
//! delivery's payload bytes untouched, with no codec anywhere on the path.
//!
//! The codec-free path itself is additionally pinned by a feature-stripped compile:
//! `cargo check --no-default-features --features macros,memory,testing --test raw_subscriber`
//! builds this file with every codec-gated test compiled out.
#![cfg(all(feature = "macros", feature = "memory", feature = "testing"))]

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{AppInfo, Ctx, HandlerResult, Router, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField, FromRef, IncomingMessage, subscriber};

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
    let router = Router::<MemoryBroker>::new().include_raw(routed);
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
