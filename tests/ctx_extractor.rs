//! The `Ctx<K>` extractor: a broker context field injected as a handler parameter, with the
//! subscription's context type projected from the key - no `&mut Context` parameter needed.
//! Also the mixed form (an explicit ctx parameter plus a `Ctx` extractor reading the same
//! context) and the state-composition form (`Ctx` next to `State`).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{AppInfo, Ctx, HandlerResult, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField, Field, FromRef, IncomingMessage, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

/// A broker-style per-delivery context, built from the message: it carries the payload length,
/// standing in for an offset / partition / delivery tag a real broker would expose.
struct DeliveryMeta {
    payload_len: usize,
}

impl BuildContext<MemoryMessage> for DeliveryMeta {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            payload_len: msg.payload().len(),
        }
    }
}

/// The zero-sized key reading the payload length; `ContextField` names its context, `Field`
/// keeps the `ctx.context(KEY)` read working for the mixed test.
#[derive(Clone, Copy, Default)]
struct PayloadLen;

impl ContextField for PayloadLen {
    type Context = DeliveryMeta;
    type Value = usize;
    fn read(self, src: &DeliveryMeta) -> usize {
        src.payload_len
    }
}

impl Field<DeliveryMeta> for PayloadLen {
    type Value<'a> = usize;
    fn get(self, src: &DeliveryMeta) -> usize {
        src.payload_len
    }
}

// --- the pure DI form: no ctx parameter, the key projects the context type ---

static SEEN_LEN: AtomicUsize = AtomicUsize::new(0);

#[subscriber("orders")]
async fn measure(_order: &Order, Ctx(len): Ctx<PayloadLen>) -> HandlerResult {
    SEEN_LEN.store(len, Ordering::Relaxed);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractor_projects_the_context_from_its_key() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(measure));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 7 })
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerResult::Ack);
    let expected = serde_json::to_vec(&Order { id: 7 }).expect("encode").len();
    assert_eq!(
        SEEN_LEN.load(Ordering::Relaxed),
        expected,
        "the extracted field must come from this delivery's context",
    );
}

// --- the mixed form: an explicit ctx parameter, plus the extractor reading the same C ---

#[subscriber("mixed")]
async fn both(
    _order: &Order,
    ctx: &mut Context<'_, DeliveryMeta>,
    Ctx(len): Ctx<PayloadLen>,
) -> HandlerResult {
    assert_eq!(
        ctx.context(PayloadLen),
        len,
        "the extractor and the key read must agree",
    );
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractor_composes_with_an_explicit_ctx_parameter() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(both));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish("mixed", &Order { id: 1 })
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("mixed")
        .assert_called_once()
        .settled(HandlerResult::Ack);
}

// --- composition with State: broker field and state component side by side ---

#[derive(Clone)]
struct Hits(Arc<AtomicU32>);

#[derive(FromRef)]
struct AppState {
    hits: Hits,
}

#[subscriber("counted")]
async fn count(
    _order: &Order,
    Ctx(len): Ctx<PayloadLen>,
    State(hits): State<Hits>,
) -> HandlerResult {
    if len > 0 {
        hits.0.fetch_add(1, Ordering::Relaxed);
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_extractor_composes_with_state() {
    let hits = Arc::new(AtomicU32::new(0));
    let state_hits = Hits(hits.clone());
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, Infallible>(AppState { hits: state_hits }) })
        .with_broker(MemoryBroker::new(), |b| b.include(count));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .publish("counted", &Order { id: 2 })
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("counted")
        .assert_called_once()
        .settled(HandlerResult::Ack);
    assert_eq!(hits.load(Ordering::Relaxed), 1);
}
