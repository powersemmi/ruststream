//! `FromContext` extractor parameters resolved by the `#[subscriber]` macro: a value pulled from
//! shared state and handed to the handler as an argument, and an extractor whose rejection
//! short-circuits the delivery before the body runs.

use std::convert::Infallible;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, FromContext, HandlerOutcome, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{FromRef, Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

// --- happy path: an extractor that clones a handle out of the shared state ---

struct AppState {
    hits: Arc<AtomicU32>,
}

/// Pulls the hit counter out of the state. Generic over the context type, bound to `AppState`.
struct Hits(Arc<AtomicU32>);

impl<C: Send> FromContext<C, AppState> for Hits {
    type Rejection = HandlerOutcome;
    fn from_context(
        ctx: &mut Context<'_, C, AppState>,
    ) -> impl Future<Output = Result<Self, HandlerOutcome>> {
        ready(Ok(Self(ctx.state().hits.clone())))
    }
}

#[subscriber("orders")]
async fn record(_order: &Order, hits: Hits) -> HandlerOutcome {
    hits.0.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extractor_resolves_from_state() {
    let hits = Arc::new(AtomicU32::new(0));
    let state_hits = hits.clone();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, Infallible>(AppState { hits: state_hits }) })
        .with_broker(MemoryBroker::new(), |b| b.include(record));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());
    assert_eq!(
        hits.load(Ordering::Relaxed),
        1,
        "extractor handed the counter to the handler"
    );
}

// --- rejection path: a failing extractor skips the body and settles by its HandlerOutcome ---

struct GuardState {
    ran: Arc<AtomicBool>,
}

/// Always rejects, settling the delivery with a drop before the handler body runs.
struct Deny;

impl<C: Send> FromContext<C, GuardState> for Deny {
    type Rejection = HandlerOutcome;
    fn from_context(
        _ctx: &mut Context<'_, C, GuardState>,
    ) -> impl Future<Output = Result<Self, HandlerOutcome>> {
        ready(Err(HandlerOutcome::drop()))
    }
}

#[subscriber("guard")]
async fn guarded(
    _order: &Order,
    ctx: &mut Context<'_, (), GuardState>,
    _deny: Deny,
) -> HandlerOutcome {
    ctx.state().ran.store(true, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extractor_rejection_short_circuits() {
    let ran = Arc::new(AtomicBool::new(false));
    let state_ran = ran.clone();
    let app = RustStream::new(AppInfo::new("guard", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, Infallible>(GuardState { ran: state_ran }) })
        .with_broker(MemoryBroker::new(), |b| b.include(guarded));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .to("guard")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("guard")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::drop());
    assert!(
        !ran.load(Ordering::Relaxed),
        "the handler body must not run when an extractor rejects"
    );
}

// --- derive: FromRef makes each state field injectable via State<T>, no hand-written extractor ---

#[derive(Clone)]
struct Tally(Arc<AtomicU32>);

#[derive(FromRef)]
struct DerivedState {
    tally: Tally,
    #[from_ref(skip)]
    _label: &'static str,
}

#[subscriber("derived")]
async fn derived(_order: &Order, State(tally): State<Tally>) -> HandlerOutcome {
    tally.0.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derive_from_ref_injects_fields() {
    let count = Arc::new(AtomicU32::new(0));
    let tally = Tally(count.clone());
    let app = RustStream::new(AppInfo::new("derived", "0.1.0"))
        .on_startup(move |()| async move {
            Ok::<_, Infallible>(DerivedState {
                tally,
                _label: "svc",
            })
        })
        .with_broker(MemoryBroker::new(), |b| b.include(derived));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 3 })
        .to("derived")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("derived")
        .assert_called_once()
        .with(&Order { id: 3 })
        .settled(HandlerOutcome::ack());
    assert_eq!(
        count.load(Ordering::Relaxed),
        1,
        "derived extractor handed the field to the handler"
    );
}
