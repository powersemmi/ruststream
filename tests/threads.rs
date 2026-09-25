//! `threads(n)` under the test harness, and the handle to the app's runtime a handler reaches
//! through its context.
//!
//! `TestApp` runs every subscription on the test's own runtime, a `threads(n)` one included, so
//! these tests assert what a handler sees and how its deliveries settle, never where they ran.
//! Placement itself is the subject of `threads_placement.rs`.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::Order;
use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField};
use tokio::runtime::Handle;

/// Acks where the context's main-runtime handle is the runtime the handler runs on: under the
/// harness that is the test's own.
#[subscriber("where")]
async fn on_main(_order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    if ctx.main_runtime().id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_context_hands_out_the_app_runtime() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_main);
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("where")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("where")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
}

/// The same through the extractor, with no context parameter.
#[subscriber("extracted")]
async fn extracted(_order: &Order, Ctx(main): Ctx<MainRuntime>) -> HandlerOutcome {
    if main.id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// A broker-style context with one field, so a handler can take a broker key beside the
/// main-runtime one.
struct Meta {
    len: usize,
}

impl BuildContext<MemoryMessage> for Meta {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            len: msg.payload().len(),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PayloadLen;

impl ContextField for PayloadLen {
    type Context = Meta;
    type Value = usize;
    fn read(self, src: &Meta) -> usize {
        src.len
    }
}

/// The main-runtime key first: the subscription's context still comes from the broker key.
#[subscriber("mixed")]
async fn mixed(
    _order: &Order,
    Ctx(main): Ctx<MainRuntime>,
    Ctx(len): Ctx<PayloadLen>,
) -> HandlerOutcome {
    if main.id() == Handle::current().id() && len > 0 {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_extractor_hands_out_the_app_runtime_beside_broker_keys() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(extracted);
            b.include(mixed);
        });
    let tb = TestApp::start(app).await.expect("startup");
    for subject in ["extracted", "mixed"] {
        tb.message(&Order { id: 1 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber(subject)
            .assert_called(1)
            .settled(HandlerOutcome::ack());
    }
}
