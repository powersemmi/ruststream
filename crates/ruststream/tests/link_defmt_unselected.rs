//! A host build with `defmt` compiled links and runs while the app selects another logger or none:
//! defmt's symbols are referenced only by an app that selects `DefmtLogger`.
// The tokio harness's futures are not `Send`; the test drives them on its own thread.
#![allow(clippy::future_not_send)]
#![cfg(all(
    feature = "defmt",
    feature = "tracing",
    feature = "tokio",
    feature = "memory",
    feature = "macros",
    feature = "testing"
))]

use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// An order, as its bytes.
#[derive(Deserialized)]
struct Order<'a>(&'a [u8]);

/// An order for the app, as its bytes.
#[derive(Outgoing, Serialized)]
struct Placed(Vec<u8>);

/// Acknowledges every order.
#[subscriber("orders")]
async fn handle_orders(order: &Order<'_>) -> HandlerOutcome {
    let _ = order.0;
    HandlerOutcome::ack()
}

/// Delivers one order to an app whose broker and handler are the same in both tests below.
async fn deliver_one(app: impl App) {
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Placed(b"x".to_vec()))
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
}

// The selection is process-wide, so both phases run in order inside one test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn links_with_defmt_compiled_and_another_logger_or_none() {
    let info = AppInfo::new("svc", "0.1.0");
    deliver_one(
        RustStream::new(info.clone())
            .with_broker(MemoryBroker::new(), |b| b.include(handle_orders)),
    )
    .await;
    deliver_one(
        RustStream::new(info)
            .logger(TracingLogger)
            .with_broker(MemoryBroker::new(), |b| b.include(handle_orders)),
    )
    .await;
}
