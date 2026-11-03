//! The `AsyncAPI` generator reports through the logger the app selects and through nothing else:
//! a message type without a JSON Schema is silent with no logger and is a warning once
//! `TracingLogger` is selected.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.
// The tokio harness's futures are not `Send`; the test drives them on its own thread.
#![allow(clippy::future_not_send)]
#![cfg(all(
    feature = "asyncapi",
    feature = "macros",
    feature = "memory",
    feature = "testing",
    feature = "tokio"
))]

use ruststream::asyncapi::build_spec;
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing::subscriber::set_default;

mod logger_capture;
use logger_capture::Traced;

/// A message model that derives no JSON Schema.
#[derive(Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn accept(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The service, selecting `TracingLogger` when `logger` is set.
fn service(logger: bool) -> impl App {
    let setup = RustStream::new(AppInfo::new("orders", "0.1.0"));
    let setup = if logger {
        setup.logger(TracingLogger)
    } else {
        setup
    };
    setup.with_broker(MemoryBroker::new(), |b| b.include(accept))
}

/// Starts the service, which selects its logger, then generates the document of a second copy
/// (the generator runs as the command line runs it) and returns the schema gaps it reports.
async fn document(logger: bool) -> Vec<String> {
    let tb = TestApp::start(service(logger)).await.expect("start");
    let spec = build_spec(&service(logger));
    tb.shutdown().await.expect("shutdown");
    spec.messages_without_schema()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn the_generator_reports_only_through_the_logger_the_app_selected() {
    let traced = Traced::at(Level::DEBUG);

    // No logger selected: the gap is in the document and nowhere else.
    {
        let _scope = set_default(traced.clone());
        assert_eq!(document(false).await, ["Order".to_owned()]);
    }
    assert!(traced.lines().is_empty(), "{:?}", traced.lines());

    // `TracingLogger`: the warning reaches the subscriber with the subscription and the type.
    {
        let _scope = set_default(traced.clone());
        document(true).await;
    }
    let lines: Vec<String> = traced
        .lines()
        .into_iter()
        .filter(|line| line.starts_with("WARN"))
        .collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("WARN ruststream::asyncapi:"),
        "{lines:?}"
    );
    assert!(lines[0].contains("subscription=orders"), "{lines:?}");
    assert!(lines[0].contains("asyncapi_logger::Order"), "{lines:?}");
}
