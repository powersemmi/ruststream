//! The `AsyncAPI` generator reports through the logger a compio app selects, as it does for a tokio
//! app: a message type without a JSON Schema is silent with no logger and is a warning once
//! `TracingLogger` is selected, without the app having started.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.
#![cfg(all(
    feature = "asyncapi",
    feature = "compio",
    feature = "macros",
    feature = "memory"
))]

use ruststream::asyncapi::build_spec;
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::compio::{App, AppInfo, RustStream};
use ruststream::subscriber;
use serde::Deserialize;
use tracing::Level;
use tracing::subscriber::set_default;

mod logger_capture;

use logger_capture::Traced;

/// A message model that derives no JSON Schema.
#[derive(Deserialize)]
struct Order {
    #[allow(
        dead_code,
        reason = "the document is built from the type, nothing reads it"
    )]
    id: u64,
}

#[subscriber("orders")]
async fn accept(_order: &Order) {}

/// The compio service, selecting `TracingLogger` when `logger` is set.
fn service(logger: bool) -> impl App {
    let setup = RustStream::new(AppInfo::new("orders", "0.1.0"));
    let setup = if logger {
        setup.logger(TracingLogger)
    } else {
        setup
    };
    setup.with_broker(MemoryBroker::new(), |b| b.include(accept))
}

#[test]
fn the_generator_reports_through_the_logger_the_compio_app_selected() {
    let traced = Traced::at(Level::DEBUG);
    // No logger selected: the gap is in the document and nowhere else.
    {
        let _scope = set_default(traced.clone());
        let spec = build_spec(&service(false));
        assert_eq!(spec.messages_without_schema(), ["Order"]);
    }
    assert!(traced.lines().is_empty(), "{:?}", traced.lines());

    // `TracingLogger`: the warning reaches the subscriber with the subscription and the type.
    {
        let _scope = set_default(traced.clone());
        let _ = build_spec(&service(true));
    }
    let lines: Vec<String> = traced
        .lines()
        .into_iter()
        .filter(|line| line.starts_with("WARN"))
        .collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("subscription=orders"), "{lines:?}");
    assert!(
        lines[0].contains("asyncapi_logger_compio::Order"),
        "{lines:?}"
    );
}
