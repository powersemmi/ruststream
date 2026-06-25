//! Serve a service's AsyncAPI document and an interactive viewer over HTTP with axum.
//!
//! ```text
//! cargo run --example asyncapi_http --features macros,memory,asyncapi
//! ```
//!
//! Then open <http://127.0.0.1:8080/> for the viewer, or fetch the raw document:
//!
//! ```text
//! curl http://127.0.0.1:8080/asyncapi.json
//! ```

use axum::Router;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

// --8<-- [start:payload]
/// An order placed by a customer.
#[derive(Debug, Deserialize, ruststream::Message, ruststream::schemars::JsonSchema)]
struct Order {
    id: u64,
    item: String,
}
// --8<-- [end:payload]

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("order {} ({})", order.id, order.item);
    HandlerResult::Ack
}

// --8<-- [start:server]
fn service() -> RustStream {
    // `MemoryBroker` has no network address, so its server is declared explicitly. A
    // self-describing broker (the sibling NATS / Redis crates implement `DescribeServer`) is
    // instead registered with `with_broker_labeled("production", broker, ...)`, which derives this
    // server entry from the broker itself, with no separate `.server(..)` call.
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .server(
            "production",
            ruststream::ServerSpec::new("nats.example.com:4222", "nats"),
        )
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
// --8<-- [end:server]

// --8<-- [start:generate]
/// Builds the AsyncAPI document and the viewer HTML from the service.
fn document() -> Result<(String, String), serde_json::Error> {
    let spec = build_spec(&service()).to_json()?;
    let viewer = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
    Ok((spec, viewer))
}
// --8<-- [end:generate]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (spec, viewer) = document()?;

    let router = Router::new()
        .route(
            "/",
            get(move || {
                let viewer = viewer.clone();
                async move { Html(viewer) }
            }),
        )
        .route(
            "/asyncapi.json",
            get(move || {
                let spec = spec.clone();
                async move { ([(CONTENT_TYPE, "application/json")], spec).into_response() }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("AsyncAPI viewer on http://127.0.0.1:8080/");
    axum::serve(listener, router).await?;
    Ok(())
}
