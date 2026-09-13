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
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use ruststream::{Contact, License, SecurityScheme, ServerSpec, Tag};
use serde::{Deserialize, Serialize};

// --8<-- [start:payload]
/// An order placed by a customer.
#[derive(Debug, Deserialize, MessageInfo, JsonSchema)]
struct Order {
    id: u64,
    item: String,
}
// --8<-- [end:payload]

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("order {} ({})", order.id, order.item);
    HandlerOutcome::ack()
}

// --8<-- [start:reply]
/// The confirmation an order gets back.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
struct Confirmed {
    id: u64,
}

/// Answers every request on `requests` with a confirmation on `responses`.
#[subscriber("requests", publish("responses"))]
async fn confirm(order: &Order) -> Confirmed {
    Confirmed { id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:server]
fn service() -> RustStream {
    // `with_broker_labeled` records the broker under a label that is both its stable identity and
    // its AsyncAPI server name, deriving the server entry from the broker's own `DescribeServer`
    // spec - here the in-memory broker, which describes itself as an in-process "memory" server
    // with no host. A broker without a `DescribeServer` impl is instead declared explicitly with
    // `.server(name, spec)` alongside a plain `with_broker`.
    // --8<-- [start:describe]
    let info = AppInfo::new("orders", "0.1.0")
        .with_description("Everything the order domain publishes")
        .with_contact(
            Contact::new()
                .with_name("Payments team")
                .with_email("payments@example.com"),
        )
        .with_license(License::new("Apache-2.0"))
        .with_tag(Tag::new("payments"));
    // --8<-- [end:describe]

    RustStream::new(info)
        // --8<-- [start:security]
        // A described external server. Security is the author's statement, not the broker's:
        // the same broker is deployed publicly and internally with different authentication,
        // so the scheme is attached to the spec at registration and brokers never set it.
        .server(
            "kafka",
            ServerSpec::new("kafka.example.com:9093", "kafka")
                // The wire protocol clients have to speak, where the protocol name alone does
                // not say it: AMQP 0.9.1 and AMQP 1.0 share the name `amqp` and share nothing else.
                .with_protocol_version("3.9")
                .with_security(SecurityScheme::scram_sha512().with_description("SASL over TLS")),
        )
        // --8<-- [end:security]
        .with_broker_labeled("in-process", MemoryBroker::new(), |b| {
            b.include(handle);
            b.include(confirm);
        })
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
