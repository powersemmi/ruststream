//! The AsyncAPI example written without the `macros` feature: the payload declares itself
//! through the trait impls the derives would emit, and the subscriber is a hand-written
//! definition, so the generated document carries the same schema, name and description.
//!
//! ```text
//! cargo run --example manual_asyncapi_http --no-default-features --features memory,asyncapi
//! ```
//!
//! Then open <http://127.0.0.1:8080/> for the viewer, or fetch the raw document:
//!
//! ```text
//! curl http://127.0.0.1:8080/asyncapi.json
//! ```

use std::future::{Future, ready};

use axum::Router;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, Decoded, Handler, HandlerResult, IncludeDef, RustStream, Settle,
    SubscriberDef, forms,
};
use ruststream::schemars::{JsonSchema, schema_for};
use ruststream::{Message, Name, SecurityScheme, ServerSpec};
use serde::Deserialize;

// --8<-- [start:payload]
/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    item: String,
}

// What `#[derive(Message)]` writes out: the component name and the doc comment as its
// description. `JsonSchema` is schemars' own derive, so the payload schema itself needs no
// macro feature.
impl Message for Order {
    const NAME: &'static str = "Order";
    const DESCRIPTION: Option<&'static str> = Some("An order placed by a customer.");
}

/// The subscriber definition `#[subscriber("orders")]` would generate. The document is built
/// from these hooks: the attribute fills them in by probing the input type, a hand-written
/// definition answers them itself, and `include` reads them either way.
struct Handle;

impl SubscriberDef for Handle {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("orders")
    }

    fn input_schema(&self) -> Option<String> {
        Some(schema_for!(Order).as_value().to_string())
    }

    fn message_name(&self) -> Option<&'static str> {
        Some(Order::NAME)
    }

    fn message_description(&self) -> Option<&'static str> {
        Order::DESCRIPTION
    }

    fn into_handler(self) -> Self {
        self
    }
}

/// The form token tells `include` which mounting machinery this definition needs; a plain
/// subscriber names `forms::Subscribing`.
impl IncludeDef for Handle {
    type Form = forms::Subscribing;
}

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly; `async fn` here would be an
    // unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("order {} ({})", order.id, order.item);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:payload]

// --8<-- [start:server]
fn service() -> RustStream {
    // `with_broker_labeled` records the broker under a label that is both its stable identity and
    // its AsyncAPI server name, deriving the server entry from the broker's own `DescribeServer`
    // spec - here the in-memory broker, which describes itself as an in-process "memory" server
    // with no host. A broker without a `DescribeServer` impl is instead declared explicitly with
    // `.server(name, spec)` alongside a plain `with_broker`.
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // --8<-- [start:security]
        // A described external server. Security is the author's statement, not the broker's:
        // the same broker is deployed publicly and internally with different authentication,
        // so the scheme is attached to the spec at registration and brokers never set it.
        .server(
            "kafka",
            ServerSpec::new("kafka.example.com:9093", "kafka")
                .with_security(SecurityScheme::scram_sha512().with_description("SASL over TLS")),
        )
        // --8<-- [end:security]
        .with_broker_labeled("in-process", MemoryBroker::new(), |b| b.include(Handle))
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
