//! Integration test for `AsyncAPI` document generation.
#![cfg(all(feature = "asyncapi", feature = "memory"))]

use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream};
use ruststream::{SecurityScheme, ServerSpec};

#[test]
fn build_spec_describes_handlers() {
    let info = AppInfo::new("orders-svc", "1.2.3").with_description("Order processing");
    let app = RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
        let orders = b.broker().subscribe("orders");
        b.handle(
            orders,
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::raw("orders").with_description("Handles orders"),
        );
        let alerts = b.broker().subscribe("alerts");
        b.handle(
            alerts,
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::typed::<u64>("alerts"),
        );
    });

    let spec = build_spec(&app);

    assert_eq!(spec.asyncapi, "3.0.0");
    assert_eq!(spec.info.title, "orders-svc");
    assert_eq!(spec.info.version, "1.2.3");
    assert_eq!(spec.info.description.as_deref(), Some("Order processing"));

    assert_eq!(spec.channels["orders"].address, "orders");
    assert!(spec.channels.contains_key("alerts"));
    assert!(spec.operations.contains_key("receive_orders"));
    assert!(spec.operations.contains_key("receive_alerts"));
    assert_eq!(spec.operations["receive_orders"].action, "receive");

    assert!(spec.components.messages.contains_key("bytes"));
    assert!(spec.components.messages.contains_key("u64"));
    assert_eq!(
        spec.components.messages["bytes"].description.as_deref(),
        Some("Handles orders"),
    );

    let json = spec.to_json().unwrap();
    assert!(json.contains("\"asyncapi\": \"3.0.0\""));
    assert!(json.contains("\"receive\""));
    assert!(json.contains("\"$ref\""));
}

#[test]
fn build_spec_includes_servers_and_yaml() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).server(
        "nats",
        ServerSpec::new("nats.example.com:4222", "nats").with_description("primary"),
    );

    let spec = build_spec(&app);
    let server = &spec.servers["nats"];
    assert_eq!(server.host.as_deref(), Some("nats.example.com:4222"));
    assert_eq!(server.protocol, "nats");
    assert_eq!(server.description.as_deref(), Some("primary"));

    let yaml = spec.to_yaml().unwrap();
    assert!(yaml.contains("asyncapi: 3.0.0"));
    assert!(yaml.contains("host: nats.example.com:4222"));
}

/// A self-describing broker: a labeled registration derives its `AsyncAPI` server from this.
struct DescribingBroker {
    host: String,
}

impl DescribingBroker {
    fn new(host: impl Into<String>) -> Self {
        Self { host: host.into() }
    }
}

impl ruststream::Broker for DescribingBroker {
    type Error = std::convert::Infallible;
    type Connected = ConnectedDescribingBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedDescribingBroker)
    }
}

struct ConnectedDescribingBroker;

impl ruststream::ConnectedBroker for ConnectedDescribingBroker {
    type Error = std::convert::Infallible;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ruststream::DescribeServer for DescribingBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.host.clone(), "nats").with_description("ingress")
    }
}

#[test]
fn labeled_broker_populates_server_from_describe() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker_labeled(
        "ingress",
        DescribingBroker::new("nats.example.com:4222"),
        |_b| {},
    );

    let spec = build_spec(&app);
    let server = &spec.servers["ingress"];
    assert_eq!(server.host.as_deref(), Some("nats.example.com:4222"));
    assert_eq!(server.protocol, "nats");
    assert_eq!(server.description.as_deref(), Some("ingress"));
}

#[test]
fn labeled_memory_broker_is_an_in_process_server() {
    // The in-memory broker has no network address: a labeled registration still gives it a server
    // entry (its label) over the "memory" protocol, with no host. This is what lets a service mount
    // several memory brokers with disjoint routing and address each by name.
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker_labeled(
        "local",
        MemoryBroker::new(),
        |b| {
            let orders = b.broker().subscribe("orders");
            b.handle(
                orders,
                |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
                HandlerMetadata::raw("orders"),
            );
        },
    );

    let spec = build_spec(&app);
    let server = &spec.servers["local"];
    assert_eq!(server.host, None);
    assert_eq!(server.protocol, "memory");

    // A server with no host must not emit a `host` key in the document.
    let json = spec.to_json().unwrap();
    assert!(!json.contains("\"host\""));
}

#[test]
fn explicit_server_overrides_labeled_broker() {
    // An explicit server set for the same label takes precedence over the broker's own spec.
    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .server("ingress", ServerSpec::new("override:4222", "custom"))
        .with_broker_labeled(
            "ingress",
            DescribingBroker::new("nats.example.com:4222"),
            |_b| {},
        );

    let spec = build_spec(&app);
    let server = &spec.servers["ingress"];
    assert_eq!(server.host.as_deref(), Some("override:4222"));
    assert_eq!(server.protocol, "custom");
}

#[test]
fn viewer_html_embeds_spec_url_and_cdn() {
    let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
    assert!(html.contains("/asyncapi.json"));
    assert!(html.contains("cdn.jsdelivr.net"));
    assert!(html.contains("AsyncApiStandalone.render"));

    let pinned = render_viewer_html(
        "/spec",
        &ViewerOptions::default()
            .with_title("My API")
            .with_cdn_base("https://example.test/assets/"),
    );
    assert!(pinned.contains("<title>My API</title>"));
    assert!(pinned.contains("https://example.test/assets/browser/standalone/index.js"));
}

#[cfg(feature = "macros")]
#[test]
fn build_spec_emits_payload_schema() {
    use ruststream::schemars::JsonSchema;
    use ruststream::subscriber;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct Order {
        id: u32,
        total: f64,
    }

    /// Handles an order.
    #[subscriber("orders")]
    async fn handle(order: &Order) -> HandlerResult {
        let _ = order;
        HandlerResult::Ack
    }

    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle));

    let spec = build_spec(&app);
    let payload = spec.components.messages["Order"]
        .payload
        .as_ref()
        .expect("Order payload schema should be emitted");
    let props = &payload["properties"];
    assert!(props.get("id").is_some());
    assert!(props.get("total").is_some());
}

/// An order with custom `Message` metadata: the manual impl overrides both the component name and
/// the description in the generated document.
#[derive(serde::Deserialize)]
struct RenamedOrder {
    #[allow(dead_code)]
    id: u32,
}

impl ruststream::Message for RenamedOrder {
    const NAME: &'static str = "CustomOrder";
    const DESCRIPTION: Option<&'static str> = Some("An order, renamed for the wire.");
}

/// Receives renamed orders.
#[ruststream::subscriber("renamed-orders")]
async fn handle_renamed(order: &RenamedOrder) -> HandlerResult {
    let _ = order;
    HandlerResult::Ack
}

#[test]
fn message_impl_names_and_describes_the_component() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_renamed));

    let spec = build_spec(&app);

    let message = spec
        .components
        .messages
        .get("CustomOrder")
        .expect("Message::NAME must name the component");
    assert_eq!(
        message.description.as_deref(),
        Some("An order, renamed for the wire."),
        "Message::DESCRIPTION must describe the component",
    );

    let operation = spec
        .operations
        .get("receive_renamed_orders")
        .expect("operation must exist");
    assert_eq!(
        operation.description.as_deref(),
        Some("Receives renamed orders."),
        "the handler doc comment must land on the operation",
    );

    let channel = spec.channels.get("renamed-orders").expect("channel");
    assert!(
        channel.messages.contains_key("CustomOrder"),
        "the channel must reference the renamed component",
    );
}

/// A shipment, documented only by its doc comment.
#[derive(serde::Deserialize, ruststream::schemars::JsonSchema)]
#[schemars(title = "WireShipment")]
struct Shipment {
    #[allow(dead_code)]
    id: u32,
}

/// Receives shipments.
#[ruststream::subscriber("shipments")]
async fn handle_shipment(shipment: &Shipment) -> HandlerResult {
    let _ = shipment;
    HandlerResult::Ack
}

#[test]
fn schema_doc_comment_feeds_message_metadata() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_shipment));

    let spec = build_spec(&app);

    // No Message impl: the schemars title names the component and the type's own doc comment
    // becomes the message description.
    let message = spec
        .components
        .messages
        .get("WireShipment")
        .expect("the schema title must name the component");
    assert_eq!(
        message.description.as_deref(),
        Some("A shipment, documented only by its doc comment."),
        "the type's doc comment must describe the component",
    );
    assert!(
        spec.channels["shipments"]
            .messages
            .contains_key("WireShipment"),
        "the channel must reference the schema-titled component",
    );

    // The handler doc comment stays on the operation.
    assert_eq!(
        spec.operations["receive_shipments"].description.as_deref(),
        Some("Receives shipments."),
    );
}

#[test]
fn server_security_lands_in_components_and_refs() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .server(
            "kafka",
            ServerSpec::new("kafka.example.com:9093", "kafka")
                .with_security(SecurityScheme::scram_sha512().with_description("SASL over TLS"))
                .with_security(SecurityScheme::custom(
                    serde_json::json!({ "type": "gssapi" }),
                )),
        )
        .server("nats", ServerSpec::new("nats.example.com:4222", "nats"));

    let spec = build_spec(&app);

    // Each scheme becomes a components entry named after the server (suffixed past the first),
    // and the server references them in order.
    let kafka = &spec.servers["kafka"];
    assert_eq!(
        kafka.security[0].reference,
        "#/components/securitySchemes/kafka"
    );
    assert_eq!(
        kafka.security[1].reference,
        "#/components/securitySchemes/kafka-1"
    );
    let schemes = &spec.components.security_schemes;
    assert_eq!(schemes["kafka"]["type"], "scramSha512");
    assert_eq!(schemes["kafka"]["description"], "SASL over TLS");
    assert_eq!(schemes["kafka-1"]["type"], "gssapi");

    // A server without schemes emits no `security` key; the untouched default document stays
    // security-free entirely.
    let json = spec.to_json().unwrap();
    assert!(json.contains("\"securitySchemes\""));
    let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(doc["servers"]["nats"].get("security").is_none());

    let bare = build_spec(
        &RustStream::new(AppInfo::new("svc", "1.0.0"))
            .server("nats", ServerSpec::new("nats.example.com:4222", "nats")),
    );
    assert!(!bare.to_json().unwrap().contains("security"));
}
