//! Integration test for `AsyncAPI` document generation.
#![cfg(all(feature = "asyncapi", feature = "memory"))]

use ruststream::ServerSpec;
use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream};

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
    assert_eq!(server.host, "nats.example.com:4222");
    assert_eq!(server.protocol, "nats");
    assert_eq!(server.description.as_deref(), Some("primary"));

    let yaml = spec.to_yaml().unwrap();
    assert!(yaml.contains("asyncapi: 3.0.0"));
    assert!(yaml.contains("host: nats.example.com:4222"));
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
    use ruststream::codec::JsonCodec;
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
        .with_broker(MemoryBroker::new(), |b| b.include(handle, JsonCodec));

    let spec = build_spec(&app);
    let payload = spec.components.messages["Order"]
        .payload
        .as_ref()
        .expect("Order payload schema should be emitted");
    let props = &payload["properties"];
    assert!(props.get("id").is_some());
    assert!(props.get("total").is_some());
}
