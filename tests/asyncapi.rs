//! Integration test for `AsyncAPI` document generation.
#![cfg(all(feature = "asyncapi", feature = "memory"))]

use ruststream::asyncapi::build_spec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerMetadata, HandlerResult, RustStream};

#[test]
fn build_spec_describes_handlers() {
    let info = AppInfo::new("orders-svc", "1.2.3").with_description("Order processing");
    let app = RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
        let orders = b.broker().subscribe("orders");
        b.handle(
            orders,
            |_msg: &_| async { HandlerResult::Ack },
            HandlerMetadata::raw("orders").with_description("Handles orders"),
        );
        let alerts = b.broker().subscribe("alerts");
        b.handle(
            alerts,
            |_msg: &_| async { HandlerResult::Ack },
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
