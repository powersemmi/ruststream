//! Integration test for `AsyncAPI` document generation.
#![cfg(all(feature = "asyncapi", feature = "memory"))]

use std::future::ready;

use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::prelude::*;
use ruststream::runtime::{HandlerMetadata, OutgoingMessageMetadata};
use ruststream::{SecurityScheme, ServerSpec};

#[test]
fn build_spec_describes_handlers() {
    let info = AppInfo::new("orders-svc", "1.2.3").with_description("Order processing");
    let app = RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
        let orders = b.broker().subscribe("orders");
        b.handle(
            orders,
            |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
            HandlerMetadata::raw("orders").with_description("Handles orders"),
        );
        let alerts = b.broker().subscribe("alerts");
        b.handle(
            alerts,
            |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
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

    // Schema coverage is assertable: the typed u64 model has no JSON Schema captured (a gap),
    // while the raw-bytes message is deliberately schema-free and not reported.
    assert_eq!(spec.messages_without_schema(), vec!["u64"]);

    let json = spec.to_json().unwrap();
    assert!(json.contains("\"asyncapi\": \"3.0.0\""));
    assert!(json.contains("\"receive\""));
    assert!(json.contains("\"$ref\""));
}

/// Shared message components merge across handlers (an absent schema fills in from a later
/// contributor; a conflicting headers schema keeps the first), and send operation ids stay
/// unique when several handlers share one subscription name.
#[test]
fn message_components_merge_and_send_ids_stay_unique() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
        let first = b.broker().subscribe("shared");
        let mut first_meta =
            HandlerMetadata::typed::<u64>("shared").with_headers_schema("{\"title\":\"MetaA\"}");
        first_meta
            .outgoing
            .push(OutgoingMessageMetadata::new("c1", "bytes"));
        b.handle(
            first,
            |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
            first_meta,
        );

        // The second handler on the same subject brings the payload schema the first one
        // lacked, plus a conflicting headers schema, plus its own outgoing channel.
        let second = b.broker().subscribe("shared");
        let mut second_meta = HandlerMetadata::typed::<u64>("shared")
            .with_payload_schema("{\"type\":\"integer\"}")
            .with_headers_schema("{\"title\":\"MetaB\"}");
        second_meta
            .outgoing
            .push(OutgoingMessageMetadata::new("c2", "bytes"));
        b.handle(
            second,
            |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
            second_meta,
        );
    });

    let spec = build_spec(&app);

    // One send operation per (subscription, channel): nothing silently overwritten.
    assert_eq!(spec.operations["send_shared_c1"].action, "send");
    assert_eq!(spec.operations["send_shared_c2"].action, "send");

    // And one receive operation per handler, for the same reason.
    assert_eq!(spec.operations["receive_shared"].action, "receive");
    assert_eq!(spec.operations["receive_shared_2"].action, "receive");

    // The shared component filled in the payload schema from the later contributor (so the
    // coverage gate reports no false gap) and kept the first headers schema on conflict.
    let component = &spec.components.messages["u64"];
    assert!(component.payload.is_some());
    assert_eq!(component.headers.as_ref().unwrap()["title"], "MetaA");
    assert!(spec.messages_without_schema().is_empty());
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

impl Broker for DescribingBroker {
    type Error = std::convert::Infallible;
    type Connected = ConnectedDescribingBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedDescribingBroker))
    }
}

struct ConnectedDescribingBroker;

impl ruststream::ConnectedBroker for ConnectedDescribingBroker {
    type Error = std::convert::Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
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
                |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
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
    async fn handle(order: &Order) -> HandlerOutcome {
        let _ = order;
        HandlerOutcome::ack()
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

/// An order with custom `MessageInfo` metadata: the manual impl overrides both the component name and
/// the description in the generated document.
#[derive(serde::Deserialize)]
struct RenamedOrder {
    #[allow(dead_code)]
    id: u32,
}

impl MessageInfo for RenamedOrder {
    const NAME: &'static str = "CustomOrder";
    const DESCRIPTION: Option<&'static str> = Some("An order, renamed for the wire.");
}

/// Receives renamed orders.
#[ruststream::subscriber("renamed-orders")]
async fn handle_renamed(order: &RenamedOrder) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
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
        .expect("MessageInfo::NAME must name the component");
    assert_eq!(
        message.description.as_deref(),
        Some("An order, renamed for the wire."),
        "MessageInfo::DESCRIPTION must describe the component",
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
async fn handle_shipment(shipment: &Shipment) -> HandlerOutcome {
    let _ = shipment;
    HandlerOutcome::ack()
}

#[test]
fn schema_doc_comment_feeds_message_metadata() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_shipment));

    let spec = build_spec(&app);

    // No MessageInfo impl: the schemars title names the component and the type's own doc comment
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

/// Typed headers and declared outgoing messages: the macro lifts a `Headers` contract into
/// the receive message's headers schema, and `publish(..)` / `#[publishes(..)]` declarations
/// become `send` operations with payload and headers schemas.
#[cfg(all(feature = "macros", feature = "json"))]
mod typed_headers_spec {
    use ruststream::memory::prelude::*;
    use ruststream::schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::build_spec;

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct ChunkMeta {
        task_id: u64,
        chunk_no: u32,
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct DoneMeta {
        task_id: u64,
    }

    #[derive(Deserialize, JsonSchema)]
    struct Chunk {
        #[allow(dead_code)]
        seq: u64,
    }

    #[derive(Outgoing, Serialize, JsonSchema)]
    #[outgoing(name = "chunks.done", headers = DoneMeta)]
    struct ChunkDone {
        output_key: String,
    }

    #[derive(Outgoing, Serialize, JsonSchema)]
    #[outgoing(name = "chunks.progress")]
    struct Progress {
        percent: u8,
    }

    #[derive(Deserialize, JsonSchema)]
    struct Request {
        #[allow(dead_code)]
        id: u64,
    }

    #[derive(MessageInfo, Serialize, JsonSchema)]
    #[message(headers(DoneMeta))]
    struct Response {
        ok: bool,
    }

    #[derive(OutSlot)]
    #[publishes(ChunkDone, Progress)]
    struct Events;

    #[subscriber("chunks.raw")]
    async fn convert(
        _chunk: &Chunk,
        Headers(_meta): Headers<ChunkMeta>,
        Out(_events): Out<impl Publisher, Events, (ChunkDone, Progress)>,
    ) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber("requests", publish("responses"))]
    async fn respond(_req: &Request) -> Response {
        Response { ok: true }
    }

    #[derive(Deserialize, JsonSchema)]
    struct Report {
        #[allow(dead_code)]
        percent: u8,
    }

    // The batch counterpart of the `Headers` contract: the pair input's contract half feeds the
    // receive message's headers schema.
    #[subscriber("chunks.bulk")]
    async fn bulk(_reports: &[Message<ChunkMeta, Report>]) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[test]
    fn receive_headers_schema_and_send_operations() {
        let app = RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(convert).out(Events, Publish).build();
                b.include(respond);
                b.include(bulk.batch(nonzero!(8)));
            },
        );
        let spec = build_spec(&app);

        // The Headers contract lands as the receive message's headers schema.
        let chunk = &spec.components.messages["Chunk"];
        let headers = chunk.headers.as_ref().expect("headers schema");
        assert!(
            headers["properties"].get("task_id").is_some()
                && headers["properties"].get("chunk_no").is_some(),
            "got: {headers}"
        );

        // The batch pair input does the same for the page's element: its contract half is the
        // receive message's headers schema, its payload half the payload schema.
        let report = &spec.components.messages["Report"];
        assert!(report.payload.is_some());
        let batch_headers = report.headers.as_ref().expect("pair headers schema");
        assert!(
            batch_headers["properties"].get("task_id").is_some()
                && batch_headers["properties"].get("chunk_no").is_some(),
            "got: {batch_headers}"
        );

        // The slot's listed types become send operations on the channels they declare.
        assert_eq!(
            spec.operations["send_chunks_raw_chunks_done"].action,
            "send"
        );
        assert_eq!(
            spec.operations["send_chunks_raw_chunks_progress"].action,
            "send"
        );
        assert!(spec.channels.contains_key("chunks.done"));
        assert!(spec.channels.contains_key("chunks.progress"));
        let done = &spec.components.messages["ChunkDone"];
        assert!(done.payload.is_some());
        let done_headers = done.headers.as_ref().expect("declared headers schema");
        assert!(done_headers["properties"].get("task_id").is_some());
        assert!(spec.components.messages["Progress"].headers.is_none());

        // The reply form declares its own send operation; the reply type's contract feeds the
        // headers schema.
        assert_eq!(spec.operations["send_requests_responses"].action, "send");
        assert!(spec.channels.contains_key("responses"));
        let response = &spec.components.messages["Response"];
        assert!(response.headers.is_some());

        // Every model here derives JsonSchema: the coverage gate reports no gaps.
        assert!(spec.messages_without_schema().is_empty());
    }
}

/// Destinations declared on the message type: a fixed name becomes its channel, a templated one
/// keeps its placeholders and declares them as the channel's parameters, and a type declaring
/// nothing contributes no channel at all.
#[cfg(all(feature = "macros", feature = "json"))]
mod declared_destinations {
    use ruststream::memory::prelude::*;
    use ruststream::schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::build_spec;

    #[derive(Deserialize, JsonSchema)]
    struct Order {
        #[allow(dead_code)]
        id: u64,
    }

    /// A confirmed order.
    #[derive(Outgoing, Serialize, JsonSchema)]
    #[outgoing(name = "orders.confirmed")]
    struct OrderConfirmed {
        id: u64,
    }

    /// An order placed into a per-tenant, per-region stream.
    #[derive(Outgoing, Serialize, JsonSchema)]
    #[outgoing(name = "orders.{tenant}.{region}.v1")]
    struct OrderPlaced {
        id: u64,
    }

    /// An order archived wherever the caller says.
    #[derive(Outgoing, Serialize, JsonSchema)]
    struct OrderArchived {
        id: u64,
    }

    #[derive(OutSlot)]
    #[publishes(OrderConfirmed, OrderPlaced, OrderArchived)]
    struct Events;

    #[subscriber("orders.in")]
    async fn route(
        order: &Order,
        Out(_events): Out<impl Publisher, Events, (OrderConfirmed, OrderPlaced, OrderArchived)>,
    ) -> HandlerOutcome {
        let _ = order;
        HandlerOutcome::ack()
    }

    #[test]
    fn a_templated_destination_declares_its_parameters() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(route).out(Events, Publish).build();
            },
        );
        let spec = build_spec(&app);

        // The fixed destination is a channel with no parameters.
        let confirmed = &spec.channels["orders.confirmed"];
        assert_eq!(confirmed.address, "orders.confirmed");
        assert!(confirmed.parameters.is_empty());

        // The templated one keeps its placeholders, and every one of them is declared.
        let placed = &spec.channels["orders.{tenant}.{region}.v1"];
        assert_eq!(placed.address, "orders.{tenant}.{region}.v1");
        assert_eq!(
            placed.parameters.keys().collect::<Vec<_>>(),
            vec!["region", "tenant"],
        );
        assert_eq!(
            spec.operations["send_orders_in_orders__tenant___region__v1"].action,
            "send",
        );

        // A type that declares no destination says nothing about where it goes.
        assert!(
            !spec
                .channels
                .keys()
                .any(|channel| channel.contains("archived")),
            "an undeclared destination must not invent a channel: {:?}",
            spec.channels.keys().collect::<Vec<_>>(),
        );
        assert!(!spec.components.messages.contains_key("OrderArchived"));
    }

    #[test]
    fn a_templated_channel_serializes_its_parameters_block() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(route).out(Events, Publish).build();
            },
        );
        let json = serde_json::to_value(build_spec(&app)).expect("the spec serializes");
        let channel = &json["channels"]["orders.{tenant}.{region}.v1"];
        assert!(
            channel["parameters"]["tenant"].is_object(),
            "got: {channel}"
        );
        // A fixed channel omits the block rather than carrying an empty one.
        assert!(json["channels"]["orders.confirmed"]["parameters"].is_null());
    }
}

/// Two handlers on one channel: each opens its own subscription, so each is its own receive
/// operation rather than the second overwriting the first.
#[derive(serde::Deserialize)]
struct Audited {
    #[allow(dead_code)]
    id: u32,
}

/// Audits every order.
#[ruststream::subscriber("orders.shared")]
async fn audit_shared(order: &Audited) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Bills every order.
#[ruststream::subscriber("orders.shared")]
async fn bill_shared(order: &Audited) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[test]
fn every_handler_on_a_shared_channel_gets_its_own_receive_operation() {
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(audit_shared);
        b.include(bill_shared);
    });

    let spec = build_spec(&app);

    assert_eq!(app.handlers().len(), 2);
    let receives: Vec<&String> = spec
        .operations
        .iter()
        .filter(|(_, operation)| operation.action == "receive")
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        receives,
        vec!["receive_orders_shared", "receive_orders_shared_2"]
    );

    // Both describe the same channel; only the operation id disambiguates them.
    for id in receives {
        assert_eq!(
            spec.operations[id].channel.reference,
            "#/channels/orders.shared"
        );
    }
}
