//! Integration test for `AsyncAPI` document generation.
#![cfg(all(feature = "asyncapi", feature = "memory"))]

use std::future::ready;

use ruststream::asyncapi::{ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::prelude::*;
use ruststream::runtime::{HandlerMetadata, OutgoingMessageMetadata};
use ruststream::{SecurityScheme, ServerSpec};

#[test]
fn build_spec_describes_handlers() {
    let info = AppInfo::new("orders-svc", "1.2.3").description("Order processing");
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

    assert_eq!(spec.asyncapi, "3.1.0");
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
    assert!(json.contains("\"asyncapi\": \"3.1.0\""));
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
        ServerSpec::new("nats.example.com:4222", "nats").description("primary"),
    );

    let spec = build_spec(&app);
    let server = &spec.servers["nats"];
    assert_eq!(server.host.as_deref(), Some("nats.example.com:4222"));
    assert_eq!(server.protocol, "nats");
    assert_eq!(server.description.as_deref(), Some("primary"));

    let yaml = spec.to_yaml().unwrap();
    assert!(yaml.contains("asyncapi: 3.1.0"));
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
        ServerSpec::new(self.host.clone(), "nats").description("ingress")
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
            .title("My API")
            .cdn_base("https://example.test/assets/"),
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

    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });

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
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle_renamed);
    });

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
    let app = RustStream::new(AppInfo::new("svc", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle_shipment);
    });

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
                .security(SecurityScheme::scram_sha512().description("SASL over TLS"))
                .security(SecurityScheme::custom(
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

    #[derive(Outgoing, Serialize, JsonSchema)]
    #[outgoing(headers = DoneMeta)]
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

        // The batch pair input does the same for the batch's element: its contract half is the
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

        // The reply answers the receive operation rather than standing as an operation of its
        // own; the reply type's contract feeds the headers schema.
        let reply = spec.operations["receive_requests"]
            .reply
            .as_ref()
            .expect("the request-reply registration reports what answers it");
        assert_eq!(reply.channel.reference, "#/channels/responses");
        assert!(!spec.operations.contains_key("send_requests_responses"));
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

/// A registration that names a dead-letter destination publishes to it, so the generated document
/// reports it as a channel of the service rather than leaving it to a runbook.
#[cfg(all(feature = "macros", feature = "json"))]
mod dead_letter {
    use super::*;
    use ruststream::nonzero;
    use serde::Deserialize;

    #[derive(Deserialize, schemars::JsonSchema)]
    struct Order {
        id: u64,
    }

    #[subscriber("orders")]
    async fn reconcile(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::retry()
    }

    #[test]
    fn a_declared_dead_letter_destination_is_a_send_operation() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(reconcile)
                    .max_attempts(nonzero!(5u32))
                    .dead_letter("orders.dead");
            },
        );
        let spec = build_spec(&app);

        assert_eq!(spec.channels["orders.dead"].address, "orders.dead");
        assert_eq!(spec.operations["send_orders_orders_dead"].action, "send");
    }

    /// A registration that names none says nothing: the document reports what the service
    /// declared, not every destination it might reach.
    #[test]
    fn a_cap_without_a_destination_adds_no_channel() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(reconcile).max_attempts(nonzero!(5u32));
            },
        );
        let spec = build_spec(&app);

        assert!(!spec.channels.contains_key("orders.dead"));
    }
}

/// The 3.1 surface the core fills from what a mounted registration already knows: what answers a
/// request, which server a channel lives on, the media type of a payload, and what the
/// registration declared about giving up.
#[cfg(all(feature = "macros", feature = "json"))]
mod document_surface {
    use ruststream::memory::prelude::*;
    use ruststream::runtime::HandlerMetadata;
    use ruststream::schemars::JsonSchema;
    use ruststream::{AppId, Contact, ExternalDocs, License, ServerSpec, Tag, nonzero};
    use serde::{Deserialize, Serialize};

    use super::{Context, HandlerOutcome, build_spec};

    /// An order to confirm.
    #[derive(Deserialize, JsonSchema)]
    #[schemars(title = "A placed order")]
    struct Order {
        #[allow(dead_code)]
        id: u64,
    }

    /// The confirmation.
    #[derive(Serialize, JsonSchema, Outgoing)]
    struct Confirmed {
        id: u64,
    }

    #[subscriber("orders", publish("orders.confirmed"))]
    async fn confirm(order: &Order) -> Confirmed {
        Confirmed { id: order.id }
    }

    #[subscriber("orders")]
    async fn reconcile(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::retry()
    }

    /// A request-reply registration is one operation carrying its reply, not two operations a
    /// reader has to pair up by name.
    #[test]
    fn a_reply_answers_the_operation_it_belongs_to() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let spec = build_spec(&app);

        let reply = spec.operations["receive_orders"]
            .reply
            .as_ref()
            .expect("a publish(..) registration reports what answers it");
        assert_eq!(reply.channel.reference, "#/channels/orders.confirmed");
        assert_eq!(
            reply.messages[0].reference,
            "#/channels/orders.confirmed/messages/Confirmed",
        );

        // The reply is not a second, unrelated operation.
        assert!(
            spec.operations
                .values()
                .all(|operation| operation.action == "receive"),
            "the reply must not stand as a send operation of its own",
        );
        // Its channel is still in the document: the traffic is real.
        assert!(spec.channels.contains_key("orders.confirmed"));
    }

    /// A labeled registration says which server its channels live on, so a multi-broker document
    /// stops showing every channel on every server.
    #[test]
    fn a_labeled_registration_names_the_server_of_its_channels() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
            .with_broker_labeled("local", MemoryBroker::new(), |b| {
                b.include(confirm);
            })
            .with_broker_labeled("audit", MemoryBroker::new(), |b| {
                let audit = b.broker().subscribe("audit");
                b.handle(
                    audit,
                    |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
                    HandlerMetadata::raw("audit"),
                );
            });
        let spec = build_spec(&app);

        assert_eq!(
            spec.channels["orders"].servers[0].reference,
            "#/servers/local",
        );
        assert_eq!(
            spec.channels["orders.confirmed"].servers[0].reference,
            "#/servers/local",
        );
        assert_eq!(
            spec.channels["audit"].servers[0].reference,
            "#/servers/audit",
        );
    }

    /// One server and no label: there is nothing to be ambiguous about, so every channel names
    /// it.
    #[test]
    fn a_single_server_document_names_it_on_every_channel() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
            .server("nats", ServerSpec::new("nats.example.com:4222", "nats"))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(confirm);
            });
        let spec = build_spec(&app);

        assert_eq!(
            spec.channels["orders"].servers[0].reference,
            "#/servers/nats"
        );
        // And the protocol of that server labels the operation.
        assert_eq!(spec.operations["receive_orders"].tags[0].name, "nats");
    }

    /// Several servers and an unlabeled registration: the core cannot say which one, and a guess
    /// would be a claim the service never made.
    #[test]
    fn several_servers_and_no_label_leave_the_channel_silent() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
            .server("nats", ServerSpec::new("nats.example.com:4222", "nats"))
            .server("kafka", ServerSpec::new("kafka.example.com:9092", "kafka"))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(confirm);
            });
        let spec = build_spec(&app);

        assert!(spec.channels["orders"].servers.is_empty());
        assert!(spec.operations["receive_orders"].tags.is_empty());
    }

    /// The codec names the media type of what it decodes, and a document that agrees on one
    /// states it once at the root.
    #[test]
    fn the_codec_names_the_media_type_of_what_it_decodes() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let spec = build_spec(&app);

        assert_eq!(
            spec.components.messages["A placed order"]
                .content_type
                .as_deref(),
            Some("application/json"),
        );
        assert_eq!(
            spec.default_content_type.as_deref(),
            Some("application/json")
        );
    }

    /// Two codecs in one service: the root default would misdescribe half the document, so it is
    /// left out and each message states its own.
    #[cfg(feature = "cbor")]
    #[test]
    fn two_codecs_leave_the_document_without_a_default_media_type() {
        use ruststream::codec::CborCodec;

        /// An audited event.
        #[derive(Deserialize, JsonSchema)]
        struct Audited {
            #[allow(dead_code)]
            id: u64,
        }

        #[subscriber("audit")]
        async fn audit(event: &Audited) -> HandlerOutcome {
            let _ = event.id;
            HandlerOutcome::ack()
        }

        let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(confirm);
            })
            .with_broker_codec(MemoryBroker::new(), CborCodec, |b| {
                b.include(audit);
            });
        let spec = build_spec(&app);

        assert_eq!(
            spec.components.messages["A placed order"]
                .content_type
                .as_deref(),
            Some("application/json"),
        );
        assert_eq!(
            spec.components.messages["Audited"].content_type.as_deref(),
            Some("application/cbor"),
        );
        assert_eq!(spec.default_content_type, None);
    }

    /// The attempt cap and the dead-letter destination ride the operation they belong to, so a
    /// reader tells a dead-letter channel from a business destination.
    #[test]
    fn a_declared_retry_rides_the_receive_operation() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(reconcile)
                    .max_attempts(nonzero!(5u32))
                    .dead_letter("orders.dead");
            },
        );
        let spec = build_spec(&app);

        let retry = spec.operations["receive_orders"]
            .retry
            .as_ref()
            .expect("a declared cap reaches the document");
        assert_eq!(retry.max_attempts, Some(5));
        assert_eq!(retry.dead_letter.as_deref(), Some("orders.dead"));

        let json = spec.to_json().unwrap();
        assert!(json.contains("\"x-ruststream-retry\""));
    }

    /// A registration that declared nothing carries no extension: the document reports what the
    /// service said, not what it might have said.
    #[test]
    fn an_undeclared_retry_adds_no_extension() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(reconcile);
            },
        );
        let spec = build_spec(&app);

        assert!(spec.operations["receive_orders"].retry.is_none());
    }

    /// The `AsyncAPI` Schema Object extends Draft 07, so that is the draft the payloads are
    /// generated in: a document that says one draft and carries another is wrong for every tool
    /// that reads it.
    #[test]
    fn payload_schemas_are_draft_07() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let spec = build_spec(&app);

        let payload = spec.components.messages["A placed order"]
            .payload
            .as_ref()
            .expect("payload schema");
        assert_eq!(
            payload["$schema"].as_str(),
            Some("http://json-schema.org/draft-07/schema#"),
        );
    }

    /// One protocol name can cover incompatible versions, and the broker is what knows which one
    /// its clients speak.
    #[test]
    fn a_server_reports_the_version_of_its_protocol() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).server(
            "rabbit",
            ServerSpec::new("rabbit.example.com:5672", "amqp").protocol_version("0.9.1"),
        );
        let spec = build_spec(&app);

        assert_eq!(
            spec.servers["rabbit"].protocol_version.as_deref(),
            Some("0.9.1"),
        );
        assert!(spec.to_json().unwrap().contains("\"protocolVersion\""));
    }

    /// The legal and editorial surround of a service: who owns it, under what licence, where its
    /// prose lives, and what identifies it.
    #[test]
    fn the_service_describes_its_owner_and_its_licence() {
        let info = AppInfo::new("orders", "1.0.0")
            .id("urn:example:orders".parse().unwrap())
            .terms_of_service("https://example.com/tos")
            .contact(Contact::new().email("payments@example.com"))
            .license(License::new("Apache-2.0"))
            .tag(Tag::new("payments"))
            .external_docs(ExternalDocs::new("https://example.com/orders"));
        let app = RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
            b.include(confirm);
        });
        let spec = build_spec(&app);

        assert_eq!(
            spec.id.as_ref().map(AppId::as_str),
            Some("urn:example:orders")
        );
        assert_eq!(
            spec.info.contact.as_ref().and_then(|c| c.email.as_deref()),
            Some("payments@example.com"),
        );
        assert_eq!(
            spec.info.license.as_ref().map(|l| l.name.as_str()),
            Some("Apache-2.0"),
        );
        assert_eq!(spec.info.tags[0].name, "payments");
        assert!(spec.info.external_docs.is_some());
        assert_eq!(
            spec.info.terms_of_service.as_deref(),
            Some("https://example.com/tos"),
        );

        let json = spec.to_json().unwrap();
        assert!(json.contains("\"termsOfService\""));
        assert!(json.contains("\"externalDocs\""));
    }

    /// A service that filled nothing in carries no empty objects: an absent contact is absent,
    /// not an object with no fields.
    #[test]
    fn an_undescribed_service_carries_no_empty_objects() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let spec = build_spec(&app);

        assert!(spec.info.contact.is_none());
        assert!(spec.id.is_none());
        let json = spec.to_json().unwrap();
        assert!(!json.contains("\"contact\""));
        // The payload schema has a field called `id`, so the check is the root key, not the word.
        assert!(!json.contains("\"asyncapi\": \"3.1.0\",\n  \"id\""));
    }

    /// The schema's title is a human name for the payload; it reaches the message when it says
    /// something the machine name does not.
    #[test]
    fn a_schema_title_becomes_the_message_title() {
        /// A shipment, named one way on the wire and another for a reader.
        #[derive(Deserialize, JsonSchema)]
        #[schemars(title = "A dispatched shipment")]
        struct Shipment {
            #[allow(dead_code)]
            id: u64,
        }

        impl MessageInfo for Shipment {
            const NAME: &'static str = "ShipmentV2";
        }

        #[subscriber("shipments")]
        async fn dispatch(shipment: &Shipment) -> HandlerOutcome {
            let _ = shipment.id;
            HandlerOutcome::ack()
        }

        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
                b.include(dispatch);
            },
        );
        let spec = build_spec(&app);

        assert_eq!(
            spec.components.messages["ShipmentV2"].title.as_deref(),
            Some("A dispatched shipment"),
        );
        // The order's own title names the component, and repeating it would tell the reader
        // nothing.
        assert_eq!(spec.components.messages["A placed order"].title, None);
        // The reply type's title is its type name, so it does not repeat either.
        assert_eq!(spec.components.messages["Confirmed"].title, None);
    }
}

/// Protocol bindings: what a broker's descriptor and its server spec add to the document, in the
/// broker's own vocabulary, without the core naming a single field of it.
#[cfg(all(feature = "macros", feature = "json"))]
mod protocol_bindings {
    use std::future::Future;

    use ruststream::asyncapi::{Binding, Bindings};
    use ruststream::memory::prelude::*;
    use ruststream::schemars::JsonSchema;
    use ruststream::{
        AddressedCopies, RedeliveryAddress, RedeliveryAddressed, ServerSpec, Subscribe,
        SubscriptionSource,
    };
    use serde::{Deserialize, Serialize};

    use super::build_spec;

    /// An order to confirm.
    #[derive(Deserialize, JsonSchema)]
    struct Order {
        #[allow(dead_code)]
        id: u64,
    }

    #[derive(Serialize)]
    struct AmqpQueue {
        name: &'static str,
        durable: bool,
    }

    #[derive(Serialize)]
    struct AmqpChannel {
        is: &'static str,
        queue: AmqpQueue,
    }

    #[derive(Serialize)]
    struct AmqpOperation {
        ack: bool,
    }

    #[derive(Serialize)]
    struct AmqpMessage {
        #[serde(rename = "messageType")]
        message_type: &'static str,
    }

    // --8<-- [start:descriptor_bindings]
    /// A descriptor of the shape a broker crate ships: it reads its own private fields and says
    /// what the protocol calls them.
    #[derive(Clone)]
    struct RabbitQueue {
        name: &'static str,
        durable: bool,
    }

    impl<C: Subscribe> SubscriptionSource<C> for RabbitQueue {
        type Subscriber = C::Subscriber;
        type Copies = AddressedCopies;

        fn name(&self) -> &str {
            self.name
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            connected.subscribe(self.name).await
        }

        fn channel_bindings(&self) -> Bindings {
            let body = AmqpChannel {
                is: "queue",
                queue: AmqpQueue {
                    name: self.name,
                    durable: self.durable,
                },
            };
            Binding::new("amqp", "0.3.0", &body)
                .map(|binding| Bindings::new().with(binding))
                .unwrap_or_default()
        }

        fn operation_bindings(&self) -> Bindings {
            Binding::new("amqp", "0.3.0", &AmqpOperation { ack: true })
                .map(|binding| Bindings::new().with(binding))
                .unwrap_or_default()
        }

        fn message_bindings(&self) -> Bindings {
            let body = AmqpMessage {
                message_type: "order",
            };
            Binding::new("amqp", "0.3.0", &body)
                .map(|binding| Bindings::new().with(binding))
                .unwrap_or_default()
        }
    }
    // --8<-- [end:descriptor_bindings]

    impl<C: Subscribe> RedeliveryAddressed<C> for RabbitQueue {
        fn redelivery_address(
            &self,
            _connected: &C,
        ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
            std::future::ready(Ok(RedeliveryAddress::new(self.name)))
        }
    }

    #[subscriber(RabbitQueue { name: "orders", durable: true })]
    async fn confirm(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[test]
    fn a_descriptor_describes_its_channel_its_operation_and_its_messages() {
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let spec = build_spec(&app);
        let json = spec.to_json().expect("the document must serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        let channel = &value["channels"]["orders"]["bindings"]["amqp"];
        assert_eq!(channel["queue"]["name"], "orders");
        assert_eq!(channel["queue"]["durable"], true);
        // The core writes the version, so a broker cannot ship a binding without one.
        assert_eq!(channel["bindingVersion"], "0.3.0");

        assert_eq!(
            value["operations"]["receive_orders"]["bindings"]["amqp"]["ack"],
            true,
        );
        assert_eq!(
            value["components"]["messages"]["Order"]["bindings"]["amqp"]["messageType"],
            "order",
        );
    }

    /// A descriptor that says nothing leaves no empty objects behind.
    #[test]
    fn a_silent_descriptor_changes_no_document() {
        #[subscriber("plain")]
        async fn plain(order: &Order) -> HandlerOutcome {
            let _ = order.id;
            HandlerOutcome::ack()
        }

        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(plain);
            },
        );
        let json = build_spec(&app)
            .to_json()
            .expect("the document must serialize");

        assert!(!json.contains("\"bindings\""));
    }

    /// The server binding is the broker's too, and it travels on the server spec.
    #[test]
    fn a_server_carries_the_brokers_own_binding() {
        #[derive(Serialize)]
        struct MqttServer {
            #[serde(rename = "clientId")]
            client_id: &'static str,
        }

        let binding = Binding::new(
            "mqtt",
            "0.2.0",
            &MqttServer {
                client_id: "orders",
            },
        )
        .expect("mqtt is a listed protocol");
        let app = RustStream::new(AppInfo::new("orders", "1.0.0")).server(
            "mqtt",
            ServerSpec::new("mqtt.example.com:1883", "mqtt")
                .bindings(Bindings::new().with(binding)),
        );
        let json = build_spec(&app)
            .to_json()
            .expect("the document must serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        assert_eq!(
            value["servers"]["mqtt"]["bindings"]["mqtt"]["clientId"],
            "orders"
        );
        assert_eq!(
            value["servers"]["mqtt"]["bindings"]["mqtt"]["bindingVersion"],
            "0.2.0",
        );
    }

    /// The conformance scan is what keeps a broker honest about credentials, so it has to fail on
    /// one.
    #[cfg(feature = "conformance")]
    #[test]
    fn the_credential_scan_catches_a_password_in_a_binding() {
        use ruststream::conformance::harness;

        #[derive(Serialize)]
        struct Leaky {
            // What a broker does when it builds a binding out of its configuration URL instead of
            // the coordinate the document is allowed to carry.
            url: &'static str,
        }

        #[derive(Clone)]
        struct LeakyQueue;

        impl<C: Subscribe> SubscriptionSource<C> for LeakyQueue {
            type Subscriber = C::Subscriber;
            type Copies = AddressedCopies;

            fn name(&self) -> &'static str {
                "orders"
            }

            async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
                connected.subscribe("orders").await
            }

            fn channel_bindings(&self) -> Bindings {
                let body = Leaky {
                    url: "amqp://svc:hunter2@rabbit:5672",
                };
                Binding::new("amqp", "0.3.0", &body)
                    .map(|binding| Bindings::new().with(binding))
                    .unwrap_or_default()
            }
        }

        impl<C: Subscribe> RedeliveryAddressed<C> for LeakyQueue {
            fn redelivery_address(
                &self,
                _connected: &C,
            ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
                std::future::ready(Ok(RedeliveryAddress::new("orders")))
            }
        }

        let leak = std::panic::catch_unwind(|| {
            harness::describes_without_credentials(&MemoryBroker::new(), &LeakyQueue, "hunter2");
        });

        assert!(leak.is_err(), "a password in a binding must fail the scan");
    }
}
