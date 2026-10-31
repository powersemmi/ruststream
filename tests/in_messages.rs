//! A handler that receives several message types on one subscription declares them with
//! `#[derive(InMessages)]`: each variant decodes through its own model's path, the variant is
//! picked left to right, by the user's discriminator, or by a header the derive reads, and the
//! generated document lists one message per variant.

#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use std::error::Error;
use std::future::ready;

use crate::common::Wire;
use crate::log_capture::Captured;
use prost::Message as _;
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryPosition, Retaining, Retention};
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

/// An order was placed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
struct OrderCreated {
    id: u64,
}

/// An order was cancelled, sent as Protobuf by another service.
#[derive(Clone, PartialEq, prost::Message, Deserialized, MessageInfo)]
#[wire(prost)]
struct OrderCancelled {
    #[prost(uint64, tag = "1")]
    id: u64,
}

/// An order was shipped: the same JSON shape as [`OrderCreated`], so only a discriminator tells
/// the two apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
struct OrderShipped {
    id: u64,
}

/// What a handler reports back: the variant it matched and the id the model carried.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
struct Seen {
    variant: String,
    id: u64,
}

impl Seen {
    fn of(variant: &str, id: u64) -> Self {
        Self {
            variant: variant.to_owned(),
            id,
        }
    }
}

/// The header a discriminator reads, as the typed-headers publish of the harness writes it.
#[derive(Serialize)]
struct EventHeader {
    event: &'static str,
}

/// Bytes published as themselves under the `event` header a discriminator reads.
#[derive(Outgoing, Serialized)]
#[outgoing(headers = EventHeader)]
struct Tagged(Vec<u8>);

fn cancelled_bytes(id: u64) -> Wire {
    Wire(OrderCancelled { id }.encode_to_vec())
}

mod left_to_right {
    use super::*;

    #[derive(InMessages)]
    enum OrderEvent {
        Created(OrderCreated),
        Cancelled(OrderCancelled),
    }

    #[subscriber("ltr.orders", reply("ltr.seen"))]
    async fn on_event(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created(created) => Seen::of("created", created.id),
            OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
        }
    }

    #[subscriber("ltr.batch", reply("ltr.batch.seen"))]
    async fn on_batch(events: &[OrderEvent]) -> Vec<Seen> {
        events
            .iter()
            .map(|event| match event {
                OrderEvent::Created(created) => Seen::of("created", created.id),
                OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
            })
            .collect()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("ltr", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_event);
        })
    }

    #[tokio::test]
    async fn each_variant_decodes_through_its_own_path() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish("ltr.orders", &OrderCreated { id: 1 })
            .await?;
        tb.settle().await?;
        broker
            .message(&cancelled_bytes(2))
            .to("ltr.orders")
            .publish()
            .await?;
        tb.settle().await?;

        broker.subscriber("ltr.orders").assert_called(2);
        assert_eq!(
            broker.published::<Seen>("ltr.seen").decoded(),
            vec![Seen::of("created", 1), Seen::of("cancelled", 2)]
        );
        Ok(())
    }

    /// The two deliveries are in the log before the subscription opens, so they reach the
    /// handler as one batch.
    #[tokio::test]
    async fn a_batch_mixes_the_variants() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
        let publisher = broker.publisher();
        publisher
            .message(&Wire(serde_json::to_vec(&OrderCreated { id: 3 })?))
            .to("ltr.batch")
            .publish()
            .await?;
        publisher
            .message(&cancelled_bytes(4))
            .to("ltr.batch")
            .publish()
            .await?;
        let app = RustStream::new(AppInfo::new("ltr", "0.1.0")).with_broker(broker, |b| {
            b.include(
                on_batch
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start()),
            );
        });
        let tb = TestApp::start(app).await?;
        tb.settle().await?;

        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker.subscriber("ltr.batch").assert_batch_sizes(&[2]);
        assert_eq!(
            broker.published::<Seen>("ltr.batch.seen").decoded(),
            vec![Seen::of("created", 3), Seen::of("cancelled", 4)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn every_variant_failing_is_one_decode_error_naming_each() -> Result<(), Box<dyn Error>> {
        let captured = Captured::default();
        let _installed = captured.install();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        // JSON with no `id` decodes as neither model: `OrderCreated` misses the field and the
        // Protobuf reader rejects the bytes.
        broker
            .message(&Wire::of(br#"{"sku":"widget"}"#))
            .to("ltr.orders")
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("ltr.orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        let log = captured.text();
        assert!(
            log.contains("no variant of the message set `OrderEvent` decodes the delivery"),
            "the failure names the set: {log}"
        );
        assert!(
            log.contains("`Created`:") && log.contains("`Cancelled`:"),
            "the failure carries every variant's error: {log}"
        );
        Ok(())
    }
}

/// A batch of a set mounts with an `Out` slot and no reply, like a batch of one model.
mod batch_slots {
    use super::*;

    #[derive(InMessages)]
    enum OrderEvent {
        Created(OrderCreated),
        Cancelled(OrderCancelled),
    }

    /// Each element of the batch leaves through the slot as what it was.
    #[subscriber("bslot.orders")]
    async fn on_batch(events: &[OrderEvent], Out(out): Out<impl Publisher>) -> HandlerOutcome {
        for event in events {
            let seen = match event {
                OrderEvent::Created(created) => Seen::of("created", created.id),
                OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
            };
            if out.message(&seen).to("bslot.seen").publish().await.is_err() {
                return HandlerOutcome::retry();
            }
        }
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_batch_of_a_set_publishes_through_its_slot() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
        let publisher = broker.publisher();
        publisher
            .message(&Wire(serde_json::to_vec(&OrderCreated { id: 15 })?))
            .to("bslot.orders")
            .publish()
            .await?;
        publisher
            .message(&cancelled_bytes(16))
            .to("bslot.orders")
            .publish()
            .await?;
        let app = RustStream::new(AppInfo::new("bslot", "0.1.0")).with_broker(broker, |b| {
            b.include(
                on_batch
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start()),
            )
            .out(DefaultSlot, Publish)
            .build();
        });
        let tb = TestApp::start(app).await?;
        tb.settle().await?;

        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker.subscriber("bslot.orders").assert_batch_sizes(&[2]);
        assert_eq!(
            broker.published::<Seen>("bslot.seen").decoded(),
            vec![Seen::of("created", 15), Seen::of("cancelled", 16)]
        );
        Ok(())
    }
}

/// A set handler that replies and publishes through a slot, one delivery at a time and as a
/// batch: the reply and the slot each see what the set decoded.
mod reply_and_slot {
    use super::*;

    #[derive(InMessages)]
    enum OrderEvent {
        Created(OrderCreated),
        Cancelled(OrderCancelled),
    }

    fn seen(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created(created) => Seen::of("created", created.id),
            OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
        }
    }

    /// Replies with what it saw and leaves an audit copy through its slot.
    #[subscriber("rslot.orders", reply("rslot.seen"))]
    async fn on_event(
        event: &OrderEvent,
        Out(audit): Out<impl Publisher>,
    ) -> Result<Seen, HandlerOutcome> {
        let seen = seen(event);
        audit
            .message(&seen)
            .to("rslot.audit")
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        Ok(seen)
    }

    /// The same for a whole batch: one reply and one audit copy per element.
    #[subscriber("rslot.batch", reply("rslot.batch.seen"))]
    async fn on_batch(
        events: &[OrderEvent],
        Out(audit): Out<impl Publisher>,
    ) -> Result<Vec<Seen>, HandlerOutcome> {
        let mut replies = Vec::with_capacity(events.len());
        for event in events {
            let seen = seen(event);
            audit
                .message(&seen)
                .to("rslot.batch.audit")
                .publish()
                .await
                .map_err(|_| HandlerOutcome::retry())?;
            replies.push(seen);
        }
        Ok(replies)
    }

    #[tokio::test]
    async fn a_set_handler_replies_and_publishes_through_its_slot() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
        let publisher = broker.publisher();
        publisher
            .message(&Wire(serde_json::to_vec(&OrderCreated { id: 21 })?))
            .to("rslot.batch")
            .publish()
            .await?;
        publisher
            .message(&cancelled_bytes(22))
            .to("rslot.batch")
            .publish()
            .await?;
        let app = RustStream::new(AppInfo::new("rslot", "0.1.0")).with_broker(broker, |b| {
            b.include(on_event)
                .out_reply(Publish)
                .out(DefaultSlot, Publish)
                .build();
            b.include(
                on_batch
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start()),
            )
            .out_reply(Publish)
            .out(DefaultSlot, Publish)
            .build();
        });
        let tb = TestApp::start(app).await?;
        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker
            .publish("rslot.orders", &OrderCreated { id: 20 })
            .await?;
        tb.settle().await?;

        assert_eq!(
            broker.published::<Seen>("rslot.seen").decoded(),
            vec![Seen::of("created", 20)]
        );
        assert_eq!(
            broker.published::<Seen>("rslot.audit").decoded(),
            vec![Seen::of("created", 20)]
        );
        let batch = vec![Seen::of("created", 21), Seen::of("cancelled", 22)];
        assert_eq!(
            broker.published::<Seen>("rslot.batch.seen").decoded(),
            batch
        );
        assert_eq!(
            broker.published::<Seen>("rslot.batch.audit").decoded(),
            batch
        );
        Ok(())
    }
}

mod discriminated {
    use super::*;

    #[derive(InMessages)]
    #[in_messages(discriminate = by_event_header)]
    enum OrderEvent {
        Created(OrderCreated),
        Shipped(OrderShipped),
    }

    fn by_event_header(message: &impl IncomingMessage) -> Option<OrderEventKind> {
        match message.headers().get("event")? {
            b"created" => Some(OrderEventKind::Created),
            b"shipped" => Some(OrderEventKind::Shipped),
            _ => None,
        }
    }

    #[subscriber("disc.orders", reply("disc.seen"))]
    async fn on_event(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created(created) => Seen::of("created", created.id),
            OrderEvent::Shipped(shipped) => Seen::of("shipped", shipped.id),
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("disc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_event);
        })
    }

    #[tokio::test]
    async fn the_function_picks_the_variant_without_trial() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        // The same bytes decode as either model: the header, not the order, picks `Shipped`.
        broker
            .publish_with_headers(
                "disc.orders",
                &OrderShipped { id: 5 },
                &EventHeader { event: "shipped" },
            )
            .await?;
        broker
            .publish_with_headers(
                "disc.orders",
                &OrderCreated { id: 6 },
                &EventHeader { event: "created" },
            )
            .await?;
        tb.settle().await?;

        assert_eq!(
            broker.published::<Seen>("disc.seen").decoded(),
            vec![Seen::of("shipped", 5), Seen::of("created", 6)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_kind_is_a_decode_error_naming_the_set() -> Result<(), Box<dyn Error>> {
        let captured = Captured::default();
        let _installed = captured.install();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish("disc.orders", &OrderCreated { id: 7 })
            .await?;
        tb.settle().await?;

        broker
            .subscriber("disc.orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        let log = captured.text();
        assert!(
            log.contains("the discriminator of the message set `OrderEvent` chose no variant"),
            "the failure names the set: {log}"
        );
        Ok(())
    }
}

mod by_header {
    use super::*;

    #[derive(InMessages)]
    #[in_messages(header = "event")]
    enum OrderEvent {
        #[in_messages(value = "created")]
        Created(OrderCreated),
        #[in_messages(value = "shipped")]
        Shipped(OrderShipped),
        #[in_messages(value = "cancelled")]
        Cancelled(OrderCancelled),
    }

    #[subscriber("hdr.orders", reply("hdr.seen"))]
    async fn on_event(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created(created) => Seen::of("created", created.id),
            OrderEvent::Shipped(shipped) => Seen::of("shipped", shipped.id),
            OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("hdr", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_event);
        })
    }

    #[tokio::test]
    async fn the_header_value_picks_the_variant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish_with_headers(
                "hdr.orders",
                &OrderShipped { id: 8 },
                &EventHeader { event: "shipped" },
            )
            .await?;
        broker
            .message(&Tagged(OrderCancelled { id: 9 }.encode_to_vec()))
            .to("hdr.orders")
            .with_headers(&EventHeader { event: "cancelled" })
            .publish()
            .await?;
        tb.settle().await?;

        assert_eq!(
            broker.published::<Seen>("hdr.seen").decoded(),
            vec![Seen::of("shipped", 8), Seen::of("cancelled", 9)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_header_is_a_decode_error_naming_it() -> Result<(), Box<dyn Error>> {
        let captured = Captured::default();
        let _installed = captured.install();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish("hdr.orders", &OrderCreated { id: 10 })
            .await?;
        tb.settle().await?;

        broker
            .subscriber("hdr.orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        let log = captured.text();
        assert!(
            log.contains("the delivery carries no `event` header")
                && log.contains("message set `OrderEvent`"),
            "the failure names the header and the set: {log}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_value_is_a_decode_error_naming_the_value_seen() -> Result<(), Box<dyn Error>>
    {
        let captured = Captured::default();
        let _installed = captured.install();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish_with_headers(
                "hdr.orders",
                &OrderCreated { id: 11 },
                &EventHeader { event: "returned" },
            )
            .await?;
        tb.settle().await?;

        broker
            .subscriber("hdr.orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        let log = captured.text();
        assert!(
            log.contains("the `event` header is `returned`")
                && log.contains("message set `OrderEvent`"),
            "the failure names the header, the value seen and the set: {log}"
        );
        Ok(())
    }
}

mod manual {
    use super::*;

    #[derive(InMessages)]
    #[in_messages(header = "event")]
    pub(super) enum OrderEvent {
        #[in_messages(value = "created")]
        Created(OrderCreated),
        #[in_messages(value = "cancelled")]
        Cancelled(OrderCancelled),
    }

    /// Acks a creation and drops a cancellation, so the settlement tells the variants apart.
    struct OnEvent;

    impl Handle<OrderEvent> for OnEvent {
        fn handle(
            &self,
            event: &OrderEvent,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            ready(match event {
                OrderEvent::Created(created) => {
                    tracing::info!(created.id, "created");
                    Ok(())
                }
                OrderEvent::Cancelled(cancelled) => {
                    tracing::info!(cancelled.id, "cancelled");
                    Err(HandlerOutcome::drop())
                }
            })
        }
    }

    /// The mount stays in the documented state: a set demands no schema of its own.
    pub(super) fn app() -> impl App {
        RustStream::new(AppInfo::new("man", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("man.orders", OnEvent).build());
        })
    }

    #[tokio::test]
    async fn a_hand_written_body_reads_the_set() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish_with_headers(
                "man.orders",
                &OrderCreated { id: 12 },
                &EventHeader { event: "created" },
            )
            .await?;
        tb.settle().await?;
        broker
            .subscriber("man.orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        broker
            .message(&Tagged(OrderCancelled { id: 13 }.encode_to_vec()))
            .to("man.orders")
            .with_headers(&EventHeader { event: "cancelled" })
            .publish()
            .await?;
        tb.settle().await?;
        broker
            .subscriber("man.orders")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}

/// A `serde` enum stays an ordinary input: `serde` picks the variant from the payload, and the
/// enum is one message to the runtime and to the document.
mod serde_tagged {
    use super::*;

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type")]
    enum OrderEvent {
        Created { id: u64 },
        Cancelled { id: u64 },
    }

    #[subscriber("tagged.orders", reply("tagged.seen"))]
    async fn on_event(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created { id } => Seen::of("created", *id),
            OrderEvent::Cancelled { id } => Seen::of("cancelled", *id),
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("tagged", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_event);
        })
    }

    #[tokio::test]
    async fn the_tag_in_the_payload_picks_the_variant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .message(&Wire::of(br#"{"type":"Cancelled","id":14}"#))
            .to("tagged.orders")
            .publish()
            .await?;
        tb.settle().await?;

        assert_eq!(
            broker.published::<Seen>("tagged.seen").decoded(),
            vec![Seen::of("cancelled", 14)]
        );
        Ok(())
    }
}

#[cfg(feature = "asyncapi")]
mod document {
    use super::*;
    use ruststream::asyncapi::build_spec;
    use serde_json::json;

    #[derive(InMessages)]
    #[in_messages(header = "event")]
    enum OrderEvent {
        #[in_messages(value = "created")]
        Created(OrderCreated),
        #[in_messages(value = "cancelled")]
        Cancelled(OrderCancelled),
    }

    /// Records every order event.
    #[subscriber("doc.orders")]
    async fn record(event: &OrderEvent) -> HandlerOutcome {
        match event {
            OrderEvent::Created(created) => tracing::info!(created.id, "created"),
            OrderEvent::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("doc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(record);
        })
    }

    #[test]
    fn the_receive_operation_lists_one_message_per_variant() {
        let spec = build_spec(&app());

        let receive = spec
            .operations
            .values()
            .find(|operation| operation.action == "receive")
            .expect("one receive operation");
        let refs: Vec<&str> = receive
            .messages
            .iter()
            .map(|reference| reference.reference.as_str())
            .collect();
        assert_eq!(
            refs,
            vec![
                "#/channels/doc.orders/messages/OrderCreated",
                "#/channels/doc.orders/messages/OrderCancelled",
            ]
        );
        let channel = &spec.channels["doc.orders"];
        assert!(channel.messages.contains_key("OrderCreated"));
        assert!(channel.messages.contains_key("OrderCancelled"));
    }

    #[test]
    fn a_manual_mount_lists_the_same_messages() {
        let spec = build_spec(&manual::app());

        let channel = &spec.channels["man.orders"];
        assert!(channel.messages.contains_key("OrderCreated"));
        assert!(channel.messages.contains_key("OrderCancelled"));
    }

    #[test]
    fn each_message_reports_its_own_format() {
        let spec = build_spec(&app());
        let messages = &spec.components.messages;

        let created = &messages["OrderCreated"];
        assert_eq!(created.content_type.as_deref(), Some("application/json"));
        assert_eq!(created.description.as_deref(), Some("An order was placed."));
        assert!(
            created.payload.is_some(),
            "the JSON model carries its schema"
        );

        let cancelled = &messages["OrderCancelled"];
        assert_eq!(cancelled.content_type, None);
        assert!(
            cancelled.schemaless,
            "a self-decoding model is schema-free by design"
        );
        assert_eq!(
            cancelled.description.as_deref(),
            Some("An order was cancelled, sent as Protobuf by another service.")
        );
    }

    /// The tenant every delivery of the subscription names, read by the handler.
    #[derive(Deserialize, schemars::JsonSchema)]
    struct Tenant {
        tenant: String,
    }

    /// Records every order event of a tenant.
    #[subscriber("doc.tenant")]
    async fn record_tenant(event: &OrderEvent, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
        match event {
            OrderEvent::Created(created) => tracing::info!(created.id, tenant.tenant, "created"),
            OrderEvent::Cancelled(cancelled) => {
                tracing::info!(cancelled.id, tenant.tenant, "cancelled");
            }
        }
        HandlerOutcome::ack()
    }

    #[test]
    fn the_handler_headers_join_each_variant() {
        let app =
            RustStream::new(AppInfo::new("doc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                b.include(record_tenant);
            });
        let spec = build_spec(&app);

        for name in ["OrderCreated", "OrderCancelled"] {
            let headers = spec.components.messages[name]
                .headers
                .as_ref()
                .expect("the handler's contract documents each variant");
            assert_eq!(
                headers["properties"]["tenant"],
                json!({ "type": "string" }),
                "{name}"
            );
            assert!(
                headers["properties"]["event"].get("const").is_some(),
                "{name}"
            );
            let required = headers["required"].as_array().expect("required list");
            assert!(required.contains(&json!("tenant")) && required.contains(&json!("event")));
        }
    }

    /// An order was returned, under the tenant that returned it.
    #[derive(Debug, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
    #[message(headers(Tenant))]
    struct OrderReturned {
        id: u64,
    }

    #[derive(InMessages)]
    #[in_messages(header = "event")]
    enum ReturnEvent {
        #[in_messages(value = "returned")]
        Returned(OrderReturned),
        #[in_messages(value = "created")]
        Created(OrderCreated),
    }

    /// Records returns, and gives a delivery up after two attempts.
    #[subscriber("doc.returns")]
    async fn record_returns(event: &ReturnEvent) -> HandlerOutcome {
        match event {
            ReturnEvent::Returned(returned) => tracing::info!(returned.id, "returned"),
            ReturnEvent::Created(created) => tracing::info!(created.id, "created"),
        }
        HandlerOutcome::ack()
    }

    #[test]
    fn a_copy_of_a_header_selected_model_keeps_its_contract() {
        let captured = Captured::default();
        let _installed = captured.install();
        let app =
            RustStream::new(AppInfo::new("doc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                b.include(record_returns)
                    .max_attempts(nonzero!(2))
                    .dead_letter("doc.returns.dlq");
            });
        let spec = build_spec(&app);

        let returned = spec.components.messages["OrderReturned"]
            .headers
            .as_ref()
            .expect("the model's contract and the header rule");
        assert_eq!(
            returned["properties"]["event"],
            json!({ "const": "returned" })
        );
        assert_eq!(
            returned["properties"]["tenant"],
            json!({ "type": "string" })
        );
        assert!(
            spec.channels["doc.returns.dlq"]
                .messages
                .contains_key("OrderReturned")
        );
        let log = captured.text();
        assert!(
            !log.contains("conflicting headers schemas"),
            "the copy carries the same contract as the receive side: {log}"
        );
    }

    /// Where an order came from, as the subscription's contract names it.
    mod routing {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub(super) enum Source {
            Web,
            Store,
        }
    }

    /// Who ships an order, as the model's own contract names it: the same type name as the
    /// subscription's, with other values.
    mod shipping {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub(super) enum Source {
            Warehouse,
            Courier,
        }
    }

    /// How the customer is told.
    #[derive(Deserialize, schemars::JsonSchema)]
    enum Channel {
        Email,
        Sms,
    }

    /// The subscription's contract: every delivery names its origin and its channel.
    #[derive(Deserialize, schemars::JsonSchema)]
    struct Routing {
        origin: routing::Source,
        channel: Channel,
    }

    /// The contract of a dispatched order: who ships it.
    #[derive(Deserialize, schemars::JsonSchema)]
    struct Shipping {
        source: shipping::Source,
    }

    /// An order was handed to a carrier.
    #[derive(Debug, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
    #[message(headers(Shipping))]
    struct OrderDispatched {
        id: u64,
    }

    #[derive(InMessages)]
    #[in_messages(header = "event")]
    enum DispatchEvent {
        #[in_messages(value = "dispatched")]
        Dispatched(OrderDispatched),
        #[in_messages(value = "created")]
        Created(OrderCreated),
    }

    /// Records dispatches with their routing.
    #[subscriber("doc.dispatch")]
    async fn record_dispatch(
        event: &DispatchEvent,
        Headers(routing): Headers<Routing>,
    ) -> HandlerOutcome {
        let _ = (&routing.origin, &routing.channel);
        match event {
            DispatchEvent::Dispatched(dispatched) => tracing::info!(dispatched.id, "dispatched"),
            DispatchEvent::Created(created) => tracing::info!(created.id, "created"),
        }
        HandlerOutcome::ack()
    }

    /// Every `$ref` inside `schema`, in document order.
    fn refs(schema: &serde_json::Value) -> Vec<String> {
        let mut found = Vec::new();
        match schema {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    if key == "$ref" {
                        if let Some(target) = value.as_str() {
                            found.push(target.to_owned());
                        }
                    } else {
                        found.extend(refs(value));
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    found.extend(refs(item));
                }
            }
            _ => {}
        }
        found
    }

    /// Asserts every `$ref` of `schema` names a definition the schema itself carries.
    fn assert_refs_resolve(name: &str, schema: &serde_json::Value) {
        let targets = refs(schema);
        assert!(
            !targets.is_empty(),
            "{name}: the contract references its enums"
        );
        for target in targets {
            let key = target
                .strip_prefix("#/definitions/")
                .unwrap_or_else(|| panic!("{name}: `{target}` is not a local definition"));
            assert!(
                schema["definitions"].get(key).is_some(),
                "{name}: `{target}` names no definition of the schema: {schema}"
            );
        }
    }

    #[test]
    fn the_joined_contracts_keep_their_definitions() {
        // The model's contract reads what its definition declares.
        let Shipping { source } = serde_json::from_value(json!({ "source": "Courier" }))
            .expect("a carrier the contract names");
        assert!(matches!(source, shipping::Source::Courier));

        let app =
            RustStream::new(AppInfo::new("doc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                b.include(record_dispatch);
            });
        let spec = build_spec(&app);

        for name in ["OrderDispatched", "OrderCreated"] {
            let headers = spec.components.messages[name]
                .headers
                .as_ref()
                .expect("a headers schema");
            assert_refs_resolve(name, headers);
            assert_eq!(
                headers["definitions"]["Channel"]["enum"],
                json!(["Email", "Sms"]),
                "{name}"
            );
        }
        let dispatched = spec.components.messages["OrderDispatched"]
            .headers
            .as_ref()
            .expect("a headers schema");
        // The model's definition keeps its name; the subscription's namesake takes the next
        // free number, and the property that reads it follows.
        assert_eq!(
            dispatched["definitions"]["Source"]["enum"],
            json!(["Warehouse", "Courier"])
        );
        assert_eq!(
            dispatched["definitions"]["Source_2"]["enum"],
            json!(["Web", "Store"])
        );
        assert_eq!(
            dispatched["properties"]["source"]["$ref"],
            "#/definitions/Source"
        );
        assert_eq!(
            dispatched["properties"]["origin"]["$ref"],
            "#/definitions/Source_2"
        );
        assert_eq!(
            dispatched["properties"]["channel"]["$ref"],
            "#/definitions/Channel"
        );
    }

    #[test]
    fn the_header_rule_is_a_const_in_each_headers_schema() {
        let spec = build_spec(&app());
        let messages = &spec.components.messages;

        let created = messages["OrderCreated"]
            .headers
            .as_ref()
            .expect("the header form documents its header");
        assert_eq!(
            created["properties"]["event"],
            json!({ "const": "created" })
        );
        assert_eq!(created["required"], json!(["event"]));
        let cancelled = messages["OrderCancelled"]
            .headers
            .as_ref()
            .expect("the header form documents its header");
        assert_eq!(
            cancelled["properties"]["event"],
            json!({ "const": "cancelled" })
        );
    }
}
