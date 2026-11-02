//! Several message types on one subscription: the handler's input is an enum that declares the
//! models it receives, and each delivery decodes as the variant the set's rule picks.
//!
//! The JSON model decodes through the codec of the mount and the Protobuf model reads its own
//! bytes: a set mixes the lanes, and the generated document lists each model with its own
//! format. Four sets below, one per rule: the `event` header, a function over the delivery, the
//! frame the decode codec reads, and the order of the variants.
//!
//! ```text
//! cargo test --example in_messages --features testing,macros,memory
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;

use ruststream::codec::{Codec, CodecError, FrameNames, JsonCodec};
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// --8<-- [start:models]
/// An order was placed.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
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
// --8<-- [end:models]

// --8<-- [start:set]
/// Every event of an order on the `orders` subscription: the `event` header names the model.
#[derive(InMessages)]
#[in_messages(header = "event")]
enum OrderEvent {
    #[in_messages(value = "created")]
    Created(OrderCreated),
    #[in_messages(value = "cancelled")]
    Cancelled(OrderCancelled),
}
// --8<-- [end:set]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn on_event(event: &OrderEvent) -> HandlerOutcome {
    match event {
        OrderEvent::Created(created) => tracing::info!(created.id, "created"),
        OrderEvent::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:discriminate]
/// The same models under a rule no header states: the first byte of the payload tells JSON
/// from Protobuf. The function reads the delivery and names the kind to decode.
#[derive(InMessages)]
#[in_messages(discriminate = by_payload_shape)]
enum OrderUpdate {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

fn by_payload_shape(message: &impl IncomingMessage) -> Option<OrderUpdateKind> {
    match message.payload().first()? {
        b'{' => Some(OrderUpdateKind::Created),
        _ => Some(OrderUpdateKind::Cancelled),
    }
}

#[subscriber("orders.updates")]
async fn on_update(update: &OrderUpdate) -> HandlerOutcome {
    match update {
        OrderUpdate::Created(created) => tracing::info!(created.id, "created"),
        OrderUpdate::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
    }
    HandlerOutcome::ack()
}
// --8<-- [end:discriminate]

// --8<-- [start:by_codec]
/// The record a schema id stands for, if the registry knows the id: what the codec reads from a
/// payload before decoding it.
struct Record(Option<&'static str>);

/// JSON behind a one-byte schema id; the registry here is a fixed table. The codec resolves the
/// id to the record it stands for, and that record is the frame.
#[derive(Debug, Clone, Copy)]
struct Registry;

impl Codec for Registry {
    type Frame<'a> = Record;

    fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
        Ok(Record(match payload.first() {
            Some(1) => Some("OrderCreated"),
            Some(2) => Some("OrderPlaced"),
            _ => None,
        }))
    }

    /// The registry is read here, never written: the producer that owns it frames its records.
    fn encode<T: Serialize>(&self, _value: &T) -> Result<BytesMut, CodecError> {
        Err(CodecError::Encode(
            "the registry codec reads records only".into(),
        ))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes.get(1..).unwrap_or_default())
    }
}

/// A model is named by the frame when its own name is the record the id stands for.
impl<Model: MessageInfo> FrameNames<Model> for Registry {
    fn names(frame: &Record) -> bool {
        frame.0 == Some(Model::NAME)
    }
}

/// An order was placed through the legacy checkout: the same JSON shape as `OrderCreated`, so
/// only the registry tells the two apart.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema, MessageInfo)]
struct OrderPlaced {
    id: u64,
}

/// Records named by the schema id the scope codec resolves. The set mounts only under a codec
/// that implements `FrameNames` for both models.
#[derive(InMessages)]
#[in_messages(by_codec)]
enum OrderRecord {
    Created(OrderCreated),
    Placed(OrderPlaced),
}

#[subscriber("orders.records")]
async fn on_record(record: &OrderRecord) -> HandlerOutcome {
    match record {
        OrderRecord::Created(created) => tracing::info!(created.id, "created"),
        OrderRecord::Placed(placed) => tracing::info!(placed.id, "placed"),
    }
    HandlerOutcome::ack()
}
// --8<-- [end:by_codec]

// --8<-- [start:left_to_right]
/// No rule: the variants are tried in their order, and the first that decodes wins. The JSON
/// model comes first because it is the stricter one: a Protobuf reader skips what it does not
/// know, so Protobuf bytes never pass as JSON while the reverse could happen.
#[derive(InMessages)]
enum OrderChange {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

#[subscriber("orders.changes")]
async fn on_change(change: &OrderChange) -> HandlerOutcome {
    match change {
        OrderChange::Created(created) => tracing::info!(created.id, "created"),
        OrderChange::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
    }
    HandlerOutcome::ack()
}
// --8<-- [end:left_to_right]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(Router::new().with_codec(Registry).include(on_record))
            .include(on_event)
            .include(on_update)
            .include(on_change)
    })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;

    /// The header the set reads, as the typed-headers publish of the harness writes it.
    #[derive(Serialize)]
    struct EventHeader {
        event: &'static str,
    }

    /// Protobuf bytes published as themselves under the `event` header.
    #[derive(Outgoing, Serialized)]
    #[outgoing(headers = EventHeader)]
    struct Tagged(Vec<u8>);

    /// Protobuf bytes published as themselves, with no header.
    #[derive(Outgoing, Serialized)]
    struct Raw(Vec<u8>);

    #[tokio::test]
    async fn the_header_names_the_model() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish_with_headers(
                "orders",
                &OrderCreated { id: 1 },
                &EventHeader { event: "created" },
            )
            .await?;
        broker
            .message(&Tagged(OrderCancelled { id: 2 }.encode_to_vec()))
            .to("orders")
            .with_headers(&EventHeader { event: "cancelled" })
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn the_function_names_the_model() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish("orders.updates", &OrderCreated { id: 3 })
            .await?;
        broker
            .message(&Raw(OrderCancelled { id: 4 }.encode_to_vec()))
            .to("orders.updates")
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("orders.updates")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// A record as a producer sends it: the schema id byte, then the JSON body.
    #[derive(Outgoing, Serialized)]
    struct Framed(Vec<u8>);

    impl Framed {
        fn of<T: Serialize>(schema: u8, value: &T) -> Result<Self, serde_json::Error> {
            let mut bytes = vec![schema];
            bytes.extend(serde_json::to_vec(value)?);
            Ok(Self(bytes))
        }
    }

    #[tokio::test]
    async fn the_codec_names_the_model() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .message(&Framed::of(2, &OrderPlaced { id: 7 })?)
            .to("orders.records")
            .publish()
            .await?;
        // A schema id the registry does not know names no record: a decode failure.
        broker
            .message(&Framed::of(9, &OrderCreated { id: 8 })?)
            .to("orders.records")
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("orders.records")
            .assert_called(2)
            .assert_last_failed_to_decode();
        Ok(())
    }

    #[test]
    fn the_registry_codec_refuses_to_write_a_record() {
        assert!(
            Registry.encode(&OrderCreated { id: 5 }).is_err(),
            "a record the codec cannot name is not written"
        );
    }

    #[tokio::test]
    async fn the_order_of_the_variants_names_the_model() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish("orders.changes", &OrderCreated { id: 5 })
            .await?;
        broker
            .message(&Raw(OrderCancelled { id: 6 }.encode_to_vec()))
            .to("orders.changes")
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("orders.changes")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
