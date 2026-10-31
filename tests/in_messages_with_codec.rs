//! A message set whose variant only the decode codec can name declares a discriminator type
//! with `#[in_messages(discriminate_with_codec = ..)]`: the type reads the scope codec and the
//! delivery and returns the kind to decode.

#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use std::error::Error;

use crate::log_capture::Captured;
use ruststream::codec::{Codec, CodecError, JsonCodec};
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// An order was placed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

/// An order was shipped: the same JSON shape as [`OrderCreated`], so only the registry tells
/// the two apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, MessageInfo)]
struct OrderShipped {
    id: u64,
}

/// What the handler reports back: the variant it matched and the id the model carried.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Outgoing)]
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

/// The schema id a producer stamps on every record, as the typed-headers publish writes it.
#[derive(Serialize)]
struct SchemaId {
    schema: &'static str,
}

/// What a registry codec answers beside decoding: the record name a schema id stands for.
trait RecordNames: Codec {
    fn record_name(&self, schema: &[u8]) -> Option<&'static str>;
}

/// JSON whose records are named by a registry the codec holds.
#[derive(Debug, Clone, Copy)]
struct Registry {
    records: &'static [(&'static str, &'static str)],
}

impl Codec for Registry {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        JsonCodec.encode(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes)
    }
}

impl RecordNames for Registry {
    fn record_name(&self, schema: &[u8]) -> Option<&'static str> {
        self.records
            .iter()
            .find(|(id, _)| id.as_bytes() == schema)
            .map(|(_, name)| *name)
    }
}

const REGISTRY: Registry = Registry {
    records: &[("1", "OrderCreated"), ("2", "OrderShipped")],
};

#[derive(InMessages)]
#[in_messages(discriminate_with_codec = ByRecord)]
enum OrderEvent {
    Created(OrderCreated),
    Shipped(OrderShipped),
}

/// Picks the variant by the record the registry names for the delivery's schema id.
struct ByRecord;

impl<DecodeCodec: RecordNames> CodecDiscriminator<OrderEventKind, DecodeCodec> for ByRecord {
    fn kind<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Option<OrderEventKind> {
        match codec.record_name(msg.headers().get("schema")?)? {
            "OrderCreated" => Some(OrderEventKind::Created),
            "OrderShipped" => Some(OrderEventKind::Shipped),
            _ => None,
        }
    }
}

#[subscriber("rec.orders", reply("rec.seen"))]
async fn on_event(event: &OrderEvent) -> Seen {
    match event {
        OrderEvent::Created(created) => Seen::of("created", created.id),
        OrderEvent::Shipped(shipped) => Seen::of("shipped", shipped.id),
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("rec", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        REGISTRY,
        |b| b.include(on_event),
    )
}

#[tokio::test]
async fn the_codec_names_the_variant() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let broker = tb.broker::<MemoryBroker>();

    // The same bytes decode as either model: the registry, not the order, picks the variant.
    broker
        .publish_with_headers(
            "rec.orders",
            &OrderShipped { id: 1 },
            &SchemaId { schema: "2" },
        )
        .await?;
    broker
        .publish_with_headers(
            "rec.orders",
            &OrderCreated { id: 2 },
            &SchemaId { schema: "1" },
        )
        .await?;
    tb.settle().await?;

    broker.subscriber("rec.orders").assert_called(2);
    assert_eq!(
        broker.published::<Seen>("rec.seen").decoded(),
        vec![Seen::of("shipped", 1), Seen::of("created", 2)]
    );
    Ok(())
}

#[tokio::test]
async fn a_schema_the_registry_does_not_know_is_a_decode_error() -> Result<(), Box<dyn Error>> {
    let captured = Captured::default();
    let _installed = captured.install();
    let tb = TestApp::start(app()).await?;
    let broker = tb.broker::<MemoryBroker>();

    broker
        .publish_with_headers(
            "rec.orders",
            &OrderCreated { id: 3 },
            &SchemaId { schema: "9" },
        )
        .await?;
    tb.settle().await?;

    broker
        .subscriber("rec.orders")
        .assert_called_once()
        .assert_last_failed_to_decode();
    let log = captured.text();
    assert!(
        log.contains("the discriminator of the message set `OrderEvent` chose no variant"),
        "the failure names the set: {log}"
    );
    Ok(())
}
