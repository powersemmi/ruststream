//! A message set whose variant only the decode codec can name declares `#[in_messages(by_codec)]`:
//! the codec reads a frame from the payload and says which model it names.

#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use std::error::Error;

use crate::log_capture::Captured;
use ruststream::codec::{Codec, CodecError, FrameNames, JsonCodec};
use ruststream::diagnostics::TracingLogger;
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

/// The record the registry names for a payload's schema id, if it knows the id.
struct Record(Option<&'static str>);

/// JSON behind a one-byte schema id; a registry the codec holds names the record behind it.
#[derive(Debug, Clone, Copy)]
struct Registry {
    records: &'static [(u8, &'static str)],
}

impl Codec for Registry {
    type Frame<'a> = Record;

    fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
        let schema = payload.first();
        Ok(Record(
            self.records
                .iter()
                .find(|(id, _)| Some(id) == schema)
                .map(|(_, name)| *name),
        ))
    }

    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let mut framed = BytesMut::from(&[0u8][..]);
        framed.extend_from_slice(&JsonCodec.encode(value)?);
        Ok(framed)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes.get(1..).unwrap_or_default())
    }
}

impl<Model: MessageInfo> FrameNames<Model> for Registry {
    fn names(frame: &Record) -> bool {
        frame.0 == Some(Model::NAME)
    }
}

const REGISTRY: Registry = Registry {
    records: &[(1, "OrderCreated"), (2, "OrderShipped")],
};

#[derive(InMessages)]
#[in_messages(by_codec)]
enum OrderEvent {
    Created(OrderCreated),
    Shipped(OrderShipped),
}

#[subscriber("rec.orders", reply("rec.seen"))]
async fn on_event(event: &OrderEvent) -> Seen {
    match event {
        OrderEvent::Created(created) => Seen::of("created", created.id),
        OrderEvent::Shipped(shipped) => Seen::of("shipped", shipped.id),
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("rec", "0.1.0"))
        .logger(TracingLogger)
        .with_broker_codec(MemoryBroker::new(), REGISTRY, |b| b.include(on_event))
}

#[tokio::test]
async fn the_codec_names_the_variant() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let broker = tb.broker::<MemoryBroker>();

    // The same JSON decodes as either model: the schema id the codec resolves picks the variant.
    broker
        .message(&Framed::of(2, &OrderShipped { id: 1 })?)
        .to("rec.orders")
        .publish()
        .await?;
    broker
        .message(&Framed::of(1, &OrderCreated { id: 2 })?)
        .to("rec.orders")
        .publish()
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
        .message(&Framed::of(9, &OrderCreated { id: 3 })?)
        .to("rec.orders")
        .publish()
        .await?;
    tb.settle().await?;

    broker
        .subscriber("rec.orders")
        .assert_called_once()
        .assert_last_failed_to_decode();
    let log = captured.text();
    assert!(
        log.contains("the codec frame names no variant of the message set `OrderEvent`"),
        "the failure names the set: {log}"
    );
    Ok(())
}
