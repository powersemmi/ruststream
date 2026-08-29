//! A wrapper codec without the `macros` feature: the envelope itself is plain trait work, and the
//! levels it mounts at collapse into the two the hand-written path has - the `typed` call on the
//! decode side, the handler's own encoder on the publish side.
//!
//! ```text
//! cargo run --example manual_custom_codec --no-default-features --features memory,json,cbor
//! ```

use std::error::Error;
use std::future::{Future, ready};

use bytes::BytesMut;
use ruststream::codec::{CborCodec, Codec, CodecError, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::prelude::*;
use ruststream::runtime::{Handler, HandlerMetadata, Settle, typed};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// --8<-- [start:codec]
/// The marker byte that says these bytes are an envelope at all.
const MAGIC: u8 = 0xE1;
/// The envelope layout this build writes, and the only one it reads.
const VERSION: u8 = 1;

/// A codec that frames another codec's output: `inner` decides the payload format, the envelope
/// only puts a versioned header in front of it.
///
/// The version is a constant on purpose. Resolving it against a schema registry would need async
/// I/O, and a codec is synchronous - that integration goes on the async edges instead.
#[derive(Debug, Clone, Copy, Default)]
struct Envelope<C> {
    inner: C,
}

impl<C> Envelope<C> {
    const fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Codec> Codec for Envelope<C> {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let payload = self.inner.encode(value)?;
        let mut framed = BytesMut::with_capacity(2 + payload.len());
        framed.extend_from_slice(&[MAGIC, VERSION]);
        framed.extend_from_slice(&payload);
        Ok(framed)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        // The inner codec's failure already is a `CodecError` and travels up with `?`; only the
        // envelope's own failures need wrapping, and they carry their own error type as the
        // source so the message names the layer that rejected the payload.
        let payload = open(bytes).map_err(|err| CodecError::Decode(Box::new(err)))?;
        self.inner.decode(payload)
    }
}

/// Strips the header, naming which of its two bytes was wrong.
fn open(bytes: &[u8]) -> Result<&[u8], EnvelopeError> {
    match bytes {
        [MAGIC, VERSION, payload @ ..] => Ok(payload),
        [MAGIC, version, ..] => Err(EnvelopeError::Version(*version)),
        [magic, _, ..] => Err(EnvelopeError::Magic(*magic)),
        _ => Err(EnvelopeError::Truncated(bytes.len())),
    }
}

/// What the envelope rejects on its own, as opposed to what the inner codec rejects.
#[derive(Debug, Error)]
enum EnvelopeError {
    #[error("too short to carry an envelope header: {0} byte(s)")]
    Truncated(usize),
    #[error("not an envelope: leading byte {0:#04x}")]
    Magic(u8),
    #[error("envelope version {0} is not the version this build reads")]
    Version(u8),
}
// --8<-- [end:codec]

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Receipt {
    id: u64,
}

/// The definition value `#[subscriber("orders")]` would have minted.
struct Handle;

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly: `async fn` here would be an
    // unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

/// A second handler, mounted on a differently framed subscription.
struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("audited order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

/// The replying handler. `publish("receipts")` is a reply clause on the definition, so without it
/// the handler owns both halves of the reply: the publisher it sends through and the codec that
/// frames what it sends. Holding the codec rather than a `TypedPublisher` is what makes the
/// framing reachable - the typed publish builder resolves its destination from an `Outgoing`
/// declaration, which is a derive the macro-free path does not have.
struct Bill {
    receipts: MemoryPublisher,
    codec: Envelope<JsonCodec>,
}

impl Handler<Order> for Bill {
    async fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> Settle {
        let Ok(payload) = self.codec.encode(&Receipt { id: order.id }) else {
            return HandlerResult::drop().into();
        };
        if self
            .receipts
            .raw(&payload)
            .to("receipts")
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
        }
        HandlerResult::ack().into()
    }
}

fn app() -> RustStream {
    let info = AppInfo::new("custom-codec", "0.1.0");
    // --8<-- [start:mount]
    RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
        // per subscription: the codec is an argument, so two handlers on one broker frame their
        // payloads differently without a scope or a router to separate them
        b.subscribe(
            Name::new("orders"),
            typed(Envelope::new(JsonCodec), Handle),
            HandlerMetadata::typed::<Order>("orders"),
        );
        b.subscribe(
            Name::new("audit"),
            typed(Envelope::new(CborCodec), Audit),
            HandlerMetadata::typed::<Order>("audit"),
        );
        // per publisher: the reply leaves under the envelope, the request still arrives under the
        // subscription's own codec. The in-memory broker hands out a live publisher synchronously;
        // a networked broker resolves one at startup instead, and the handler reads it off the
        // application state.
        let receipts = b.broker().publisher();
        b.subscribe(
            Name::new("billing"),
            typed(
                Envelope::new(JsonCodec),
                Bill {
                    receipts,
                    codec: Envelope::new(JsonCodec),
                },
            ),
            HandlerMetadata::typed::<Order>("billing"),
        );
    })
    // --8<-- [end:mount]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
