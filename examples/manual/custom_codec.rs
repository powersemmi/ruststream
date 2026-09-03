//! A wrapper codec without the `macros` feature: the envelope itself is plain trait work, and the
//! levels it mounts at collapse into the two the hand-written path has - the `codec(..)` step on
//! the decode side, the handler's own encoder on the publish side.
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Receipt {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the destination form is the one that leaves the
// name to the call, so the publish builder offers `to(..)`.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// The definition value `#[subscriber("orders")]` would have minted.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("got order {}", order.id);
        ready(Ok(()))
    }
}

/// A second handler, mounted on a differently framed subscription.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("audited order {}", order.id);
        ready(Ok(()))
    }
}

/// The replying handler. `publish("receipts")` is a reply clause on the definition, so without it
/// the handler owns both halves of the reply: the publisher it sends through and the codec that
/// frames what it sends. Holding the codec rather than a `TypedPublisher` is what keeps the
/// framing per handler - the publish names it at the call, the most specific rung of the codec
/// ladder.
struct Bill {
    receipts: MemoryPublisher,
    codec: Envelope<JsonCodec>,
}

impl Handle<Order> for Bill {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if self
            .receipts
            .message(&Receipt { id: order.id })
            .with_codec(self.codec)
            .to("receipts")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> RustStream {
    let info = AppInfo::new("custom-codec", "0.1.0");
    // --8<-- [start:mount]
    RustStream::new(info).with_broker(MemoryBroker::new(), |b| {
        // per subscription: the codec is a step on the definition, so two handlers on one broker
        // frame their payloads differently without a scope or a router to separate them
        b.include(
            subscriber("orders", Receive)
                .codec(Envelope::new(JsonCodec))
                .build(),
        );
        b.include(
            subscriber("audit", Audit)
                .codec(Envelope::new(CborCodec))
                .build(),
        );
        // per publisher: the reply leaves under the envelope, the request still arrives under the
        // subscription's own codec. The in-memory broker hands out a live publisher synchronously;
        // a networked broker resolves one at startup instead, and the handler reads it off the
        // application state.
        let receipts = b.broker().publisher();
        b.include(
            subscriber(
                "billing",
                Bill {
                    receipts,
                    codec: Envelope::new(JsonCodec),
                },
            )
            .codec(Envelope::new(JsonCodec))
            .build(),
        );
    })
    // --8<-- [end:mount]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
