//! A wrapper codec from the Codecs guide: an envelope that frames another codec's bytes with a
//! format version, plus the three levels a codec mounts at.
//!
//! ```text
//! cargo run --example custom_codec --features macros,memory,json,cbor -- run
//! ```

use bytes::BytesMut;
use ruststream::codec::{CborCodec, Codec, CodecError, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream, TypedPublisher};
use ruststream::subscriber;
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

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[subscriber("audit")]
async fn audit(order: &Order) -> HandlerResult {
    println!("audited order {}", order.id);
    HandlerResult::Ack
}

#[subscriber("billing", publish("receipts"))]
async fn bill(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[ruststream::app]
fn app() -> RustStream {
    let info = AppInfo::new("custom-codec", "0.1.0");
    // --8<-- [start:mount]
    RustStream::new(info).with_broker_codec(
        MemoryBroker::new(),
        // per scope: every handler mounted here decodes through the envelope
        Envelope::new(JsonCodec),
        |b| {
            b.include(handle);
            // per chain: this router names its own codec for what it mounts
            b.include_router(
                Router::new()
                    .with_codec(Envelope::new(CborCodec))
                    .include(audit),
            );
            // per publisher: the reply leaves under the envelope, the request still arrives
            // under the scope codec
            b.include(bill).publisher(TypedPublisher::with_codec(
                MemoryPublish,
                Envelope::new(JsonCodec),
            ));
        },
    )
    // --8<-- [end:mount]
}
