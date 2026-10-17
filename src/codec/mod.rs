//! Pluggable serialization codecs: a [`Codec`] turns a value into wire bytes and back.
//!
//! The codec is a compile-time type fixed where a handler is mounted, so it costs nothing on the
//! delivery path. Each built-in codec is an additive cargo feature, and `DefaultCodec` is the
//! one a mounting that names none decodes and encodes with.
//!
//! - [Codecs][codecs]: where a mounting takes its codec from (`with_broker_codec`,
//!   `Router::with_codec`, `.out_reply(..).codec(..)`) and the default it falls back to.
//! - [JSON][json]: `JsonCodec` over `serde_json`, the `json` feature and the default.
//! - [CBOR][cbor]: `CborCodec` over `ciborium`, the `cbor` feature.
//! - [MessagePack][msgpack]: `MsgpackCodec` over `rmp-serde`, the `msgpack` feature.
//! - [Codec][custom]: a codec of your own, an implementation of [`Codec`] that returns
//!   [`CodecError`] and names its [`Codec::CONTENT_TYPE`].
//! - [Protobuf][protobuf]: a `prost` message on the byte lanes with `#[wire(prost)]`, with no
//!   codec feature.
//! - [Bytes][bytes]: payloads that are their own encoding,
//!   [`Deserialized`](crate::runtime::Deserialized) on the way in and
//!   [`Serialized`](crate::runtime::Serialized) on the way out.
//!
//! [codecs]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/
//! [json]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/json/
//! [cbor]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/cbor/
//! [msgpack]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/msgpack/
//! [custom]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/custom-codec/
//! [protobuf]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/protobuf/
//! [bytes]: https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/bytes/

/// The block an encode buffer starts with, in bytes.
///
/// A serializer writes a document in fragments, so a buffer that starts empty is grown - and
/// reallocated - more than once before even the smallest message is out. This is the capacity
/// `serde_json`'s own `to_vec` reserves, and it puts a single block between a value and the wire
/// at every size: a small document fits the first write, and a larger one grows from here exactly
/// as a `Vec` would.
#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
pub(crate) const ENCODE_CAPACITY: usize = 128;

#[cfg(feature = "json")]
mod json;

#[cfg(feature = "msgpack")]
mod msgpack;

#[cfg(feature = "cbor")]
mod cbor;

#[cfg(feature = "cbor")]
pub use self::cbor::CborCodec;
#[cfg(feature = "json")]
pub use json::JsonCodec;
#[cfg(feature = "msgpack")]
pub use msgpack::MsgpackCodec;

/// The codec used when an `include` / publisher call does not name one explicitly.
///
/// Resolved at compile time by feature priority: `json`, then `cbor`, then `msgpack`. It exists
/// only when at least one codec feature is enabled; with none, a mounting that needs the default
/// is a compile error listing the ways out: enable a feature, name a codec, or move the message
/// to a byte lane.
#[cfg(feature = "json")]
pub type DefaultCodec = JsonCodec;
/// The codec used when an `include` / publisher call does not name one explicitly. See the `json`
/// variant for details.
#[cfg(all(not(feature = "json"), feature = "cbor"))]
pub type DefaultCodec = CborCodec;
/// The codec used when an `include` / publisher call does not name one explicitly. See the `json`
/// variant for details.
#[cfg(all(not(feature = "json"), not(feature = "cbor"), feature = "msgpack"))]
pub type DefaultCodec = MsgpackCodec;

use std::error::Error as StdError;
#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
use std::io;

use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// The [`io::Write`] a serializer writes an encode buffer through: every write is an append.
///
/// `bytes`' own writer answers `write_all` with the trait's default loop over `write`, which the
/// optimizer inlines or not depending on what else the binary holds; the difference is several
/// hundred instructions per encoded message. These methods are appends marked for inlining, so
/// the encode path costs the same whatever else changes around it.
#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
pub(crate) struct BufWriter<'a>(pub(crate) &'a mut BytesMut);

#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
impl io::Write for BufWriter<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Errors returned by codec implementations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The codec failed to encode a Rust value into bytes.
    #[error("encode failed: {0}")]
    Encode(#[source] Box<dyn StdError + Send + Sync>),

    /// The codec failed to decode bytes into a Rust value.
    #[error("decode failed: {0}")]
    Decode(#[source] Box<dyn StdError + Send + Sync>),
}

/// A serializer that converts Rust values to and from bytes.
///
/// Implementations are stateless and cheap to clone. The trait uses generic methods rather
/// than associated types so a single codec instance can handle any `Serialize` /
/// `DeserializeOwned` value. This means `dyn Codec` is not object-safe; use generics or
/// boxed concrete codecs at the call site.
///
/// `encode` and `decode` are synchronous: a codec holds only what a constant and the bytes at
/// hand decide. An integration that needs I/O to serialize (a schema registry, a key service)
/// goes around the codec, on the async edges: the broker's delivery path on the way in, a
/// [`PublishLayer`](crate::runtime::PublishLayer) on the way out.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::BytesMut;
/// use ruststream::codec::{Codec, CodecError};
/// use ruststream::memory::prelude::*;
/// use serde::de::DeserializeOwned;
/// use serde::{Deserialize, Serialize};
///
/// /// JSON under the vendor media type the order contract was published with.
/// #[derive(Debug, Clone, Copy, Default)]
/// struct OrdersJson;
///
/// impl Codec for OrdersJson {
///     const CONTENT_TYPE: &'static str = "application/vnd.orders.v1+json";
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         let bytes =
///             serde_json::to_vec(value).map_err(|error| CodecError::Encode(Box::new(error)))?;
///         Ok(BytesMut::from(bytes.as_slice()))
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         serde_json::from_slice(bytes).map_err(|error| CodecError::Decode(Box::new(error)))
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     // The scope decodes with the codec, and the document reports its media type.
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
///         MemoryBroker::new(),
///         OrdersJson,
///         |b| {
///             b.include(accept);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A codec generic over another composes: the inner one decides the format, and the wrapper
/// transforms the bytes around it (a versioned envelope, a registry header, an encrypting layer).
/// A failure of the wrapper becomes [`CodecError::Decode`] or [`CodecError::Encode`] with its own
/// error as the source. The wrapper writes its version byte into the caller's buffer in
/// [`encode_into`](Codec::encode_into) and hands the rest to the inner codec:
///
/// ```
/// # #[cfg(not(feature = "json"))]
/// # fn main() {}
/// # #[cfg(feature = "json")]
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::fmt;
///
/// use ruststream::BytesMut;
/// use ruststream::codec::{Codec, CodecError, JsonCodec};
/// use serde::Serialize;
/// use serde::de::DeserializeOwned;
///
/// /// Frames another codec's bytes with a one-byte format version.
/// #[derive(Clone, Copy)]
/// struct Versioned<C>(C);
///
/// const VERSION: u8 = 1;
///
/// #[derive(Debug)]
/// struct BadVersion(u8);
///
/// impl fmt::Display for BadVersion {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "envelope version {} is not the version this build reads", self.0)
///     }
/// }
///
/// impl std::error::Error for BadVersion {}
///
/// impl<C: Codec> Codec for Versioned<C> {
///     const CONTENT_TYPE: &'static str = C::CONTENT_TYPE;
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         let mut framed = BytesMut::new();
///         self.encode_into(value, &mut framed)?;
///         Ok(framed)
///     }
///
///     fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
///         buf.extend_from_slice(&[VERSION]);
///         self.0.encode_into(value, buf)
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         match bytes {
///             [VERSION, payload @ ..] => self.0.decode(payload),
///             [version, ..] => Err(CodecError::Decode(Box::new(BadVersion(*version)))),
///             [] => Err(CodecError::Decode(Box::new(BadVersion(0)))),
///         }
///     }
/// }
///
/// let codec = Versioned(JsonCodec);
/// let bytes = codec.encode(&7u32)?;
/// assert_eq!(bytes[0], VERSION);
/// let back: u32 = codec.decode(&bytes)?;
/// assert_eq!(back, 7);
/// # Ok(())
/// # }
/// ```
pub trait Codec: Send + Sync {
    /// The media type of the bytes this codec produces, reported as the `contentType` of every
    /// message it encodes or decodes in the generated `AsyncAPI` document.
    ///
    /// The built-in codecs name `application/json`, `application/cbor` (RFC 8949) and
    /// `application/msgpack`. `MessagePack` has no registered media type; of the two spellings
    /// in use, `application/msgpack` is the one this crate reports.
    ///
    /// The default is `application/octet-stream`: what a codec whose output carries no media
    /// type of its own should keep.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use ruststream::BytesMut;
    /// use ruststream::codec::{Codec, CodecError};
    /// use ruststream::memory::prelude::*;
    /// use serde::de::DeserializeOwned;
    /// use serde::{Deserialize, Serialize};
    ///
    /// /// JSON under the vendor media type the order contract was published with: the generated
    /// /// document reports it as the `contentType` of every message this codec carries.
    /// #[derive(Debug, Clone, Copy, Default)]
    /// struct OrdersJson;
    ///
    /// impl Codec for OrdersJson {
    ///     const CONTENT_TYPE: &'static str = "application/vnd.orders.v1+json";
    ///
    ///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
    ///         let bytes = serde_json::to_vec(value)
    ///             .map_err(|error| CodecError::Encode(Box::new(error)))?;
    ///         Ok(BytesMut::from(bytes.as_slice()))
    ///     }
    ///
    ///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
    ///         serde_json::from_slice(bytes).map_err(|error| CodecError::Decode(Box::new(error)))
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     // The scope decodes with the codec, and the document reports its media type.
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
    ///         MemoryBroker::new(),
    ///         OrdersJson,
    ///         |b| {
    ///             b.include(accept);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    /// Encodes `value` into a mutable byte buffer.
    ///
    /// Returning [`BytesMut`] lets the encoded buffer move into the publish pipeline (an
    /// [`Outgoing`](crate::runtime::Outgoing) payload) without a copy, while still allowing
    /// publish middleware to mutate it in place. It is what a publish to a transport that keeps
    /// the payload ([`Take`](crate::Take)) calls, once per message.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the underlying serializer fails.
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError>;

    /// Encodes `value` into the end of a buffer the caller owns.
    ///
    /// What a publish to a transport that only reads the payload ([`Lend`](crate::Lend)) calls:
    /// inside a dispatch loop that buffer is the loop's own, reused for every message, so a
    /// codec that writes into it leaves the publish path with nothing to allocate. Implement it
    /// wherever the serializer can write into a sink; the default encodes into a buffer of its
    /// own and copies, which is correct but pays the allocation this exists to avoid.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the underlying serializer fails. The buffer's
    /// contents are then unspecified: the publish path discards what a failed encode wrote.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::BytesMut;
    /// use ruststream::codec::{Codec, CodecError, JsonCodec};
    /// use ruststream::runtime::{Serialized, WireBytes};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// /// A value that brings its own wire format: a one-byte schema tag, then the JSON body, both
    /// /// written straight into the buffer the publish lends it.
    /// struct Tagged(Order);
    ///
    /// impl Serialized for Tagged {
    ///     type Error = CodecError;
    ///
    ///     fn wire_bytes(&self, buf: &mut BytesMut) -> Result<WireBytes<'_>, CodecError> {
    ///         buf.extend_from_slice(&[1]);
    ///         JsonCodec.encode_into(&self.0, buf)?;
    ///         Ok(WireBytes::InBuffer)
    ///     }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
        buf.extend_from_slice(&self.encode(value)?);
        Ok(())
    }

    /// Decodes `bytes` into a Rust value of type `T`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when the underlying deserializer fails.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
