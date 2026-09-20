//! Pluggable serialization codecs.
//!
//! A [`Codec`] turns a value into wire bytes and back. It is not tied to the broker: on the
//! consume side the pipeline is `bytes -> Codec -> typed payload -> handler`, and on the publish
//! side the same runs in reverse. The codec is fixed where the handler is mounted, so it costs
//! nothing on the delivery path.
//!
//! # Cargo features
//!
//! Codecs are additive cargo features: enable only what you need. A message type derives
//! `serde::Deserialize`, and a reply type `Serialize` as well.
//!
//! * `json` (default): [`JsonCodec`] via `serde_json`, `application/json`.
//! * `msgpack`: [`MsgpackCodec`] via `rmp-serde`, `application/msgpack`.
//! * `cbor`: [`CborCodec`] via `ciborium`, `application/cbor`.
//!
//! [`DefaultCodec`] is an alias picked by the enabled features: `json`, else `cbor`, else
//! `msgpack`. It is what `include(def)` decodes with and what `.out_reply(policy)` encodes with
//! when nothing names a codec. With no codec feature at all there is nothing to name, so a
//! mounting that needs the default becomes a compile error listing the ways out: enable a
//! feature, name a codec, or move the message to a byte lane.
//!
//! # Where the codec comes from
//!
//! The decode codec comes from the most specific level that names one, and `include` takes no
//! codec argument:
//!
//! 1. per handler, `Router::new().with_codec(codec).include(def)`;
//! 2. per scope, `with_broker_codec(broker, codec, |b| ..)`;
//! 3. [`DefaultCodec`].
//!
//! The publish side mirrors it at the mount site: `.out_reply(policy).codec(codec)` names the
//! reply codec, `.out(marker, policy).codec(codec)` a slot's, and `.with_codec(codec)` on the
//! builder one call's. The request decodes with the scope's codec while the reply travels on the
//! wiring the chain built, so the two formats differ freely. The codec is a property of the
//! mounting, never of the message type.
//!
//! ```
//! # #[cfg(all(feature = "macros", feature = "memory", feature = "json", feature = "cbor"))]
//! # mod demo {
//! use ruststream::codec::{CborCodec, JsonCodec};
//! use ruststream::memory::prelude::*;
//! use serde::Deserialize;
//!
//! # #[derive(Deserialize)]
//! # struct Order {
//! #     id: u64,
//! # }
//! # #[subscriber("orders")]
//! # async fn handle(order: &Order) -> HandlerOutcome {
//! #     HandlerOutcome::ack()
//! # }
//! # #[subscriber("audit")]
//! # async fn audit(order: &Order) -> HandlerOutcome {
//! #     HandlerOutcome::ack()
//! # }
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("codecs", "0.1.0"))
//!         // per scope: every handler mounted here decodes with CBOR
//!         .with_broker_codec(MemoryBroker::new(), CborCodec, |b| {
//!             b.include(handle);
//!             // per handler: this mounting decodes with JSON
//!             b.include_router(Router::new().with_codec(JsonCodec).include(audit));
//!         })
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! A payload that does not decode is settled by the subscriber's `on_failure(decode = ..)`
//! policy, a drop by default; see [`FailurePolicy`](crate::runtime::FailurePolicy).
//!
//! # Byte lanes
//!
//! A value that is its own encoding takes no codec position. A newtype over `&'a [u8]` deriving
//! [`Deserialized`](macro@crate::Deserialized) receives the delivery's bytes as they arrived,
//! and a type deriving [`Serialized`](macro@crate::Serialized) publishes the bytes it produces.
//! A generated Protobuf message goes on both lanes with `#[wire(prost)]` next to the derives;
//! the general form names the functions, `#[wire(encode = <path>, decode = <path>)]`, and serves
//! Cap'n Proto, `FlatBuffers` and a hand-rolled frame alike. A service on the lanes alone runs
//! with no codec feature.
//!
//! # A codec of your own
//!
//! Implement [`Codec`] and pass the value wherever a built-in one goes. A codec generic over
//! another composes: the inner one decides the format, the wrapper transforms the bytes around
//! it, which is the shape of a versioned envelope, a registry header or an encrypting layer.
//! Both sides return [`CodecError`]; an inner error passes up with `?`, and a failure of the
//! wrapper becomes [`CodecError::Decode`] or [`CodecError::Encode`] with your error as its
//! source. [`Codec::CONTENT_TYPE`] names the media type the generated document reports.
//!
//! ```
//! # #[cfg(not(feature = "json"))]
//! # fn main() {}
//! # #[cfg(feature = "json")]
//! # fn main() {
//! use std::fmt;
//!
//! use ruststream::BytesMut;
//! use ruststream::codec::{Codec, CodecError, JsonCodec};
//! use serde::Serialize;
//! use serde::de::DeserializeOwned;
//!
//! /// Frames another codec's bytes with a one-byte format version.
//! #[derive(Clone, Copy)]
//! struct Versioned<C>(C);
//!
//! const VERSION: u8 = 1;
//!
//! #[derive(Debug)]
//! struct BadVersion(u8);
//!
//! impl fmt::Display for BadVersion {
//!     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         write!(f, "envelope version {} is not the version this build reads", self.0)
//!     }
//! }
//!
//! impl std::error::Error for BadVersion {}
//!
//! impl<C: Codec> Codec for Versioned<C> {
//!     const CONTENT_TYPE: &'static str = C::CONTENT_TYPE;
//!
//!     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
//!         let mut framed = BytesMut::new();
//!         self.encode_into(value, &mut framed)?;
//!         Ok(framed)
//!     }
//!
//!     fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
//!         buf.extend_from_slice(&[VERSION]);
//!         self.0.encode_into(value, buf)
//!     }
//!
//!     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
//!         match bytes {
//!             [VERSION, payload @ ..] => self.0.decode(payload),
//!             [version, ..] => Err(CodecError::Decode(Box::new(BadVersion(*version)))),
//!             [] => Err(CodecError::Decode(Box::new(BadVersion(0)))),
//!         }
//!     }
//! }
//!
//! let codec = Versioned(JsonCodec);
//! let bytes = codec.encode(&7u32).unwrap();
//! assert_eq!(bytes[0], VERSION);
//! let back: u32 = codec.decode(&bytes).unwrap();
//! assert_eq!(back, 7);
//! # }
//! ```
//!
//! [`Codec::encode_into`] is the buffer-taking sibling of `encode`, and the one the publish path
//! calls for a broker that only reads the payload: inside a dispatch loop that buffer is the
//! loop's own, so a codec that writes into it leaves the publish nothing to allocate. It is
//! defaulted (encode into a buffer of its own, then copy), so a codec compiles without it and is
//! faster with it. The wrapper above writes its version byte into the caller's buffer and hands
//! the rest to the inner codec, which is why its `encode` is the one line that calls it.
//!
//! `encode` and `decode` are synchronous, and that is the boundary of what fits: only what a
//! constant and the bytes at hand decide. An integration that needs I/O to serialize (a schema
//! registry, a key service) goes around the codec, on the async edges: the broker's delivery
//! path on the way in, a [`PublishLayer`](crate::runtime::PublishLayer) on the way out.

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
/// only when at least one codec feature is enabled; with none, every call site must name a codec.
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

use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

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
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # fn main() -> Result<(), ruststream::codec::CodecError> {
/// use ruststream::codec::{Codec, JsonCodec};
/// # use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct Order { id: u32, total: f64 }
///
/// let codec = JsonCodec;
/// let bytes = codec.encode(&Order { id: 1, total: 9.99 })?;
/// let back: Order = codec.decode(&bytes)?;
/// assert_eq!(back, Order { id: 1, total: 9.99 });
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "json"))]
/// # fn main() {}
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
    /// # #[cfg(feature = "json")]
    /// # fn main() {
    /// use ruststream::codec::{Codec, JsonCodec};
    ///
    /// assert_eq!(JsonCodec::CONTENT_TYPE, "application/json");
    /// # }
    /// # #[cfg(not(feature = "json"))]
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
    /// # fn main() -> Result<(), ruststream::codec::CodecError> {
    /// use ruststream::BytesMut;
    /// use ruststream::codec::{Codec, JsonCodec};
    ///
    /// let mut buf = BytesMut::new();
    /// JsonCodec.encode_into(&7u8, &mut buf)?;
    /// assert_eq!(&buf[..], b"7");
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "json"))]
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
