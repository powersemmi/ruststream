//! The codecs: the `Codec` trait, the built-in JSON, CBOR and `MessagePack` codecs and the
//! `DefaultCodec` a mounting that names none uses.
//!
//! A build without `alloc` has `SliceCodec` in place of `Codec` (encode into a slice, decode from
//! one), with the `PostcardCodec` and `JsonCoreCodec` behind the `postcard` and `json-core`
//! features; `ActiveCodec` names whichever the build has.
//!
//! Re-exported as [`ruststream::codec`](https://docs.rs/ruststream/latest/ruststream/codec/).

/// The block an encode buffer starts with, in bytes.
///
/// A serializer writes a document in fragments, so a buffer that starts empty is grown - and
/// reallocated - more than once before even the smallest message is out. This is the capacity
/// `serde_json`'s own `to_vec` reserves, and it puts a single block between a value and the wire
/// at every size: a small document fits the first write, and a larger one grows from here exactly
/// as a `Vec` would.
#[cfg(ruststream_alloc)]
use alloc::boxed::Box;
#[cfg(ruststream_alloc)]
use alloc::string::ToString;
use core::fmt::Display;
#[cfg(not(ruststream_alloc))]
use ruststream_core::codec::CodecFailure;
#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
pub(crate) const ENCODE_CAPACITY: usize = 128;

#[cfg(feature = "json")]
mod json;

#[cfg(not(ruststream_alloc))]
mod slice;

#[cfg(all(not(ruststream_alloc), feature = "postcard"))]
mod postcard;

#[cfg(all(not(ruststream_alloc), feature = "json-core"))]
mod json_core;

// The bare-metal JSON encoder is built on the host under test too, so its tests run there.
#[cfg(all(feature = "json", any(target_os = "none", test)))]
mod json_slice;

#[cfg(feature = "msgpack")]
mod msgpack;

#[cfg(feature = "cbor")]
mod cbor;

#[cfg(feature = "cbor")]
pub use self::cbor::CborCodec;
#[cfg(all(not(ruststream_alloc), feature = "json-core"))]
pub use self::json_core::JsonCoreCodec;
#[cfg(all(not(ruststream_alloc), feature = "postcard"))]
pub use self::postcard::PostcardCodec;
#[cfg(not(ruststream_alloc))]
pub use self::slice::SliceCodec;
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

/// The codec trait of this build: [`Codec`] where `alloc` is on, `SliceCodec` (encode into a
/// slice, decode from one) where it is not. The delivery body names the codec of a mounting
/// through this one name.
#[cfg(ruststream_alloc)]
pub use self::Codec as ActiveCodec;
/// The codec trait of this build: [`Codec`] where `alloc` is on, [`SliceCodec`] (encode into a
/// slice, decode from one) where it is not. The delivery body names the codec of a mounting
/// through this one name.
#[cfg(not(ruststream_alloc))]
pub use self::slice::SliceCodec as ActiveCodec;

// A bare-metal target has no standard library and `rmp-serde` does not build there, so the
// `msgpack` codec is a build error that names the way out.
#[cfg(all(feature = "msgpack", target_os = "none"))]
compile_error!(
    "the `msgpack` codec needs the standard library: `rmp-serde` builds only with `std`. On a \
     target without `std`, select the `cbor` codec instead"
);

// The codecs write through `std::io::Write` on every target that has it, whether or not the
// crate's `std` feature is on: the manifest gives their serializers `std` on those targets.
#[cfg(all(
    not(target_os = "none"),
    any(feature = "json", feature = "msgpack", feature = "cbor")
))]
extern crate std;
#[cfg(all(
    not(target_os = "none"),
    any(feature = "json", feature = "msgpack", feature = "cbor")
))]
use std::io;

#[cfg(all(feature = "cbor", target_os = "none"))]
use core::convert::Infallible;

#[cfg(ruststream_alloc)]
use bytes::BytesMut;
use serde::Serialize;
#[cfg(ruststream_alloc)]
use serde::de::DeserializeOwned;

use ruststream_core::PayloadEncode;
pub use ruststream_core::codec::CodecError;
#[cfg(not(ruststream_alloc))]
use ruststream_core::{EncodeBuf, PayloadBuf};

/// The [`io::Write`] a serializer writes an encode buffer through: every write is an append.
///
/// `bytes`' own writer answers `write_all` with the trait's default loop over `write`, which the
/// optimizer inlines or not depending on what else the binary holds; the difference is several
/// hundred instructions per encoded message. These methods are appends marked for inlining, so
/// the encode path costs the same whatever else changes around it.
#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
pub(crate) struct BufWriter<'a>(pub(crate) &'a mut BytesMut);

#[cfg(all(
    not(target_os = "none"),
    any(feature = "json", feature = "msgpack", feature = "cbor")
))]
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

/// The JSON writer on a bare-metal target: the same append, through the trait `serde-json-slice`
/// writes to. The buffer grows as a `BytesMut` does, so no write is refused.
#[cfg(all(feature = "json", any(target_os = "none", test)))]
impl serde_json_slice::Write for BufWriter<'_> {
    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), serde_json_slice::EncodeError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

/// The CBOR writer on a bare-metal target: the same append, through the trait `ciborium` writes
/// to without `std`.
#[cfg(all(feature = "cbor", target_os = "none"))]
impl ciborium_io::Write for BufWriter<'_> {
    type Error = Infallible;

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<(), Infallible> {
        self.0.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
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
/// [`PublishLayer`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.PublishLayer.html) on the way out.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
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
///     type Frame<'a> = ();
///
///     fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
///         Ok(())
///     }
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
///             b.include(accept)
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
/// /// The bytes behind the version byte, or the error of a frame this build does not read.
/// fn unversioned(bytes: &[u8]) -> Result<&[u8], CodecError> {
///     match bytes {
///         [VERSION, payload @ ..] => Ok(payload),
///         [version, ..] => Err(CodecError::Decode(Box::new(BadVersion(*version)))),
///         [] => Err(CodecError::Decode(Box::new(BadVersion(0)))),
///     }
/// }
///
/// impl<C: Codec> Codec for Versioned<C> {
///     const CONTENT_TYPE: &'static str = C::CONTENT_TYPE;
///
///     // The frame is the wrapped codec's, read behind the version byte.
///     type Frame<'a>
///         = C::Frame<'a>
///     where
///         Self: 'a;
///
///     fn frame<'a>(&'a self, bytes: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
///         self.0.frame(unversioned(bytes)?)
///     }
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
///         self.0.decode(unversioned(bytes)?)
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
#[cfg(ruststream_alloc)]
pub trait Codec: Send + Sync + Clone {
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
    ///     type Frame<'a> = ();
    ///
    ///     fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
    ///         Ok(())
    ///     }
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
    ///             b.include(accept)
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    /// What the codec reads from a frame before decoding it: `()` for a codec that reads none
    /// (JSON, `CBOR`, `MessagePack`), the schema id resolved through the codec's own cache for a
    /// schema-registry codec.
    ///
    /// A message set declared with `#[in_messages(by_codec)]` picks its variant through
    /// [`FrameNames`], which maps this value to a model. A codec that wraps another forwards the
    /// frame of the codec it wraps.
    type Frame<'a>
    where
        Self: 'a;

    /// Encodes `value` into a mutable byte buffer.
    ///
    /// Returning [`BytesMut`] lets the encoded buffer move into the publish pipeline (an
    /// [`Outgoing`](crate::outgoing::Outgoing) payload) without a copy, while still allowing
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

    /// Reads what this codec knows about a frame before decoding it.
    ///
    /// A message set declared with `#[in_messages(by_codec)]` asks the frame which model it
    /// names. Only the payload and the codec's own state decide it, so the read costs a delivery
    /// nothing beyond the codec's own work, and a codec whose frame is `()` never has this called.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when the payload carries no readable frame (a truncated
    /// header, a schema id the codec cannot resolve). The subscriber's decode failure policy
    /// settles the delivery.
    fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError>;
}

/// The codec says which model a frame names: implemented by codecs whose frame names records.
///
/// `#[in_messages(by_codec)]` bounds the codec by this trait once per model of the set, so a set
/// mounts only under a codec that can tell its models apart. A codec that reads no frame (JSON,
/// `CBOR`, `MessagePack`) implements none, and mounting such a set under it is a compile error
/// naming the set, the codec and the model.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::codec::{Codec, CodecError, FrameNames, JsonCodec};
/// use ruststream::memory::prelude::*;
/// use serde::de::DeserializeOwned;
/// use serde::{Deserialize, Serialize};
///
/// /// What the codec reads from a payload: the record the schema id stands for, if known.
/// pub struct Record(Option<&'static str>);
///
/// /// JSON behind a one-byte schema id; the registry the codec holds names the record.
/// #[derive(Debug, Clone, Copy)]
/// pub struct Registry;
///
/// impl Registry {
///     fn record(schema: u8) -> Option<&'static str> {
///         match schema {
///             1 => Some("OrderCreated"),
///             2 => Some("OrderShipped"),
///             _ => None,
///         }
///     }
/// }
///
/// impl Codec for Registry {
///     /// The record the schema id stands for, resolved through the registry.
///     type Frame<'a> = Record;
///
///     fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
///         Ok(Record(payload.first().and_then(|schema| Self::record(*schema))))
///     }
///
///     /// The registry is read here, never written: the producer that owns it frames its records.
///     fn encode<T: Serialize>(&self, _value: &T) -> Result<BytesMut, CodecError> {
///         Err(CodecError::Encode("the registry codec reads records only".into()))
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         JsonCodec.decode(bytes.get(1..).unwrap_or_default())
///     }
/// }
///
/// /// Every model that derives `MessageInfo` is named by its own name.
/// impl<Model: MessageInfo> FrameNames<Model> for Registry {
///     fn names(frame: &Record) -> bool {
///         frame.0 == Some(Model::NAME)
///     }
/// }
///
/// /// An order was placed.
/// #[derive(Deserialize, MessageInfo)]
/// struct OrderCreated {
///     id: u64,
/// }
///
/// /// An order was shipped: the same shape, so only the frame tells the two apart.
/// #[derive(Deserialize, MessageInfo)]
/// struct OrderShipped {
///     id: u64,
/// }
///
/// #[derive(InMessages)]
/// #[in_messages(by_codec)]
/// enum OrderEvent {
///     Created(OrderCreated),
///     Shipped(OrderShipped),
/// }
///
/// #[subscriber("orders")]
/// async fn on_event(event: &OrderEvent) -> HandlerOutcome {
///     match event {
///         OrderEvent::Created(created) => tracing::info!(created.id, "created"),
///         OrderEvent::Shipped(shipped) => tracing::info!(shipped.id, "shipped"),
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
///         MemoryBroker::new(),
///         Registry,
///         |b| {
///             b.include(on_event)
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait FrameNames<Model>: ActiveCodec {
    /// Whether the frame names `Model`.
    fn names(frame: &Self::Frame<'_>) -> bool;
}

/// A value paired with the codec that encodes it, the shape the payload forms encode through.
pub(crate) struct Encoding<'a, C, T> {
    codec: &'a C,
    value: &'a T,
}

impl<'a, C, T> Encoding<'a, C, T> {
    #[inline]
    pub(crate) const fn new(codec: &'a C, value: &'a T) -> Self {
        Self { codec, value }
    }
}

#[cfg(ruststream_alloc)]
impl<C: Codec, T: Serialize> PayloadEncode for Encoding<'_, C, T> {
    #[inline]
    fn encode_into(&self, buf: &mut BytesMut) -> Result<(), CodecError> {
        self.codec.encode_into(self.value, buf)
    }

    #[inline]
    fn encode(&self) -> Result<BytesMut, CodecError> {
        self.codec.encode(self.value)
    }
}

#[cfg(not(ruststream_alloc))]
impl<C: SliceCodec, T: Serialize> PayloadEncode for Encoding<'_, C, T> {
    #[inline]
    fn encode_into(&self, buf: &mut EncodeBuf) -> Result<(), CodecError> {
        buf.write_with(|room| self.codec.encode_slice(self.value, room))
            .map(|_| ())
    }

    #[inline]
    fn encode(&self) -> Result<PayloadBuf, CodecError> {
        self.codec.encode(self.value)
    }
}

/// The decode error of a delivery that cannot be read as the type asked for, for the causes the
/// codecs do not produce themselves (a missing carried value, a header contract that fails).
///
/// With `alloc` the error carries `detail` as its text. Without it the cause is a `Copy` kind,
/// so the error is a type mismatch and the text is dropped.
#[cfg(ruststream_alloc)]
pub(crate) fn decode_error(detail: impl Display) -> CodecError {
    CodecError::Decode(Box::from(detail.to_string()))
}

/// The decode error of a delivery that cannot be read as the type asked for. Without `alloc` the
/// cause is a `Copy` kind: a type mismatch, with no text.
#[cfg(not(ruststream_alloc))]
pub(crate) fn decode_error(detail: impl Display) -> CodecError {
    let _ = detail;
    CodecError::Decode(CodecFailure::Type)
}

/// The encode error of a payload that does not fit the buffer it is written to. With `alloc` a
/// buffer grows, so this is the error of a form that still refuses; without it the cause is
/// `BufferFull`.
#[cfg(ruststream_alloc)]
pub(crate) fn overflow_error() -> CodecError {
    CodecError::Encode(Box::from("the payload does not fit the buffer"))
}

/// The encode error of a payload that does not fit the buffer it is written to: `BufferFull`.
#[cfg(not(ruststream_alloc))]
pub(crate) fn overflow_error() -> CodecError {
    CodecError::Encode(CodecFailure::BufferFull)
}

#[cfg(test)]
mod tests {
    #[cfg(ruststream_alloc)]
    use alloc::string::ToString;

    use super::{CodecError, decode_error, overflow_error};

    #[test]
    fn an_overflow_is_an_encode_error() {
        let err = overflow_error();
        assert!(matches!(err, CodecError::Encode(_)), "{err}");
        #[cfg(ruststream_alloc)]
        assert_eq!(
            err.to_string(),
            "encode failed: the payload does not fit the buffer"
        );
        #[cfg(not(ruststream_alloc))]
        assert!(matches!(
            err,
            CodecError::Encode(ruststream_core::codec::CodecFailure::BufferFull)
        ));
    }

    #[cfg(ruststream_alloc)]
    #[test]
    fn a_decode_error_keeps_the_text_of_its_cause() {
        use alloc::string::ToString;

        let err = decode_error(format_args!("the `{}` header is missing", "event"));
        assert!(matches!(err, CodecError::Decode(_)));
        assert!(
            err.to_string().contains("the `event` header is missing"),
            "{err}"
        );
    }

    #[cfg(not(ruststream_alloc))]
    #[test]
    fn a_decode_error_without_a_heap_is_a_type_mismatch() {
        let err = decode_error(format_args!("the `{}` header is missing", "event"));
        assert!(matches!(
            err,
            CodecError::Decode(ruststream_core::codec::CodecFailure::Type)
        ));
    }
}
