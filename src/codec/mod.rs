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

mod contract;

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

#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
use std::io;

#[cfg(any(feature = "json", feature = "msgpack", feature = "cbor"))]
use bytes::BytesMut;

pub use contract::{Codec, CodecError};

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
