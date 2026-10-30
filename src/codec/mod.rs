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

#[doc(inline)]
pub use ruststream_runtime::codec::*;
