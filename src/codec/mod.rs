//! Pluggable serialization codecs.
//!
//! A [`Codec`] turns Rust values into bytes for the wire and back. Each runtime middleware
//! that needs to materialize typed handler arguments takes a `Codec` by reference; users
//! choose the implementation that matches their broker's payload format.
//!
//! # Cargo features
//!
//! Codecs are additive cargo features: enable only what you need. Mutually-exclusive
//! combinations are forbidden by design.
//!
//! * `json` (default): [`JsonCodec`] via `serde_json`.
//! * `msgpack`: [`MsgpackCodec`] via `rmp-serde`.
//! * `cbor`: [`CborCodec`] via `ciborium`.

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

use std::error::Error as StdError;

use bytes::Bytes;
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
/// # #[cfg(feature = "json")] {
/// use ruststream::codec::{Codec, JsonCodec};
/// # use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct Order { id: u32, total: f64 }
///
/// let codec = JsonCodec;
/// let bytes = codec.encode(&Order { id: 1, total: 9.99 }).unwrap();
/// let back: Order = codec.decode(&bytes).unwrap();
/// assert_eq!(back, Order { id: 1, total: 9.99 });
/// # }
/// ```
pub trait Codec: Send + Sync {
    /// Encodes `value` into a byte buffer.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the underlying serializer fails.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, CodecError>;

    /// Decodes `bytes` into a Rust value of type `T`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when the underlying deserializer fails.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
