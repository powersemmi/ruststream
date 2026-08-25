//! Pluggable serialization codecs.
//!
//! A [`Codec`] turns Rust values into bytes for the wire and back. Each runtime middleware
//! that needs to materialize typed handler arguments takes a `Codec` by reference; users
//! choose the implementation that matches their broker's payload format.
//!
//! # Cargo features
//!
//! Codecs are additive cargo features: enable only what you need.
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
    /// Encodes `value` into a mutable byte buffer.
    ///
    /// Returning [`BytesMut`] lets the encoded buffer move into the publish pipeline (an
    /// [`Outgoing`](crate::runtime::Outgoing) payload) without a copy, while still allowing
    /// publish middleware to mutate it in place.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the underlying serializer fails.
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError>;

    /// Decodes `bytes` into a Rust value of type `T`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when the underlying deserializer fails.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
