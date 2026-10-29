//! What a codec reports when it fails: [`CodecError`].

use std::error::Error as StdError;

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
