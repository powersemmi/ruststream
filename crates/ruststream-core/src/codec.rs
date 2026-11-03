//! What a codec reports when it fails: [`CodecError`].

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "alloc")]
use core::error::Error as StdError;

use thiserror::Error;

/// Errors returned by codec implementations.
///
/// With `alloc` the cause is the codec's own error, boxed. Without it the cause is a
/// `CodecFailure`: a codec of that build maps its error onto one of its four kinds.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The codec failed to encode a Rust value into bytes.
    #[cfg(feature = "alloc")]
    #[error("encode failed: {0}")]
    Encode(#[source] Box<dyn StdError + Send + Sync>),

    /// The codec failed to decode bytes into a Rust value.
    #[cfg(feature = "alloc")]
    #[error("decode failed: {0}")]
    Decode(#[source] Box<dyn StdError + Send + Sync>),

    /// The codec failed to encode a Rust value into bytes.
    #[cfg(not(feature = "alloc"))]
    #[error("encode failed: {0}")]
    Encode(#[source] CodecFailure),

    /// The codec failed to decode bytes into a Rust value.
    #[cfg(not(feature = "alloc"))]
    #[error("decode failed: {0}")]
    Decode(#[source] CodecFailure),
}

/// Why a slice codec failed, in the build without `alloc`: a cause that is `Copy`, because
/// there is no heap to box the codec's own error on.
///
/// # Examples
///
/// ```
/// use ruststream_core::codec::{CodecError, CodecFailure};
///
/// // A slice codec reports a message that does not fit the encode buffer like this.
/// let refused = CodecError::Encode(CodecFailure::BufferFull);
/// assert_eq!(refused.to_string(), "encode failed: the encode buffer is full");
/// ```
#[cfg(not(feature = "alloc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[non_exhaustive]
pub enum CodecFailure {
    /// The encoded value does not fit the buffer it was written into.
    #[error("the encode buffer is full")]
    BufferFull,

    /// The bytes are not a well-formed document of the format.
    #[error("the bytes are not well formed")]
    Syntax,

    /// The document is well formed but is not the type asked for.
    #[error("the document does not match the type")]
    Type,

    /// A string in the document is not valid UTF-8.
    #[error("a string is not valid UTF-8")]
    Utf8,
}

#[cfg(all(test, not(feature = "alloc")))]
mod tests {
    use core::error::Error as StdError;

    use super::*;

    #[test]
    fn a_codec_failure_is_a_copy_cause_of_both_directions() {
        let failure = CodecFailure::BufferFull;
        let encode = CodecError::Encode(failure);
        let decode = CodecError::Decode(CodecFailure::Syntax);
        assert_eq!(failure, CodecFailure::BufferFull);
        assert!(matches!(
            encode,
            CodecError::Encode(CodecFailure::BufferFull)
        ));
        assert!(matches!(decode, CodecError::Decode(CodecFailure::Syntax)));
        assert!(decode.source().unwrap().is::<CodecFailure>());
    }

    #[test]
    fn the_failures_are_distinct() {
        let all = [
            CodecFailure::BufferFull,
            CodecFailure::Syntax,
            CodecFailure::Type,
            CodecFailure::Utf8,
        ];
        for (at, left) in all.iter().enumerate() {
            for (other, right) in all.iter().enumerate() {
                assert_eq!(left == right, at == other);
            }
        }
    }
}
