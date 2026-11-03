//! [`SliceCodec`]: the codec of a build without `alloc`.

use ruststream_core::codec::CodecError;
use ruststream_core::{FixedBuf, MaybeSendSync};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A serializer that writes into a slice and reads from one: the codec of a build without
/// `alloc`, where [`Codec`](https://docs.rs/ruststream/latest/ruststream/codec/trait.Codec.html)
/// (which hands out a growing `BytesMut`) does not exist.
///
/// The encoder is given the room and reports how much of it the value took, so a message that does
/// not fit is `CodecFailure::BufferFull` and never a reallocation. Implementations are
/// stateless and cheap to clone. `PostcardCodec` and `JsonCoreCodec` are the built-in ones, behind
/// the `postcard` and `json-core` features.
///
/// A service names the codec of its build through [`ActiveCodec`](super::ActiveCodec), which is
/// this trait here and `Codec` where `alloc` is on.
///
/// # Examples
///
/// ```
/// # #[cfg(all(not(ruststream_alloc), feature = "postcard"))]
/// # mod demo {
/// use ruststream_core::FixedBuf;
/// use ruststream_core::codec::{CodecError, CodecFailure};
/// use ruststream_runtime::codec::{PostcardCodec, SliceCodec};
/// use serde::Serialize;
/// use serde::de::DeserializeOwned;
///
/// /// Postcard under a one-byte format version, so a reader can refuse a frame it does not know.
/// #[derive(Clone, Copy)]
/// struct Versioned;
///
/// const VERSION: u8 = 1;
///
/// impl SliceCodec for Versioned {
///     type Frame<'a> = ();
///
///     fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<(), CodecError> {
///         Ok(())
///     }
///
///     fn encode_slice<T: Serialize>(&self, value: &T, out: &mut [u8]) -> Result<usize, CodecError> {
///         let [version, rest @ ..] = out else {
///             return Err(CodecError::Encode(CodecFailure::BufferFull));
///         };
///         *version = VERSION;
///         Ok(1 + PostcardCodec.encode_slice(value, rest)?)
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         match bytes {
///             [VERSION, rest @ ..] => PostcardCodec.decode(rest),
///             _ => Err(CodecError::Decode(CodecFailure::Syntax)),
///         }
///     }
/// }
///
/// pub fn round_trip() -> Result<u32, CodecError> {
///     let payload: FixedBuf<16> = Versioned.encode(&300u32)?;
///     Versioned.decode(&payload)
/// }
/// # }
/// # fn main() {}
/// ```
pub trait SliceCodec: MaybeSendSync + Clone {
    /// The media type of the bytes this codec produces, reported as the `contentType` of every
    /// message it encodes or decodes in the generated `AsyncAPI` document.
    ///
    /// The default is `application/octet-stream`: what a codec whose output carries no media
    /// type of its own should keep.
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    /// What [`frame`](Self::frame) reads of a payload ahead of the decode; `()` for a codec that
    /// reads nothing.
    type Frame<'a>
    where
        Self: 'a;

    /// Reads the part of `payload` a mount site may route on, without decoding the value.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when the payload is not a document of this format.
    fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError>;

    /// Encodes `value` into the start of `out` and returns how many bytes it took.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the value does not encode, with
    /// `CodecFailure::BufferFull` when it does not fit `out`.
    fn encode_slice<T: Serialize>(&self, value: &T, out: &mut [u8]) -> Result<usize, CodecError>;

    /// Decodes a `T` from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Decode`] when `bytes` are not a `T`.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;

    /// Encodes `value` into a buffer of its own, `N` bytes at most: what a transport that keeps
    /// the payload is handed.
    ///
    /// # Errors
    ///
    /// The errors of [`encode_slice`](Self::encode_slice).
    fn encode<const N: usize, T: Serialize>(&self, value: &T) -> Result<FixedBuf<N>, CodecError> {
        let mut buf = FixedBuf::new();
        buf.write_with(|room| self.encode_slice(value, room))?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use ruststream_core::FixedBuf;
    use ruststream_core::codec::{CodecError, CodecFailure};
    use serde::Serialize;
    use serde::de::value::Error as ValueError;
    use serde::de::{DeserializeOwned, IntoDeserializer};

    use super::*;
    use crate::codec::ActiveCodec;

    /// Writes the byte 9 whatever the value is and reads one byte back as a `u8`: the smallest
    /// codec that is a codec.
    #[derive(Clone, Copy)]
    struct NineCodec;

    impl SliceCodec for NineCodec {
        type Frame<'a> = ();

        fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<(), CodecError> {
            Ok(())
        }

        fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
            let [byte] = bytes else {
                return Err(CodecError::Decode(CodecFailure::Syntax));
            };
            T::deserialize(IntoDeserializer::<ValueError>::into_deserializer(*byte))
                .map_err(|_| CodecError::Decode(CodecFailure::Type))
        }

        fn encode_slice<T: Serialize>(
            &self,
            _value: &T,
            out: &mut [u8],
        ) -> Result<usize, CodecError> {
            let [first, ..] = out else {
                return Err(CodecError::Encode(CodecFailure::BufferFull));
            };
            *first = 9;
            Ok(1)
        }
    }

    fn encoded_by<Codec: ActiveCodec>(codec: &Codec) -> Result<FixedBuf<4>, CodecError> {
        codec.encode(&0u8)
    }

    #[test]
    fn encode_lands_in_a_fixed_buffer() {
        let buf = encoded_by(&NineCodec).unwrap();
        assert_eq!(buf.as_slice(), &[9]);
    }

    #[test]
    fn encode_reports_a_buffer_with_no_room() {
        let error = NineCodec.encode::<0, _>(&0u8).unwrap_err();
        assert!(matches!(
            error,
            CodecError::Encode(CodecFailure::BufferFull)
        ));
    }

    #[test]
    fn default_content_type_is_octet_stream() {
        assert_eq!(NineCodec::CONTENT_TYPE, "application/octet-stream");
    }

    #[test]
    fn decode_reads_what_the_codec_wrote() {
        let buf = encoded_by(&NineCodec).unwrap();
        assert_eq!(NineCodec.decode::<u8>(&buf).unwrap(), 9);
    }
}
