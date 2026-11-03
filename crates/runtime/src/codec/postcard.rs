//! The `postcard` slice codec.

use ruststream_core::codec::{CodecError, CodecFailure};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::SliceCodec;

/// A [`SliceCodec`] over `postcard` (`application/octet-stream`), a compact binary format that
/// needs no allocator. Stateless; clone freely.
///
/// The wire format is `postcard`'s own, so both ends need the same Rust types. A value that does
/// not fit the buffer is [`CodecFailure::BufferFull`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(not(ruststream_alloc), feature = "postcard"))]
/// # mod demo {
/// use ruststream_core::FixedBuf;
/// use ruststream_core::codec::CodecError;
/// use ruststream_runtime::codec::{PostcardCodec, SliceCodec};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Reading {
///     sensor: u8,
///     millivolts: u16,
/// }
///
/// /// What a device puts on the wire, and what the receiving device reads back.
/// pub fn forward(reading: &Reading) -> Result<Reading, CodecError> {
///     let payload: FixedBuf<8> = PostcardCodec.encode(reading)?;
///     PostcardCodec.decode(&payload)
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardCodec;

impl SliceCodec for PostcardCodec {
    type Frame<'a> = ();

    fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<(), CodecError> {
        Ok(())
    }

    fn encode_slice<T: Serialize>(&self, value: &T, out: &mut [u8]) -> Result<usize, CodecError> {
        ::postcard::to_slice(value, out)
            .map(|written| written.len())
            .map_err(|error| CodecError::Encode(encode_failure(&error)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        ::postcard::from_bytes(bytes).map_err(|error| CodecError::Decode(decode_failure(&error)))
    }
}

fn encode_failure(error: &::postcard::Error) -> CodecFailure {
    match error {
        ::postcard::Error::SerializeBufferFull => CodecFailure::BufferFull,
        // A serializer that cannot take this value (an unsized sequence, a custom error): the
        // value is the wrong shape for the format.
        _ => CodecFailure::Type,
    }
}

fn decode_failure(error: &::postcard::Error) -> CodecFailure {
    use ::postcard::Error::{
        DeserializeBadBool, DeserializeBadChar, DeserializeBadEncoding, DeserializeBadEnum,
        DeserializeBadOption, DeserializeBadUtf8, DeserializeBadVarint, DeserializeUnexpectedEnd,
    };
    match error {
        DeserializeBadUtf8 | DeserializeBadChar => CodecFailure::Utf8,
        DeserializeUnexpectedEnd
        | DeserializeBadVarint
        | DeserializeBadBool
        | DeserializeBadOption
        | DeserializeBadEnum
        | DeserializeBadEncoding => CodecFailure::Syntax,
        // The format cannot carry the asked type, or the type's own `Deserialize` refused it.
        _ => CodecFailure::Type,
    }
}

#[cfg(test)]
mod tests {
    use ruststream_core::FixedBuf;
    use ruststream_core::codec::{CodecError, CodecFailure};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::SliceCodec;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: u32,
        paid: bool,
        sku: [u8; 3],
    }

    const ORDER: Order = Order {
        id: 300,
        paid: true,
        sku: *b"abc",
    };

    #[test]
    fn a_value_round_trips_through_a_fixed_buffer() {
        let buf: FixedBuf<16> = PostcardCodec.encode(&ORDER).unwrap();
        // 300 is a two-byte varint, then the bool, then three bytes.
        assert_eq!(buf.as_slice(), &[0xAC, 0x02, 1, b'a', b'b', b'c']);
        assert_eq!(PostcardCodec.decode::<Order>(&buf).unwrap(), ORDER);
    }

    #[test]
    fn a_value_past_the_buffer_is_buffer_full() {
        let error = PostcardCodec.encode::<4, _>(&ORDER).unwrap_err();
        assert!(matches!(
            error,
            CodecError::Encode(CodecFailure::BufferFull)
        ));
    }

    #[test]
    fn a_truncated_document_is_a_syntax_failure() {
        let error = PostcardCodec.decode::<Order>(&[0xAC]).unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Syntax)));
    }

    #[test]
    fn a_bad_string_is_a_utf8_failure() {
        // A character of one byte that is not UTF-8.
        let error = PostcardCodec.decode::<char>(&[1, 0xFF]).unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Utf8)));
    }

    #[test]
    fn a_type_the_format_cannot_read_is_a_type_failure() {
        // `IgnoredAny` asks the deserializer to skip a value of unknown shape, which a format
        // without field tags cannot do.
        let error = PostcardCodec
            .decode::<serde::de::IgnoredAny>(&[1])
            .unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Type)));
    }

    #[test]
    fn a_sequence_of_unknown_length_is_a_type_failure() {
        struct Endless;

        impl Serialize for Endless {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_seq(None)?.end()
            }
        }

        let mut out = [0u8; 8];
        let error = PostcardCodec.encode_slice(&Endless, &mut out).unwrap_err();
        assert!(matches!(error, CodecError::Encode(CodecFailure::Type)));
    }

    #[test]
    fn the_media_type_is_octet_stream() {
        assert_eq!(PostcardCodec::CONTENT_TYPE, "application/octet-stream");
    }
}
