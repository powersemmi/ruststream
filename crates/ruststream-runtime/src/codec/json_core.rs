//! The `serde-json-core` slice codec.

use ruststream_core::codec::{CodecError, CodecFailure};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json_core::de::Error as DecodeError;

use super::SliceCodec;

/// A [`SliceCodec`] over `serde-json-core` (`application/json`), JSON without an allocator.
/// Stateless; clone freely.
///
/// The bytes are the same JSON `serde_json` writes. The decoder reads borrowed strings and does
/// not unescape them, so a message type of this build holds `&str`-free data: numbers, booleans,
/// fixed arrays and fixed-capacity strings.
///
/// # Examples
///
/// ```
/// # #[cfg(all(not(ruststream_alloc), feature = "json-core"))]
/// # mod demo {
/// use ruststream_core::FixedBuf;
/// use ruststream_core::codec::CodecError;
/// use ruststream_runtime::codec::{JsonCoreCodec, SliceCodec};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Reading {
///     sensor: u8,
///     millivolts: u16,
/// }
///
/// /// The document a gateway reads: `{"sensor":3,"millivolts":3300}`.
/// pub fn forward(reading: &Reading) -> Result<Reading, CodecError> {
///     let payload: FixedBuf<40> = JsonCoreCodec.encode(reading)?;
///     JsonCoreCodec.decode(&payload)
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCoreCodec;

impl SliceCodec for JsonCoreCodec {
    const CONTENT_TYPE: &'static str = "application/json";
    type Frame<'a> = ();

    fn frame<'a>(&'a self, _payload: &'a [u8]) -> Result<(), CodecError> {
        Ok(())
    }

    fn encode_slice<T: Serialize>(&self, value: &T, out: &mut [u8]) -> Result<usize, CodecError> {
        // The serializer's only failure is a full buffer.
        serde_json_core::to_slice(value, out)
            .map_err(|_| CodecError::Encode(CodecFailure::BufferFull))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json_core::from_slice(bytes)
            .map(|(value, _used)| value)
            .map_err(|error| CodecError::Decode(decode_failure(error)))
    }
}

fn decode_failure(error: DecodeError) -> CodecFailure {
    match error {
        DecodeError::InvalidUnicodeCodePoint => CodecFailure::Utf8,
        DecodeError::InvalidType
        | DecodeError::BytesIsUnsupported
        | DecodeError::AnyIsUnsupported
        | DecodeError::CustomError => CodecFailure::Type,
        // Every other failure is the text not being JSON: an end too early, a missing colon, a
        // trailing character, a bad number or escape.
        _ => CodecFailure::Syntax,
    }
}

#[cfg(test)]
mod tests {
    use ruststream_core::FixedBuf;
    use ruststream_core::codec::{CodecError, CodecFailure};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::SliceCodec;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: u32,
        paid: bool,
    }

    const ORDER: Order = Order { id: 7, paid: true };

    #[test]
    fn a_value_round_trips_as_json() {
        let buf: FixedBuf<32> = JsonCoreCodec.encode(&ORDER).unwrap();
        assert_eq!(buf.as_slice(), br#"{"id":7,"paid":true}"#);
        assert_eq!(JsonCoreCodec.decode::<Order>(&buf).unwrap(), ORDER);
    }

    #[test]
    fn a_value_past_the_buffer_is_buffer_full() {
        let error = JsonCoreCodec.encode::<8, _>(&ORDER).unwrap_err();
        assert!(matches!(
            error,
            CodecError::Encode(CodecFailure::BufferFull)
        ));
    }

    #[test]
    fn broken_json_is_a_syntax_failure() {
        let error = JsonCoreCodec.decode::<Order>(b"{ invalid").unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Syntax)));
    }

    #[test]
    fn a_wrong_type_is_a_type_failure() {
        let error = JsonCoreCodec
            .decode::<Order>(br#"{"id":"x","paid":true}"#)
            .unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Type)));
    }

    #[test]
    fn a_missing_field_is_a_type_failure() {
        let error = JsonCoreCodec.decode::<Order>(br#"{"id":7}"#).unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Type)));
    }

    #[test]
    fn a_bad_code_point_is_a_utf8_failure() {
        let error = JsonCoreCodec
            .decode::<serde::de::IgnoredAny>(b"\"\xFF\"")
            .unwrap_err();
        assert!(matches!(error, CodecError::Decode(CodecFailure::Utf8)));
    }

    #[test]
    fn the_media_type_is_json() {
        assert_eq!(JsonCoreCodec::CONTENT_TYPE, "application/json");
    }
}
