//! JSON codec backed by [`serde_json`], with `serde-json-slice` writing on a bare-metal target.

use alloc::boxed::Box;
use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};

#[cfg(not(target_os = "none"))]
use crate::codec::BufWriter;
#[cfg(target_os = "none")]
use crate::codec::json_slice;
use crate::codec::{Codec, CodecError, ENCODE_CAPACITY};

/// A JSON [`Codec`] (`application/json`). Stateless; clone freely.
///
/// With the standard library, `serde_json` encodes and decodes. On a bare-metal target
/// (`target_os = "none"`) the type is the same and so is the wire format: decoding stays
/// `serde_json` on the allocator, which allocates only scratch for a string with escapes, and
/// encoding goes through `serde-json-slice`, which writes the same bytes as `serde_json` straight
/// into the buffer without an allocation of its own. The buffer grows as a `BytesMut` does, so a
/// message is never cut at a size; a heap that cannot grow it is the allocator's failure, as for
/// the CBOR codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    const CONTENT_TYPE: &'static str = "application/json";

    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        // One buffer, sized once, and no Vec-to-Bytes hop: what a transport that keeps the
        // payload is handed.
        let mut buf = BytesMut::with_capacity(ENCODE_CAPACITY);
        // Written here rather than through `encode_into`: the hop through the trait method costs
        // a taking publish about thirty instructions per message once the two have separate
        // callers, and this is the path every such publish takes.
        #[cfg(not(target_os = "none"))]
        serde_json::to_writer(BufWriter(&mut buf), value)
            .map_err(|err| CodecError::Encode(Box::new(err)))?;
        #[cfg(target_os = "none")]
        json_slice::encode_into(value, &mut buf)
            .map_err(|err| CodecError::Encode(Box::new(err)))?;
        Ok(buf)
    }

    fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Straight into the caller's buffer: a publish that lends the payload reuses the one
        // its dispatch loop holds, so this writes where the bytes already are.
        #[cfg(not(target_os = "none"))]
        let written = serde_json::to_writer(BufWriter(buf), value);
        #[cfg(target_os = "none")]
        let written = json_slice::encode_into(value, buf);
        written.map_err(|err| CodecError::Encode(Box::new(err)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(bytes).map_err(|err| CodecError::Decode(Box::new(err)))
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        id: u32,
        name: String,
    }

    #[test]
    fn json_roundtrip() {
        let codec = JsonCodec;
        let value = Sample {
            id: 7,
            name: "test".into(),
        };
        let bytes = codec.encode(&value).unwrap();
        let back: Sample = codec.decode(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn json_decode_error_surfaces() {
        let codec = JsonCodec;
        let err = codec.decode::<Sample>(b"{ invalid").unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
