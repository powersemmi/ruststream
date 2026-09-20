//! JSON codec backed by [`serde_json`].

use bytes::{BufMut, BytesMut};
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{Codec, CodecError, ENCODE_CAPACITY};

/// A `serde_json`-based [`Codec`]. Stateless; clone freely.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    const CONTENT_TYPE: &'static str = "application/json";

    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        // One buffer, sized once, and no Vec-to-Bytes hop: what a transport that keeps the
        // payload is handed.
        let mut buf = BytesMut::with_capacity(ENCODE_CAPACITY);
        self.encode_into(value, &mut buf)?;
        Ok(buf)
    }

    fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Straight into the caller's buffer: a publish that lends the payload reuses the one
        // its dispatch loop holds, so this writes where the bytes already are.
        serde_json::to_writer(buf.writer(), value).map_err(|err| CodecError::Encode(Box::new(err)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(bytes).map_err(|err| CodecError::Decode(Box::new(err)))
    }
}

#[cfg(test)]
mod tests {
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
