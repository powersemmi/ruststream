//! JSON codec backed by [`serde_json`].

use bytes::{BufMut, BytesMut};
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{Codec, CodecError};

/// A `serde_json`-based [`Codec`]. Stateless; clone freely.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        // Serialize straight into the BytesMut writer: one buffer, no Vec-to-Bytes hop.
        let mut buf = BytesMut::new();
        serde_json::to_writer((&mut buf).writer(), value)
            .map_err(|err| CodecError::Encode(Box::new(err)))?;
        Ok(buf)
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
