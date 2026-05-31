//! CBOR codec backed by [`ciborium`].

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{Codec, CodecError};

/// A `ciborium`-based [`Codec`]. Stateless; clone freely.
#[derive(Debug, Clone, Copy, Default)]
pub struct CborCodec;

impl Codec for CborCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, CodecError> {
        let mut buf: Vec<u8> = Vec::new();
        ciborium::into_writer(value, &mut buf).map_err(|err| CodecError::Encode(Box::new(err)))?;
        Ok(Bytes::from(buf))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        ciborium::from_reader(bytes).map_err(|err| CodecError::Decode(Box::new(err)))
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
    fn cbor_roundtrip() {
        let codec = CborCodec;
        let value = Sample {
            id: 12,
            name: "cbor".into(),
        };
        let bytes = codec.encode(&value).unwrap();
        let back: Sample = codec.decode(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn cbor_decode_error_surfaces() {
        let codec = CborCodec;
        // 0xff is the CBOR "break" code; not a complete top-level item.
        let err = codec.decode::<Sample>(b"\xff").unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
