//! CBOR codec backed by [`ciborium`].

use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{BufWriter, Codec, CodecError, ENCODE_CAPACITY};

/// A `ciborium`-based [`Codec`]. Stateless; clone freely.
#[derive(Debug, Clone, Copy, Default)]
pub struct CborCodec;

impl Codec for CborCodec {
    const CONTENT_TYPE: &'static str = "application/cbor";

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
        ciborium::into_writer(value, BufWriter(buf))
            .map_err(|err| CodecError::Encode(Box::new(err)))
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
