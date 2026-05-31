//! `MessagePack` codec backed by [`rmp_serde`].

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{Codec, CodecError};

/// An `rmp-serde`-based [`Codec`]. Stateless; clone freely.
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgpackCodec;

impl Codec for MsgpackCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, CodecError> {
        rmp_serde::to_vec(value)
            .map(Bytes::from)
            .map_err(|err| CodecError::Encode(Box::new(err)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|err| CodecError::Decode(Box::new(err)))
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
    fn msgpack_roundtrip() {
        let codec = MsgpackCodec;
        let value = Sample {
            id: 9,
            name: "msgpack".into(),
        };
        let bytes = codec.encode(&value).unwrap();
        let back: Sample = codec.decode(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn msgpack_decode_error_surfaces() {
        let codec = MsgpackCodec;
        let err = codec.decode::<Sample>(b"\xc1\x00\x00").unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
