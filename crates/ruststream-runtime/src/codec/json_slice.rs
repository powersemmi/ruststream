//! The JSON encoder of a bare-metal target: `serde-json-slice` writing into the codec's buffer.

use bytes::BytesMut;
use serde::Serialize;
use serde_json_slice::EncodeError;

use crate::codec::BufWriter;

/// Appends the JSON of `value` to `buf`, writing each piece where the bytes belong.
///
/// The output is what `serde_json` writes for the same value. The buffer grows as a [`BytesMut`]
/// does, so no write is refused and a failure is the value's own: a `Serialize` impl that errors
/// or a map key JSON cannot write.
pub(crate) fn encode_into<T: Serialize>(value: &T, buf: &mut BytesMut) -> Result<(), EncodeError> {
    serde_json_slice::to_writer(BufWriter(buf), value)
}

#[cfg(test)]
mod tests {
    use alloc::collections::BTreeMap;
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;
    use core::iter::once;
    use serde::ser::{Error as _, Serializer};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::ENCODE_CAPACITY;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        id: u32,
        name: String,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Mode {
        Idle,
        Move(i16, i16),
        Named { label: String },
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Rich {
        flag: bool,
        small: i8,
        big: u64,
        ratio: f64,
        letter: char,
        text: String,
        escaped: String,
        none: Option<u8>,
        some: Option<Sample>,
        list: Vec<u16>,
        modes: Vec<Mode>,
        by_number: BTreeMap<u32, String>,
    }

    fn rich() -> Rich {
        Rich {
            flag: true,
            small: -7,
            big: u64::MAX,
            ratio: 0.1 + 0.2,
            letter: '\u{e9}',
            text: "plain".into(),
            escaped: "quote \" slash \\ tab \t nl \n bell \u{7} snow \u{2603}".into(),
            none: None,
            some: Some(Sample {
                id: 1,
                name: "n".into(),
            }),
            list: alloc::vec![1, 2, 3],
            modes: alloc::vec![
                Mode::Idle,
                Mode::Move(3, -4),
                Mode::Named { label: "x".into() }
            ],
            by_number: [(1, "one".into()), (20, "twenty".into())]
                .into_iter()
                .collect(),
        }
    }

    fn encoded<T: Serialize>(value: &T) -> BytesMut {
        let mut buf = BytesMut::new();
        encode_into(value, &mut buf).unwrap();
        buf
    }

    #[test]
    fn struct_round_trips() {
        let value = Sample {
            id: 7,
            name: "test".into(),
        };
        let back: Sample = serde_json::from_slice(&encoded(&value)).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn char_field_is_a_string() {
        #[derive(Serialize)]
        struct Reading {
            sensor: char,
        }
        assert_eq!(&*encoded(&Reading { sensor: 'A' }), br#"{"sensor":"A"}"#);
    }

    #[test]
    fn numeric_map_key_is_quoted() {
        let map: BTreeMap<u8, bool> = [(1, true), (2, false)].into_iter().collect();
        assert_eq!(&*encoded(&map), br#"{"1":true,"2":false}"#);
    }

    #[test]
    fn output_is_byte_identical_with_serde_json() {
        let value = rich();
        assert_eq!(&*encoded(&value), &*serde_json::to_vec(&value).unwrap());
    }

    #[test]
    fn appends_after_what_the_buffer_holds() {
        let mut buf = BytesMut::from(&b"\x01"[..]);
        encode_into(&7u8, &mut buf).unwrap();
        assert_eq!(&*buf, b"\x017");
    }

    #[test]
    fn message_larger_than_the_first_block_grows_the_buffer() {
        let text = "x".repeat(10 * ENCODE_CAPACITY);
        let mut buf = BytesMut::with_capacity(ENCODE_CAPACITY);
        encode_into(&text, &mut buf).unwrap();
        assert_eq!(buf.len(), text.len() + 2);
        let back: String = serde_json::from_slice(&buf).unwrap();
        assert_eq!(back, text);
    }

    struct Failing;

    impl Serialize for Failing {
        fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("refused"))
        }
    }

    #[test]
    fn error_of_a_user_serialize_impl_surfaces() {
        let mut buf = BytesMut::new();
        let err = encode_into(&Failing, &mut buf).unwrap_err();
        assert_eq!(err.to_string(), "refused");
    }

    #[test]
    fn map_key_json_cannot_write_is_an_error() {
        let map: BTreeMap<Option<u8>, u8> = once((None, 1)).collect();
        let mut buf = BytesMut::new();
        assert!(matches!(
            encode_into(&map, &mut buf),
            Err(EncodeError::KeyMustBeAString)
        ));
    }
}
