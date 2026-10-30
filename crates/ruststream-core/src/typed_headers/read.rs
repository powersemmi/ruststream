//! The error of [`HeaderStore::read_typed`], which borrows what it reports from the store.

use core::fmt::{self, Display, Write as _};

use serde::de::Error as DeError;
use thiserror::Error;

use super::de::ReadError;
#[cfg(doc)]
use crate::header_store::HeaderStore;

/// Error of [`HeaderStore::read_typed`]: the headers do not satisfy the typed contract.
///
/// It borrows the offending header's name and value from the store, so it holds no heap and
/// lives as long as the borrow of the store.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, HeaderStore, ReadHeadersError};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Meta {
///     seq: u32,
/// }
///
/// /// A device handler: a frame whose `seq` header is missing or broken is dropped and logged
/// /// with the header that broke it.
/// fn accept(headers: &FixedHeaderMap<4, 64>) -> Option<u32> {
///     match headers.read_typed::<Meta>() {
///         Ok(meta) => Some(meta.seq),
///         Err(ReadHeadersError::Parse { header, .. }) => {
///             tracing::warn!(header, "a header does not parse; frame dropped");
///             None
///         }
///         Err(error) => {
///             tracing::warn!(%error, "frame dropped");
///             None
///         }
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadHeadersError<'de> {
    /// A header value is not valid UTF-8 while the target field expects a textual value.
    #[error("header `{header}` is not valid UTF-8")]
    NotUtf8 {
        /// The offending header name.
        header: &'de str,
    },
    /// A header value does not parse into the target field's type.
    #[error("header `{header}` does not parse as {expected}: `{}`", Lossy(value))]
    Parse {
        /// The offending header name.
        header: &'de str,
        /// The type the target field expects.
        expected: &'static str,
        /// The value as received.
        value: &'de [u8],
    },
    /// The target type asks for a shape header values cannot carry (a nested struct, a sequence,
    /// a non-unit enum variant).
    #[error("header `{header}` cannot be deserialized as {kind}: header values are scalars")]
    UnsupportedShape {
        /// The offending header name.
        header: &'de str,
        /// The requested shape.
        kind: &'static str,
    },
    /// The top-level type is not a struct or map: there is no header to read a bare scalar from.
    #[error("typed headers deserialize into a struct or map, not {kind}")]
    TopLevel {
        /// The requested top-level shape.
        kind: &'static str,
    },
    /// A required header is absent.
    #[error("missing header `{0}`")]
    MissingField(&'static str),
    /// The contract's own `Deserialize` rejected a value (an unknown enum variant, a check of its
    /// own).
    #[error("a header value is rejected by the contract")]
    Rejected,
}

impl DeError for ReadHeadersError<'_> {
    fn custom<T: Display>(_msg: T) -> Self {
        Self::Rejected
    }

    fn missing_field(field: &'static str) -> Self {
        Self::MissingField(field)
    }
}

impl<'de> ReadError<'de> for ReadHeadersError<'de> {
    fn not_utf8(header: &'de str) -> Self {
        Self::NotUtf8 { header }
    }

    fn parse(header: &'de str, expected: &'static str, value: &'de [u8]) -> Self {
        Self::Parse {
            header,
            expected,
            value,
        }
    }

    fn unsupported_shape(header: &'de str, kind: &'static str) -> Self {
        Self::UnsupportedShape { header, kind }
    }

    fn top_level(kind: &'static str) -> Self {
        Self::TopLevel { kind }
    }
}

/// Bytes shown as text, each invalid sequence as U+FFFD, without copying them anywhere.
struct Lossy<'a>(&'a [u8]);

impl Display for Lossy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for chunk in self.0.utf8_chunks() {
            f.write_str(chunk.valid())?;
            if !chunk.invalid().is_empty() {
                f.write_char(char::REPLACEMENT_CHARACTER)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "heapless")]
mod tests {
    use core::ptr;
    use std::collections::BTreeMap;
    use std::string::{String, ToString};
    use std::vec::Vec;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::fixed_headers::FixedHeaderMap;
    use crate::header_store::{HeaderStore, HeaderStoreMut};

    type Store = FixedHeaderMap<16, 512>;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Mode {
        Burst,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Contract<'a> {
        device: &'a str,
        #[serde(with = "serde_bytes_like")]
        raw: &'a [u8],
        seq: u32,
        delta: i64,
        wide: u128,
        ratio: f64,
        huge: f64,
        ok: bool,
        mark: char,
        mode: Mode,
        trace: Option<&'a str>,
        absent: Option<u8>,
    }

    /// Byte fields travel through `serialize_bytes` / `deserialize_bytes`, as `serde_bytes`
    /// drives them.
    mod serde_bytes_like {
        use serde::{Deserializer, Serializer};

        pub(super) fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(v)
        }

        pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<&'de [u8], D::Error> {
            <&[u8]>::deserialize(d)
        }

        use serde::Deserialize;
    }

    fn contract() -> Contract<'static> {
        Contract {
            device: "sensor-7",
            raw: &[0xff, 0x00],
            seq: 42,
            delta: -3,
            wide: u128::MAX,
            ratio: 0.25,
            huge: 1e300,
            ok: true,
            mark: 'z',
            mode: Mode::Burst,
            trace: Some("abc"),
            absent: None,
        }
    }

    #[test]
    fn a_contract_round_trips_borrowing_from_the_store() {
        let mut headers = Store::new();
        headers.write_typed(&contract()).unwrap();

        assert_eq!(headers.get_str("seq"), Some("42"));
        assert_eq!(headers.get_str("delta"), Some("-3"));
        assert_eq!(headers.get_str("ok"), Some("true"));
        assert_eq!(headers.get_str("mode"), Some("Burst"));
        assert_eq!(headers.get_str("ratio"), Some("0.25"));
        // Too long for its display form, so it travels in its exponent form.
        assert_eq!(headers.get_str("huge"), Some("1e300"));
        assert!(!headers.contains("absent"));

        let back: Contract<'_> = headers.read_typed().unwrap();
        assert_eq!(back, contract());
        assert!(ptr::eq(
            back.device.as_ptr(),
            headers.get("device").unwrap().as_ptr()
        ));
    }

    #[test]
    fn a_missing_required_header_names_the_field() {
        #[derive(Debug, Deserialize)]
        struct Meta {
            #[allow(dead_code)]
            seq: u32,
        }
        let headers = Store::new();
        let err = headers.read_typed::<Meta>().unwrap_err();
        assert_eq!(err, ReadHeadersError::MissingField("seq"));
        assert_eq!(err.to_string(), "missing header `seq`");
    }

    #[derive(Debug, Deserialize)]
    struct One<T> {
        #[allow(dead_code)]
        field: T,
    }

    fn read_err<T: for<'de> Deserialize<'de> + fmt::Debug>(value: &[u8]) -> String {
        let mut headers = Store::new();
        headers.insert("field", value).unwrap();
        let err = headers.read_typed::<One<T>>().unwrap_err();
        err.to_string()
    }

    #[test]
    fn a_rejected_value_borrows_its_header_into_the_error() {
        assert_eq!(
            read_err::<u8>(b"300\xff"),
            "header `field` is not valid UTF-8"
        );
        assert_eq!(
            read_err::<u8>(b"300"),
            "header `field` does not parse as u8: `300`"
        );
        assert_eq!(
            read_err::<Vec<u8>>(b"1"),
            "header `field` cannot be deserialized as a sequence: header values are scalars"
        );
        assert_eq!(
            read_err::<Mode>(b"Idle"),
            "a header value is rejected by the contract"
        );
        let empty = Store::new();
        assert_eq!(
            empty.read_typed::<u8>().unwrap_err().to_string(),
            "typed headers deserialize into a struct or map, not a self-describing value"
        );

        let mut headers = Store::new();
        headers.insert("field", b"x\xffy").unwrap();
        let err = headers.read_typed::<One<u8>>();
        // Bytes that are not UTF-8 show as replacement characters, and nothing is copied.
        assert_eq!(
            ReadHeadersError::Parse {
                header: "field",
                expected: "u8",
                value: b"x\xffy",
            }
            .to_string(),
            "header `field` does not parse as u8: `x\u{fffd}y`"
        );
        assert!(err.is_err());
    }

    #[test]
    fn a_map_contract_reads_every_header_as_an_entry() {
        let mut headers = Store::new();
        headers.insert("first", b"1").unwrap();
        headers.insert("second", b"2").unwrap();
        let map: BTreeMap<&str, u8> = headers.read_typed().unwrap();
        assert_eq!(map, BTreeMap::from([("first", 1), ("second", 2)]));
    }
}
