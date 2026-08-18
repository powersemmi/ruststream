//! Typed views over [`Headers`]: serialize a flat struct into the header map and parse it back.
//!
//! Header values travel as bytes and are string-encoded by convention, while the Rust side keeps
//! a typed contract: a plain struct whose fields are scalars (numbers, booleans, strings, raw
//! bytes, unit-only enums) or `Option`s of those. [`Headers::insert_typed`] flattens such a
//! struct into `field name -> string value` entries, and [`Headers::to_typed`] parses them back,
//! converting each value by what the target field expects. The same machinery backs the
//! [`FromHeaders`](crate::runtime::FromHeaders) extractor.

mod de;
mod ser;

use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::headers::Headers;

/// Error of [`Headers::to_typed`]: the header map does not satisfy the typed contract.
///
/// Every variant names the offending header, so a failed extraction can be diagnosed from the
/// error alone.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DeserializeHeadersError {
    /// A header value is not valid UTF-8 while the target field expects a textual value.
    #[error("header `{header}` is not valid UTF-8")]
    NotUtf8 {
        /// The offending header name.
        header: String,
    },
    /// A header value does not parse into the target field's type.
    #[error("header `{header}` does not parse as {expected}: `{value}`")]
    Parse {
        /// The offending header name.
        header: String,
        /// The type the target field expects.
        expected: &'static str,
        /// The value as received (lossy UTF-8).
        value: String,
    },
    /// The target type asks for a shape header values cannot carry (a nested struct, a sequence,
    /// a non-unit enum variant).
    #[error("header `{header}` cannot be deserialized as {kind}: header values are scalars")]
    UnsupportedShape {
        /// The offending header name.
        header: String,
        /// The requested shape.
        kind: &'static str,
    },
    /// The top-level type is not a struct or map: there is no header to read a bare scalar from.
    #[error("typed headers deserialize into a struct or map, not {kind}")]
    TopLevel {
        /// The requested top-level shape.
        kind: &'static str,
    },
    /// Any other serde-reported error (a missing field, an unknown enum variant, a custom
    /// `Deserialize` failure).
    #[error("{0}")]
    Message(String),
}

impl serde::de::Error for DeserializeHeadersError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

/// Error of [`Headers::insert_typed`]: the value does not fit the flat header-map shape.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SerializeHeadersError {
    /// A field holds a value header entries cannot carry (a nested struct, a sequence, a map, a
    /// data-carrying enum variant).
    #[error("field `{field}` cannot be serialized into a header value: {kind} is not a scalar")]
    UnsupportedValue {
        /// The offending field name.
        field: String,
        /// The value's shape.
        kind: &'static str,
    },
    /// The top-level value is not a struct or a string-keyed map.
    #[error("typed headers serialize from a struct or map, not {kind}")]
    TopLevel {
        /// The offered top-level shape.
        kind: &'static str,
    },
    /// Any other serde-reported error (a custom `Serialize` failure).
    #[error("{0}")]
    Message(String),
}

impl serde::ser::Error for SerializeHeadersError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

impl Headers {
    /// Parses the header map into a typed contract `T`.
    ///
    /// `T` is a flat struct (or string-keyed map): each field reads the header of the same name,
    /// converting the string-encoded value into what the field expects - integers and floats via
    /// their string form, booleans from `true` / `false`, unit-only enums from the variant name,
    /// byte fields (`serde_bytes`-style) from the raw value. An `Option` field is `None` when the
    /// header is absent; a missing non-`Option` field is an error. Lookups are case-insensitive
    /// like every [`Headers`] read; use `#[serde(rename = "...")]` for wire names that are not
    /// Rust identifiers. `#[serde(flatten)]` is not supported (the flattened shape cannot be
    /// parsed back); a map's keys come back in the header map's normalized lowercase form.
    ///
    /// # Errors
    ///
    /// Returns [`DeserializeHeadersError`] when a required header is missing, a value does not
    /// parse into its field's type, or `T` asks for a shape header values cannot carry (nested
    /// structs, sequences, data-carrying enum variants).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Headers;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct ChunkMeta {
    ///     task_id: u64,
    ///     chunk_no: u32,
    ///     trace: Option<String>,
    /// }
    ///
    /// let mut headers = Headers::new();
    /// headers.insert("task_id", "7");
    /// headers.insert("chunk_no", "3");
    ///
    /// let meta: ChunkMeta = headers.to_typed()?;
    /// assert_eq!(meta.task_id, 7);
    /// assert_eq!(meta.chunk_no, 3);
    /// assert_eq!(meta.trace, None);
    /// # Ok::<(), ruststream::DeserializeHeadersError>(())
    /// ```
    pub fn to_typed<T: DeserializeOwned>(&self) -> Result<T, DeserializeHeadersError> {
        T::deserialize(de::HeadersDeserializer::new(self))
    }

    /// Serializes a typed contract into the header map, one entry per field.
    ///
    /// The mirror of [`to_typed`](Self::to_typed): every field of the flat struct (or
    /// string-keyed map) becomes a header named after the field, with the value string-encoded
    /// (numbers and booleans via their display form, unit-only enums as the variant name, byte
    /// fields as the raw value). An `Option` field that is `None` inserts nothing. Existing
    /// headers under other names are kept; a colliding name is overwritten. Header names
    /// normalize to lowercase on insertion, and `#[serde(flatten)]` is rejected (it could not
    /// be parsed back).
    ///
    /// # Errors
    ///
    /// Returns [`SerializeHeadersError`] when the value is not a struct/map or a field holds a
    /// non-scalar (nested struct, sequence, map, data-carrying enum variant).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Headers;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct ChunkMeta {
    ///     task_id: u64,
    ///     done: bool,
    /// }
    ///
    /// let mut headers = Headers::new();
    /// headers.insert_typed(&ChunkMeta { task_id: 7, done: true })?;
    ///
    /// assert_eq!(headers.get_str("task_id"), Some("7"));
    /// assert_eq!(headers.get_str("done"), Some("true"));
    /// # Ok::<(), ruststream::SerializeHeadersError>(())
    /// ```
    pub fn insert_typed<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), SerializeHeadersError> {
        value.serialize(ser::HeadersSerializer::new(self))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Encoding {
        Binary,
        Json,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Meta {
        task_id: u64,
        ratio: f64,
        active: bool,
        label: String,
        encoding: Encoding,
        note: Option<String>,
        #[serde(
            with = "serde_bytes_shim",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        blob: Option<Vec<u8>>,
    }

    // A tiny stand-in for serde_bytes: forces the bytes path through the value (de)serializer.
    mod serde_bytes_shim {
        use serde::de::{Deserializer, Error as _, Visitor};
        use serde::ser::Serializer;

        // serde's `with` contract passes the field by reference, so `&Option<..>` is forced.
        #[allow(clippy::ref_option)]
        pub(super) fn serialize<S: Serializer>(
            value: &Option<Vec<u8>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match value {
                Some(bytes) => serializer.serialize_bytes(bytes),
                None => serializer.serialize_none(),
            }
        }

        pub(super) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Vec<u8>>, D::Error> {
            struct BytesVisitor;
            impl<'de> Visitor<'de> for BytesVisitor {
                type Value = Option<Vec<u8>>;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("bytes")
                }
                fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                    Ok(Some(v.to_vec()))
                }
                fn visit_some<D2: Deserializer<'de>>(
                    self,
                    d: D2,
                ) -> Result<Self::Value, D2::Error> {
                    d.deserialize_bytes(Self)
                }
                fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                    Ok(None)
                }
            }
            deserializer
                .deserialize_option(BytesVisitor)
                .map_err(|e| D::Error::custom(format!("blob: {e}")))
        }
    }

    fn sample() -> Meta {
        Meta {
            task_id: 7,
            ratio: 0.5,
            active: true,
            label: "movie".to_owned(),
            encoding: Encoding::Json,
            note: None,
            blob: Some(vec![0xff, 0x00]),
        }
    }

    #[test]
    fn round_trips_every_scalar_kind() {
        let mut headers = Headers::new();
        headers.insert_typed(&sample()).expect("flat struct");

        assert_eq!(headers.get_str("task_id"), Some("7"));
        assert_eq!(headers.get_str("ratio"), Some("0.5"));
        assert_eq!(headers.get_str("active"), Some("true"));
        assert_eq!(headers.get_str("label"), Some("movie"));
        assert_eq!(headers.get_str("encoding"), Some("Json"));
        assert!(!headers.contains("note"));
        assert_eq!(headers.get("blob"), Some([0xff, 0x00].as_slice()));

        let back: Meta = headers.to_typed().expect("round trip");
        assert_eq!(back, sample());
    }

    #[test]
    fn missing_required_header_is_an_error_naming_the_field() {
        let mut headers = Headers::new();
        headers.insert("task_id", "7");
        let err = headers.to_typed::<Meta>().expect_err("ratio missing");
        assert!(err.to_string().contains("ratio"), "got: {err}");
    }

    #[test]
    fn unparsable_value_names_header_and_type() {
        #[derive(Debug, Deserialize)]
        struct OnlyId {
            #[allow(dead_code)]
            task_id: u64,
        }
        let mut headers = Headers::new();
        headers.insert("task_id", "not-a-number");
        let err = headers.to_typed::<OnlyId>().expect_err("parse failure");
        let msg = err.to_string();
        assert!(msg.contains("task_id") && msg.contains("u64"), "got: {msg}");
    }

    #[test]
    fn non_utf8_value_for_textual_field_is_an_error() {
        #[derive(Debug, Deserialize)]
        struct OnlyLabel {
            #[allow(dead_code)]
            label: String,
        }
        let mut headers = Headers::new();
        headers.insert("label", Bytes::from_static(&[0xff, 0xfe]));
        let err = headers.to_typed::<OnlyLabel>().expect_err("not utf-8");
        assert!(
            matches!(err, DeserializeHeadersError::NotUtf8 { .. }),
            "got: {err}"
        );
    }

    #[test]
    fn nested_value_is_rejected_on_serialize() {
        #[derive(Serialize)]
        struct Inner {
            x: u8,
        }
        #[derive(Serialize)]
        struct Nested {
            inner: Inner,
        }
        let mut headers = Headers::new();
        let err = headers
            .insert_typed(&Nested {
                inner: Inner { x: 1 },
            })
            .expect_err("nested struct");
        assert!(
            matches!(err, SerializeHeadersError::UnsupportedValue { ref field, .. } if field == "inner"),
            "got: {err}"
        );
    }

    #[test]
    fn flattened_struct_is_rejected_on_serialize() {
        #[derive(Serialize)]
        struct Inner {
            n: u64,
        }
        #[derive(Serialize)]
        struct Flat {
            top: u64,
            #[serde(flatten)]
            rest: Inner,
        }
        let mut headers = Headers::new();
        let err = headers
            .insert_typed(&Flat {
                top: 1,
                rest: Inner { n: 2 },
            })
            .expect_err("flatten cannot round-trip");
        assert!(
            matches!(err, SerializeHeadersError::TopLevel { .. }),
            "got: {err}"
        );
    }

    #[test]
    fn a_custom_serialize_failure_is_reported_as_written() {
        struct Refuses;

        impl Serialize for Refuses {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("this value refuses to travel"))
            }
        }

        let mut headers = Headers::new();
        let err = headers
            .insert_typed(&Refuses)
            .expect_err("the custom failure must surface");
        // A user-written Serialize failure keeps its own wording, so the cause stays findable.
        assert!(err.to_string().contains("this value refuses to travel"));
    }

    #[test]
    fn top_level_scalar_is_rejected_both_ways() {
        let mut headers = Headers::new();
        assert!(matches!(
            headers.insert_typed(&7_u64),
            Err(SerializeHeadersError::TopLevel { .. })
        ));
        assert!(matches!(
            headers.to_typed::<u64>(),
            Err(DeserializeHeadersError::TopLevel { .. })
        ));
    }

    #[test]
    fn string_keyed_map_round_trips() {
        use std::collections::BTreeMap;

        let mut source = BTreeMap::new();
        source.insert("a".to_owned(), "1".to_owned());
        source.insert("b".to_owned(), "two".to_owned());

        let mut headers = Headers::new();
        headers.insert_typed(&source).expect("string map");
        let back: BTreeMap<String, String> = headers.to_typed().expect("map back");
        assert_eq!(back, source);
    }

    #[test]
    fn untagged_union_serializes_as_its_content() {
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Either {
            #[allow(dead_code)]
            Upload { size: u64 },
        }
        let mut headers = Headers::new();
        headers
            .insert_typed(&Either::Upload { size: 9 })
            .expect("untagged");
        assert_eq!(headers.get_str("size"), Some("9"));
    }

    #[test]
    fn renamed_field_reads_dashed_wire_name() {
        #[derive(Serialize, Deserialize)]
        struct Renamed {
            #[serde(rename = "event-class")]
            event_class: String,
        }
        let mut headers = Headers::new();
        headers
            .insert_typed(&Renamed {
                event_class: "upload".to_owned(),
            })
            .expect("renamed");
        assert_eq!(headers.get_str("event-class"), Some("upload"));
        let back: Renamed = headers.to_typed().expect("read back");
        assert_eq!(back.event_class, "upload");
    }
}
