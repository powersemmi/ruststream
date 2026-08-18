//! The serde deserializer behind [`Headers::to_typed`](crate::Headers::to_typed): a flat struct
//! (or string-keyed map) reads one header per field, parsing the string-encoded value into
//! whatever scalar the field expects.

use std::str::FromStr;

use serde::de::value::{BorrowedStrDeserializer, StrDeserializer};
use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, Visitor};
use serde::forward_to_deserialize_any;

use super::DeserializeHeadersError;
use crate::headers::Headers;

/// Top-level deserializer over a borrowed header map.
pub(super) struct HeadersDeserializer<'de> {
    headers: &'de Headers,
}

impl<'de> HeadersDeserializer<'de> {
    pub(super) fn new(headers: &'de Headers) -> Self {
        Self { headers }
    }

    fn top_level(kind: &'static str) -> DeserializeHeadersError {
        DeserializeHeadersError::TopLevel { kind }
    }
}

impl<'de> de::Deserializer<'de> for HeadersDeserializer<'de> {
    type Error = DeserializeHeadersError;

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_map(FieldAccess {
            headers: self.headers,
            fields: fields.iter(),
            pending: None,
        })
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_map(EntryAccess {
            entries: self.headers.iter(),
            pending: None,
        })
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Self::top_level("a self-describing value"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct seq tuple tuple_struct enum identifier
    }
}

/// Struct fields: walks the declared field list and yields only the headers that are present, so
/// `Option` fields default to `None` and serde reports missing required fields by name.
struct FieldAccess<'de> {
    headers: &'de Headers,
    fields: std::slice::Iter<'static, &'static str>,
    pending: Option<(&'static str, &'de [u8])>,
}

impl<'de> MapAccess<'de> for FieldAccess<'de> {
    type Error = DeserializeHeadersError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        for &field in self.fields.by_ref() {
            if let Some(raw) = self.headers.get(field) {
                self.pending = Some((field, raw));
                let key: StrDeserializer<'_, DeserializeHeadersError> = field.into_deserializer();
                return seed.deserialize(key).map(Some);
            }
        }
        Ok(None)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let (header, raw) = self
            .pending
            .take()
            .expect("next_value_seed called before next_key_seed");
        seed.deserialize(ValueDeserializer { header, raw })
    }
}

/// Map entries: yields every header as a `(name, value)` pair.
struct EntryAccess<'de, Entries> {
    entries: Entries,
    pending: Option<(&'de str, &'de [u8])>,
}

impl<'de, Entries> MapAccess<'de> for EntryAccess<'de, Entries>
where
    Entries: Iterator<Item = (&'de str, &'de [u8])>,
{
    type Error = DeserializeHeadersError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        match self.entries.next() {
            Some((name, raw)) => {
                self.pending = Some((name, raw));
                seed.deserialize(BorrowedStrDeserializer::new(name))
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let (header, raw) = self
            .pending
            .take()
            .expect("next_value_seed called before next_key_seed");
        seed.deserialize(ValueDeserializer { header, raw })
    }
}

/// One header value, parsed by what the target field asks for.
struct ValueDeserializer<'de> {
    header: &'de str,
    raw: &'de [u8],
}

impl<'de> ValueDeserializer<'de> {
    fn utf8(&self) -> Result<&'de str, DeserializeHeadersError> {
        std::str::from_utf8(self.raw).map_err(|_| DeserializeHeadersError::NotUtf8 {
            header: self.header.to_owned(),
        })
    }

    fn parse<T: FromStr>(&self, expected: &'static str) -> Result<T, DeserializeHeadersError> {
        self.utf8()?
            .parse()
            .map_err(|_| DeserializeHeadersError::Parse {
                header: self.header.to_owned(),
                expected,
                value: String::from_utf8_lossy(self.raw).into_owned(),
            })
    }

    fn unsupported(&self, kind: &'static str) -> DeserializeHeadersError {
        DeserializeHeadersError::UnsupportedShape {
            header: self.header.to_owned(),
            kind,
        }
    }
}

macro_rules! parse_scalar {
    ($($method:ident => $ty:ty : $visit:ident),* $(,)?) => {
        $(
            fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
                visitor.$visit(self.parse::<$ty>(stringify!($ty))?)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for ValueDeserializer<'de> {
    type Error = DeserializeHeadersError;

    parse_scalar! {
        deserialize_bool => bool: visit_bool,
        deserialize_i8 => i8: visit_i8,
        deserialize_i16 => i16: visit_i16,
        deserialize_i32 => i32: visit_i32,
        deserialize_i64 => i64: visit_i64,
        deserialize_i128 => i128: visit_i128,
        deserialize_u8 => u8: visit_u8,
        deserialize_u16 => u16: visit_u16,
        deserialize_u32 => u32: visit_u32,
        deserialize_u64 => u64: visit_u64,
        deserialize_u128 => u128: visit_u128,
        deserialize_f32 => f32: visit_f32,
        deserialize_f64 => f64: visit_f64,
        deserialize_char => char: visit_char,
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_str(self.utf8()?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_bytes(self.raw)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        // Presence decides: the field access never yields absent headers, so a value that is
        // being deserialized at all is `Some`.
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        // Unit-only enums: the value is the variant name.
        visitor.visit_enum(self.utf8()?.into_deserializer())
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match std::str::from_utf8(self.raw) {
            Ok(s) => visitor.visit_borrowed_str(s),
            Err(_) => visitor.visit_borrowed_bytes(self.raw),
        }
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a unit"))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a unit struct"))
    }

    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a sequence"))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a tuple"))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a tuple struct"))
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a map"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.unsupported("a nested struct"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;

    use bytes::Bytes;
    use serde::de::{DeserializeOwned, Deserializer, IgnoredAny};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Deserialize)]
    struct One<T> {
        field: T,
    }

    #[derive(Debug, Deserialize)]
    struct Unit;

    #[derive(Debug, Deserialize)]
    struct Newtype(u8);

    // Only the shape matters: a tuple struct is rejected before any field is read.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct Pair(u8, u8);

    #[derive(Debug, Deserialize)]
    struct Nested {
        inner: u8,
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    enum Encoding {
        Binary,
    }

    // Asks for whatever the value carries, which is the self-describing path no derive takes.
    #[derive(Debug, PartialEq)]
    enum AnyValue {
        Text(String),
        Bytes(Vec<u8>),
    }

    impl<'de> Deserialize<'de> for AnyValue {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct AnyVisitor;

            impl Visitor<'_> for AnyVisitor {
                type Value = AnyValue;

                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("any header value")
                }

                fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                    Ok(AnyValue::Text(v.to_owned()))
                }

                fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                    Ok(AnyValue::Bytes(v.to_vec()))
                }
            }

            deserializer.deserialize_any(AnyVisitor)
        }
    }

    // Byte fields travel through `deserialize_bytes`, the shape serde_bytes produces.
    #[derive(Debug, PartialEq)]
    struct Blob(Vec<u8>);

    impl<'de> Deserialize<'de> for Blob {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct BlobVisitor;

            impl Visitor<'_> for BlobVisitor {
                type Value = Blob;

                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("raw bytes")
                }

                fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                    Ok(Blob(v.to_vec()))
                }
            }

            deserializer.deserialize_byte_buf(BlobVisitor)
        }
    }

    fn headers_with(value: impl Into<Bytes>) -> Headers {
        let mut headers = Headers::new();
        headers.insert("field", value);
        headers
    }

    fn field<T: DeserializeOwned>(value: &'static str) -> Result<T, DeserializeHeadersError> {
        headers_with(value)
            .to_typed::<One<T>>()
            .map(|one| one.field)
    }

    fn field_err<T: DeserializeOwned + fmt::Debug>(value: &'static str) -> DeserializeHeadersError {
        field::<T>(value).expect_err("value should not satisfy the field's type")
    }

    #[track_caller]
    fn assert_unsupported(error: &DeserializeHeadersError, expected_kind: &str) {
        match error {
            DeserializeHeadersError::UnsupportedShape { header, kind } => {
                assert_eq!(header, "field");
                assert_eq!(*kind, expected_kind);
            }
            other => panic!("expected an unsupported-shape error, got {other:?}"),
        }
    }

    #[test]
    fn every_scalar_field_kind_parses_from_its_string_form() {
        assert!(field::<bool>("true").unwrap());
        assert_eq!(field::<i8>("-8").unwrap(), -8);
        assert_eq!(field::<i16>("-16").unwrap(), -16);
        assert_eq!(field::<i32>("-32").unwrap(), -32);
        assert_eq!(field::<i64>("-64").unwrap(), -64);
        assert_eq!(field::<i128>("-128").unwrap(), -128);
        assert_eq!(field::<u8>("8").unwrap(), 8);
        assert_eq!(field::<u16>("16").unwrap(), 16);
        assert_eq!(field::<u32>("32").unwrap(), 32);
        assert_eq!(field::<u64>("64").unwrap(), 64);
        assert_eq!(field::<u128>("128").unwrap(), 128);
        assert!((field::<f32>("1.5").unwrap() - 1.5).abs() < f32::EPSILON);
        assert!((field::<f64>("2.5").unwrap() - 2.5).abs() < f64::EPSILON);
        assert_eq!(field::<char>("x").unwrap(), 'x');
        assert_eq!(field::<String>("text").unwrap(), "text");
        assert_eq!(field::<Newtype>("7").unwrap().0, 7);
        assert_eq!(field::<Encoding>("Binary").unwrap(), Encoding::Binary);
        assert_eq!(field::<Option<u8>>("9").unwrap(), Some(9));
        assert_eq!(field::<Blob>("raw").unwrap(), Blob(b"raw".to_vec()));
    }

    #[test]
    fn a_value_that_does_not_parse_names_the_header_and_the_expected_type() {
        match field_err::<u8>("not a number") {
            DeserializeHeadersError::Parse {
                header,
                expected,
                value,
            } => {
                assert_eq!(header, "field");
                assert_eq!(expected, "u8");
                assert_eq!(value, "not a number");
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn shapes_a_header_value_cannot_carry_name_the_header() {
        assert_unsupported(&field_err::<()>("x"), "a unit");
        assert_unsupported(&field_err::<Unit>("x"), "a unit struct");
        assert_unsupported(&field_err::<Vec<u8>>("x"), "a sequence");
        assert_unsupported(&field_err::<(u8, u8)>("x"), "a tuple");
        assert_unsupported(&field_err::<Pair>("x"), "a tuple struct");
        assert_unsupported(&field_err::<BTreeMap<String, u8>>("x"), "a map");
        assert_unsupported(&field_err::<Nested>("x"), "a nested struct");
    }

    #[test]
    fn a_self_describing_field_takes_the_value_as_it_travels() {
        assert_eq!(
            field::<AnyValue>("text").unwrap(),
            AnyValue::Text("text".to_owned())
        );

        let one: One<AnyValue> = headers_with(Bytes::from_static(&[0xff, 0xfe]))
            .to_typed()
            .unwrap();
        assert_eq!(one.field, AnyValue::Bytes(vec![0xff, 0xfe]));
    }

    #[test]
    fn unknown_headers_are_ignored_rather_than_read() {
        // `IgnoredAny` is what serde uses for a field it decided to skip, at either level.
        let mut headers = Headers::new();
        headers.insert("field", "value");
        headers.to_typed::<IgnoredAny>().unwrap();

        // IgnoredAny is deliberately zero-sized: it is how serde skips a value it does not want.
        #[allow(clippy::zero_sized_map_values)]
        let map: BTreeMap<String, IgnoredAny> = headers.to_typed().unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn the_top_level_type_may_wrap_the_struct_it_reads() {
        #[derive(Debug, Deserialize)]
        struct Wrapper(Nested);

        let mut headers = Headers::new();
        headers.insert("inner", "3");
        let wrapper: Wrapper = headers.to_typed().unwrap();
        assert_eq!(wrapper.0.inner, 3);
    }

    #[test]
    fn a_map_contract_reads_every_header_as_an_entry() {
        let mut headers = Headers::new();
        headers.insert("first", "1");
        headers.insert("second", "2");

        let map: BTreeMap<String, u8> = headers.to_typed().unwrap();
        assert_eq!(
            map,
            BTreeMap::from([("first".into(), 1), ("second".into(), 2)])
        );
    }
}
