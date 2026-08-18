//! The serde serializer behind [`Headers::insert_typed`](crate::Headers::insert_typed): each
//! field of a flat struct (or string-keyed map) becomes one header entry with a string-encoded
//! value.

use bytes::Bytes;
use serde::ser::{
    self, Impossible, Serialize, SerializeMap, SerializeStruct, SerializeStructVariant,
};

use super::SerializeHeadersError;
use crate::headers::Headers;

/// Top-level serializer writing into a header map.
pub(super) struct HeadersSerializer<'a> {
    headers: &'a mut Headers,
}

impl<'a> HeadersSerializer<'a> {
    pub(super) fn new(headers: &'a mut Headers) -> Self {
        Self { headers }
    }
}

fn top_level(kind: &'static str) -> SerializeHeadersError {
    SerializeHeadersError::TopLevel { kind }
}

macro_rules! reject_top_level {
    ($($method:ident($($arg:ty),*) => $kind:literal),* $(,)?) => {
        $(
            fn $method(self, $(_: $arg),*) -> Result<Self::Ok, Self::Error> {
                Err(top_level($kind))
            }
        )*
    };
}

impl<'a> ser::Serializer for HeadersSerializer<'a> {
    type Ok = ();
    type Error = SerializeHeadersError;
    type SerializeSeq = Impossible<(), SerializeHeadersError>;
    type SerializeTuple = Impossible<(), SerializeHeadersError>;
    type SerializeTupleStruct = Impossible<(), SerializeHeadersError>;
    type SerializeTupleVariant = Impossible<(), SerializeHeadersError>;
    type SerializeMap = EntrySink<'a>;
    type SerializeStruct = FieldSink<'a>;
    type SerializeStructVariant = FieldSink<'a>;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(FieldSink {
            headers: self.headers,
        })
    }

    // An untagged enum serializes as its content; a struct variant lands here and stays flat.
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(FieldSink {
            headers: self.headers,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        // A map of unknown size is how `#[serde(flatten)]` drives a serializer; flattened
        // structs cannot be read back (serde buffers them through erased values on the
        // deserialize side), so rejecting them here keeps insert_typed and to_typed symmetric.
        if len.is_none() {
            return Err(top_level(
                "a map of unknown size (#[serde(flatten)] is not supported)",
            ));
        }
        Ok(EntrySink {
            headers: self.headers,
            key: None,
        })
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        // The untagged-union shape: the variant's single field is itself the flat struct.
        value.serialize(self)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(top_level("a unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(top_level("a unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(top_level("a unit variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(top_level("a sequence"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(top_level("a tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(top_level("a tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(top_level("a tuple variant"))
    }

    reject_top_level! {
        serialize_bool(bool) => "a boolean",
        serialize_i8(i8) => "an integer",
        serialize_i16(i16) => "an integer",
        serialize_i32(i32) => "an integer",
        serialize_i64(i64) => "an integer",
        serialize_i128(i128) => "an integer",
        serialize_u8(u8) => "an integer",
        serialize_u16(u16) => "an integer",
        serialize_u32(u32) => "an integer",
        serialize_u64(u64) => "an integer",
        serialize_u128(u128) => "an integer",
        serialize_f32(f32) => "a float",
        serialize_f64(f64) => "a float",
        serialize_char(char) => "a character",
        serialize_str(&str) => "a string",
        serialize_bytes(&[u8]) => "bytes",
    }
}

/// Struct fields: one header per field, skipped when the value serializer yields nothing.
pub(super) struct FieldSink<'a> {
    headers: &'a mut Headers,
}

impl SerializeStruct for FieldSink<'_> {
    type Ok = ();
    type Error = SerializeHeadersError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        if let Some(bytes) = value.serialize(ValueSerializer { field: key })? {
            self.headers.insert(key, bytes);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for FieldSink<'_> {
    type Ok = ();
    type Error = SerializeHeadersError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

/// Map entries: string keys become header names.
pub(super) struct EntrySink<'a> {
    headers: &'a mut Headers,
    key: Option<String>,
}

impl SerializeMap for EntrySink<'_> {
    type Ok = ();
    type Error = SerializeHeadersError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let key = self
            .key
            .take()
            .expect("serialize_value called before serialize_key");
        if let Some(bytes) = value.serialize(ValueSerializer { field: &key })? {
            self.headers.insert(key, bytes);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

/// One field value, string-encoded. `Ok` is `None` for `Option::None` (the header is skipped).
struct ValueSerializer<'k> {
    field: &'k str,
}

impl ValueSerializer<'_> {
    fn unsupported(&self, kind: &'static str) -> SerializeHeadersError {
        SerializeHeadersError::UnsupportedValue {
            field: self.field.to_owned(),
            kind,
        }
    }
}

macro_rules! display_scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method(self, v: $ty) -> Result<Self::Ok, Self::Error> {
                Ok(Some(Bytes::from(v.to_string())))
            }
        )*
    };
}

impl ser::Serializer for ValueSerializer<'_> {
    type Ok = Option<Bytes>;
    type Error = SerializeHeadersError;
    type SerializeSeq = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeTuple = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeTupleStruct = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeTupleVariant = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeMap = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeStruct = Impossible<Option<Bytes>, SerializeHeadersError>;
    type SerializeStructVariant = Impossible<Option<Bytes>, SerializeHeadersError>;

    display_scalar! {
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Bytes::copy_from_slice(v.as_bytes())))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Bytes::copy_from_slice(v)))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(None)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Bytes::from_static(variant.as_bytes())))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(self.unsupported("a unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(self.unsupported("a unit struct"))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(self.unsupported("a data-carrying enum variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(self.unsupported("a sequence"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(self.unsupported("a tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(self.unsupported("a tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(self.unsupported("a tuple variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(self.unsupported("a map"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(self.unsupported("a nested struct"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(self.unsupported("a struct variant"))
    }
}

/// Map keys must be strings.
struct KeySerializer;

impl ser::Serializer for KeySerializer {
    type Ok = String;
    type Error = SerializeHeadersError;
    type SerializeSeq = Impossible<String, SerializeHeadersError>;
    type SerializeTuple = Impossible<String, SerializeHeadersError>;
    type SerializeTupleStruct = Impossible<String, SerializeHeadersError>;
    type SerializeTupleVariant = Impossible<String, SerializeHeadersError>;
    type SerializeMap = Impossible<String, SerializeHeadersError>;
    type SerializeStruct = Impossible<String, SerializeHeadersError>;
    type SerializeStructVariant = Impossible<String, SerializeHeadersError>;

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_owned())
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_i128(self, _v: i128) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_u128(self, _v: u128) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(variant.to_owned())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(non_string_key())
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(non_string_key())
    }
}

fn non_string_key() -> SerializeHeadersError {
    SerializeHeadersError::Message("header names must serialize as strings".to_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Serialize;
    use serde::ser::{SerializeMap, Serializer};

    use super::*;

    #[derive(Serialize)]
    struct One<T> {
        field: T,
    }

    #[derive(Serialize)]
    struct Unit;

    #[derive(Serialize)]
    struct Newtype(u8);

    #[derive(Serialize)]
    struct Pair(u8, u8);

    #[derive(Serialize)]
    struct Nested {
        inner: u8,
    }

    // One enum per variant shape a value serializer can meet.
    #[derive(Serialize)]
    enum Shape {
        Bare,
        Wrapping(u8),
        Pair(u8, u8),
        Fields { a: u8 },
    }

    #[derive(Serialize)]
    enum TaggedNewtype {
        Meta(Nested),
    }

    #[derive(Serialize)]
    enum TaggedFields {
        Meta { inner: u8 },
    }

    // Serializes as raw bytes, the one shape no derive produces (serde_bytes territory).
    struct RawBytes(&'static [u8]);

    impl Serialize for RawBytes {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }

    // A one-entry map whose key is the tested value: drives the key serializer directly, since a
    // derived type can only ever offer a string key.
    struct KeyedMap<K>(K);

    impl<K: Serialize> Serialize for KeyedMap<K> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry(&self.0, "value")?;
            map.end()
        }
    }

    fn field<T: Serialize>(value: T) -> Result<Option<String>, SerializeHeadersError> {
        let mut headers = Headers::new();
        headers.insert_typed(&One { field: value })?;
        Ok(headers.get_str("field").map(str::to_owned))
    }

    fn field_err<T: Serialize>(value: T) -> SerializeHeadersError {
        field(value).expect_err("value should not fit a header entry")
    }

    fn top_level_err<T: Serialize>(value: T) -> SerializeHeadersError {
        let mut headers = Headers::new();
        headers
            .insert_typed(&value)
            .expect_err("value should not serialize into a header map")
    }

    fn key_err<K: Serialize>(key: K) -> SerializeHeadersError {
        let mut headers = Headers::new();
        headers
            .insert_typed(&KeyedMap(key))
            .expect_err("key should not serialize into a header name")
    }

    #[track_caller]
    fn assert_unsupported(error: &SerializeHeadersError, expected_kind: &str) {
        match error {
            SerializeHeadersError::UnsupportedValue { field, kind } => {
                assert_eq!(field, "field");
                assert_eq!(*kind, expected_kind);
            }
            other => panic!("expected an unsupported-value error, got {other:?}"),
        }
    }

    #[track_caller]
    fn assert_top_level(error: &SerializeHeadersError, expected_kind: &str) {
        match error {
            SerializeHeadersError::TopLevel { kind } => assert_eq!(*kind, expected_kind),
            other => panic!("expected a top-level error, got {other:?}"),
        }
    }

    #[track_caller]
    fn assert_non_string_key(error: &SerializeHeadersError) {
        match error {
            SerializeHeadersError::Message(message) => {
                assert!(message.contains("header names must serialize as strings"));
            }
            other => panic!("expected a non-string-key error, got {other:?}"),
        }
    }

    #[test]
    fn every_scalar_field_kind_serializes_to_its_display_form() {
        assert_eq!(field(true).unwrap().as_deref(), Some("true"));
        assert_eq!(field(-8i8).unwrap().as_deref(), Some("-8"));
        assert_eq!(field(-16i16).unwrap().as_deref(), Some("-16"));
        assert_eq!(field(-32i32).unwrap().as_deref(), Some("-32"));
        assert_eq!(field(-64i64).unwrap().as_deref(), Some("-64"));
        assert_eq!(field(-128i128).unwrap().as_deref(), Some("-128"));
        assert_eq!(field(8u8).unwrap().as_deref(), Some("8"));
        assert_eq!(field(16u16).unwrap().as_deref(), Some("16"));
        assert_eq!(field(32u32).unwrap().as_deref(), Some("32"));
        assert_eq!(field(64u64).unwrap().as_deref(), Some("64"));
        assert_eq!(field(128u128).unwrap().as_deref(), Some("128"));
        assert_eq!(field(1.5f32).unwrap().as_deref(), Some("1.5"));
        assert_eq!(field(2.5f64).unwrap().as_deref(), Some("2.5"));
        assert_eq!(field('x').unwrap().as_deref(), Some("x"));
        assert_eq!(field("text").unwrap().as_deref(), Some("text"));
        assert_eq!(field(RawBytes(b"raw")).unwrap().as_deref(), Some("raw"));
        assert_eq!(field(Newtype(7)).unwrap().as_deref(), Some("7"));
        assert_eq!(field(Shape::Bare).unwrap().as_deref(), Some("Bare"));
        assert_eq!(field(Some(9u8)).unwrap().as_deref(), Some("9"));
        assert_eq!(field(Option::<u8>::None).unwrap(), None);
    }

    #[test]
    fn non_scalar_field_values_name_the_field_and_the_shape() {
        assert_unsupported(&field_err(()), "a unit");
        assert_unsupported(&field_err(Unit), "a unit struct");
        assert_unsupported(
            &field_err(Shape::Wrapping(1)),
            "a data-carrying enum variant",
        );
        assert_unsupported(&field_err(vec![1u8]), "a sequence");
        assert_unsupported(&field_err((1u8, 2u8)), "a tuple");
        assert_unsupported(&field_err(Pair(1, 2)), "a tuple struct");
        assert_unsupported(&field_err(Shape::Pair(1, 2)), "a tuple variant");
        assert_unsupported(&field_err(BTreeMap::from([("k", "v")])), "a map");
        assert_unsupported(&field_err(Nested { inner: 1 }), "a nested struct");
        assert_unsupported(&field_err(Shape::Fields { a: 1 }), "a struct variant");
    }

    #[test]
    fn top_level_non_struct_values_are_rejected_by_shape() {
        assert_top_level(&top_level_err(true), "a boolean");
        assert_top_level(&top_level_err(-8i8), "an integer");
        assert_top_level(&top_level_err(-16i16), "an integer");
        assert_top_level(&top_level_err(-32i32), "an integer");
        assert_top_level(&top_level_err(-64i64), "an integer");
        assert_top_level(&top_level_err(-128i128), "an integer");
        assert_top_level(&top_level_err(8u8), "an integer");
        assert_top_level(&top_level_err(16u16), "an integer");
        assert_top_level(&top_level_err(32u32), "an integer");
        assert_top_level(&top_level_err(64u64), "an integer");
        assert_top_level(&top_level_err(128u128), "an integer");
        assert_top_level(&top_level_err(1.5f32), "a float");
        assert_top_level(&top_level_err(2.5f64), "a float");
        assert_top_level(&top_level_err('x'), "a character");
        assert_top_level(&top_level_err("text"), "a string");
        assert_top_level(&top_level_err(RawBytes(b"raw")), "bytes");
        assert_top_level(&top_level_err(()), "a unit");
        assert_top_level(&top_level_err(Unit), "a unit struct");
        assert_top_level(&top_level_err(Shape::Bare), "a unit variant");
        assert_top_level(&top_level_err(vec![1u8]), "a sequence");
        assert_top_level(&top_level_err((1u8, 2u8)), "a tuple");
        assert_top_level(&top_level_err(Pair(1, 2)), "a tuple struct");
        assert_top_level(&top_level_err(Shape::Pair(1, 2)), "a tuple variant");
    }

    #[test]
    fn top_level_wrappers_unwrap_to_the_struct_they_carry() {
        #[derive(Serialize)]
        struct Wrapper(Nested);

        let mut headers = Headers::new();
        headers.insert_typed(&Wrapper(Nested { inner: 1 })).unwrap();
        assert_eq!(headers.get_str("inner"), Some("1"));

        let mut headers = Headers::new();
        headers.insert_typed(&Some(Nested { inner: 2 })).unwrap();
        assert_eq!(headers.get_str("inner"), Some("2"));

        // A `None` contract writes nothing rather than failing: there is no shape to reject.
        let mut headers = Headers::new();
        headers.insert_typed(&Option::<Nested>::None).unwrap();
        assert!(headers.is_empty());

        // An externally tagged newtype variant stays flat, like the untagged form.
        let mut headers = Headers::new();
        headers
            .insert_typed(&TaggedNewtype::Meta(Nested { inner: 3 }))
            .unwrap();
        assert_eq!(headers.get_str("inner"), Some("3"));

        // A struct variant is a flat field list too.
        let mut headers = Headers::new();
        headers
            .insert_typed(&TaggedFields::Meta { inner: 4 })
            .unwrap();
        assert_eq!(headers.get_str("inner"), Some("4"));
    }

    #[test]
    fn map_keys_must_serialize_as_strings() {
        assert_non_string_key(&key_err(true));
        assert_non_string_key(&key_err(-8i8));
        assert_non_string_key(&key_err(-16i16));
        assert_non_string_key(&key_err(-32i32));
        assert_non_string_key(&key_err(-64i64));
        assert_non_string_key(&key_err(-128i128));
        assert_non_string_key(&key_err(8u8));
        assert_non_string_key(&key_err(16u16));
        assert_non_string_key(&key_err(32u32));
        assert_non_string_key(&key_err(64u64));
        assert_non_string_key(&key_err(128u128));
        assert_non_string_key(&key_err(1.5f32));
        assert_non_string_key(&key_err(2.5f64));
        assert_non_string_key(&key_err(RawBytes(b"raw")));
        assert_non_string_key(&key_err(Option::<u8>::None));
        assert_non_string_key(&key_err(Some(1u8)));
        assert_non_string_key(&key_err(()));
        assert_non_string_key(&key_err(Unit));
        assert_non_string_key(&key_err(Newtype(1)));
        assert_non_string_key(&key_err(Shape::Wrapping(1)));
        assert_non_string_key(&key_err(vec![1u8]));
        assert_non_string_key(&key_err((1u8, 2u8)));
        assert_non_string_key(&key_err(Pair(1, 2)));
        assert_non_string_key(&key_err(Shape::Pair(1, 2)));
        assert_non_string_key(&key_err(BTreeMap::from([("k", "v")])));
        assert_non_string_key(&key_err(Nested { inner: 1 }));
        assert_non_string_key(&key_err(Shape::Fields { a: 1 }));
    }

    #[test]
    fn textual_map_keys_become_header_names() {
        let mut headers = Headers::new();
        headers.insert_typed(&KeyedMap("name")).unwrap();
        assert_eq!(headers.get_str("name"), Some("value"));

        let mut headers = Headers::new();
        headers.insert_typed(&KeyedMap('c')).unwrap();
        assert_eq!(headers.get_str("c"), Some("value"));

        let mut headers = Headers::new();
        headers.insert_typed(&KeyedMap(Shape::Bare)).unwrap();
        assert_eq!(headers.get_str("Bare"), Some("value"));
    }
}
