//! The serde serializer behind [`Headers::insert_typed`](crate::Headers::insert_typed): each
//! field of a flat struct (or string-keyed map) becomes one header entry with a string-encoded
//! value.

#[cfg(not(feature = "alloc"))]
use core::fmt::Display;

use serde::ser::{
    self, Impossible, Serialize, SerializeMap, SerializeStruct, SerializeStructVariant,
};

use super::SerializeHeadersError;
use super::repr::{self, Key, Scalar};
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

    // Without `alloc` serde offers no default for this method.
    #[cfg(not(feature = "alloc"))]
    fn collect_str<T: Display + ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        Err(top_level("a string"))
    }

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
            repr::put_static(self.headers, key, bytes)?;
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
    key: Option<Key>,
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
            repr::put_owned(self.headers, key, bytes)?;
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
            field: repr::text(self.field),
            kind,
        }
    }
}

macro_rules! display_scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method(self, v: $ty) -> Result<Self::Ok, Self::Error> {
                Ok(Some(repr::scalar_display(&v)?))
            }
        )*
    };
}

impl ser::Serializer for ValueSerializer<'_> {
    type Ok = Option<Scalar>;
    type Error = SerializeHeadersError;
    type SerializeSeq = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeTuple = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeTupleStruct = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeTupleVariant = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeMap = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeStruct = Impossible<Option<Scalar>, SerializeHeadersError>;
    type SerializeStructVariant = Impossible<Option<Scalar>, SerializeHeadersError>;

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

    #[cfg(not(feature = "alloc"))]
    fn collect_str<T: Display + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        Ok(Some(repr::scalar_display(&format_args!("{value}"))?))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Some(repr::scalar_bytes(v.as_bytes())?))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Some(repr::scalar_bytes(v)?))
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
        Ok(Some(repr::scalar_static(variant)?))
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
    type Ok = Key;
    type Error = SerializeHeadersError;
    type SerializeSeq = Impossible<Key, SerializeHeadersError>;
    type SerializeTuple = Impossible<Key, SerializeHeadersError>;
    type SerializeTupleStruct = Impossible<Key, SerializeHeadersError>;
    type SerializeTupleVariant = Impossible<Key, SerializeHeadersError>;
    type SerializeMap = Impossible<Key, SerializeHeadersError>;
    type SerializeStruct = Impossible<Key, SerializeHeadersError>;
    type SerializeStructVariant = Impossible<Key, SerializeHeadersError>;

    #[cfg(not(feature = "alloc"))]
    fn collect_str<T: Display + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        repr::key_display(&format_args!("{value}"))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        repr::key(v)
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
        repr::key_of_char(v)
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
        repr::key(variant)
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
    SerializeHeadersError::Message(repr::text("header names must serialize as strings"))
}

#[cfg(test)]
#[cfg(feature = "alloc")]
mod tests;
