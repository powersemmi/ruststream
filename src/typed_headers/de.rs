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
