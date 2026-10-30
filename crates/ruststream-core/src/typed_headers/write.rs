//! Typed headers written into any [`HeaderStoreMut`] without a heap: the serializer behind
//! [`HeaderStoreMut::write_typed`] and its error.
//!
//! The heap's [`HeaderMap::insert_typed`](crate::HeaderMap::insert_typed) keeps its own
//! serializer: it moves each value into a shared buffer and names each field with a static key,
//! which a serializer writing through the trait's borrowed `insert` cannot do.

use core::error::Error as StdError;
use core::fmt::{self, Display, LowerExp, Write as _};

use serde::ser::SerializeStructVariant;
use serde::ser::{self, Error as SerError, Impossible, Serialize, SerializeStruct};
use thiserror::Error;

use crate::header_store::HeaderStoreMut;

/// Error of [`HeaderStoreMut::write_typed`]: the value does not fit the flat headers shape, or
/// the store has no room for it.
///
/// `InsertError` is the store's own [`HeaderStoreMut::InsertError`].
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, HeaderStoreMut, InsertHeaderError, WriteHeadersError};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Origin<'a> {
///     device: &'a str,
///     firmware: &'a str,
/// }
///
/// /// A device's publish step: a frame that cannot carry its origin goes out without it, and
/// /// the log names the field that did not fit.
/// fn stamp(headers: &mut FixedHeaderMap<4, 48>, origin: &Origin<'_>) {
///     match headers.write_typed(origin) {
///         Ok(()) => {}
///         Err(WriteHeadersError::Insert { field, source: InsertHeaderError::Bytes }) => {
///             tracing::warn!(field, "the origin is too long for the frame");
///         }
///         Err(error) => tracing::warn!(%error, "frame sent without its origin"),
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteHeadersError<InsertError> {
    /// A field holds a value header entries cannot carry (a nested struct, a sequence, a map, a
    /// data-carrying enum variant).
    #[error("field `{field}` cannot be serialized into a header value: {kind} is not a scalar")]
    UnsupportedValue {
        /// The offending field name.
        field: &'static str,
        /// The value's shape.
        kind: &'static str,
    },
    /// The top-level value is not a struct.
    #[error("typed headers serialize from a struct, not {kind}")]
    TopLevel {
        /// The offered top-level shape.
        kind: &'static str,
    },
    /// The store has no room for a field.
    #[error("field `{field}` does not fit into the headers")]
    Insert {
        /// The field that did not fit.
        field: &'static str,
        /// What the store reported.
        #[source]
        source: InsertError,
    },
    /// A field that serializes through its display form (an address, a timestamp) displays as
    /// more text than is formatted on the stack, 48 bytes.
    #[error("field `{field}` displays as more than 48 bytes")]
    TextTooLong {
        /// The offending field name.
        field: &'static str,
    },
    /// The value's own `Serialize` failed.
    #[error("the value refuses to serialize into headers")]
    Rejected,
}

impl<InsertError: StdError + 'static> SerError for WriteHeadersError<InsertError> {
    fn custom<T: Display>(_msg: T) -> Self {
        Self::Rejected
    }
}

/// Top-level serializer writing into a header store.
pub(crate) struct StoreSerializer<'a, Store> {
    headers: &'a mut Store,
}

impl<'a, Store> StoreSerializer<'a, Store> {
    pub(crate) const fn new(headers: &'a mut Store) -> Self {
        Self { headers }
    }
}

macro_rules! reject_top_level {
    ($($method:ident($($arg:ty),*) => $kind:literal),* $(,)?) => {
        $(
            fn $method(self, $(_: $arg),*) -> Result<Self::Ok, Self::Error> {
                Err(WriteHeadersError::TopLevel { kind: $kind })
            }
        )*
    };
}

impl<'a, Store: HeaderStoreMut> ser::Serializer for StoreSerializer<'a, Store> {
    type Ok = ();
    type Error = WriteHeadersError<Store::InsertError>;
    type SerializeSeq = Impossible<(), Self::Error>;
    type SerializeTuple = Impossible<(), Self::Error>;
    type SerializeTupleStruct = Impossible<(), Self::Error>;
    type SerializeTupleVariant = Impossible<(), Self::Error>;
    type SerializeMap = Impossible<(), Self::Error>;
    type SerializeStruct = FieldSink<'a, Store>;
    type SerializeStructVariant = FieldSink<'a, Store>;

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

    // A map's keys are serialized values that live only inside the key's own serialize call, so
    // naming a header after one needs a buffer of its own; a struct names its fields statically.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(WriteHeadersError::TopLevel { kind: "a map" })
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

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(WriteHeadersError::TopLevel {
            kind: "a unit variant",
        })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(WriteHeadersError::TopLevel {
            kind: "a unit struct",
        })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(WriteHeadersError::TopLevel { kind: "a sequence" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(WriteHeadersError::TopLevel { kind: "a tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(WriteHeadersError::TopLevel {
            kind: "a tuple struct",
        })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(WriteHeadersError::TopLevel {
            kind: "a tuple variant",
        })
    }

    fn collect_str<T: Display + ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        Err(WriteHeadersError::TopLevel { kind: "a string" })
    }

    reject_top_level! {
        serialize_unit() => "a unit",
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

/// Struct fields: one header per field, skipped when the value is `None`.
pub(crate) struct FieldSink<'a, Store> {
    headers: &'a mut Store,
}

impl<Store: HeaderStoreMut> SerializeStruct for FieldSink<'_, Store> {
    type Ok = ();
    type Error = WriteHeadersError<Store::InsertError>;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(ValueSerializer {
            headers: &mut *self.headers,
            field: key,
        })
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<Store: HeaderStoreMut> SerializeStructVariant for FieldSink<'_, Store> {
    type Ok = ();
    type Error = WriteHeadersError<Store::InsertError>;

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

/// The text of a number, formatted on the stack.
///
/// Every integer fits in its display form. A float's display form spells every digit out, which
/// for the extremes runs to hundreds of bytes, so a float that does not fit is written in its
/// exponent form, which parses back to the same value.
struct Text {
    buf: [u8; Self::CAPACITY],
    len: usize,
}

impl Text {
    /// The longest integer, `i128::MIN`, is 40 bytes; the longest exponent form of a float is 24.
    const CAPACITY: usize = 48;

    const fn new() -> Self {
        Self {
            buf: [0; Self::CAPACITY],
            len: 0,
        }
    }

    fn of<Number: Display + LowerExp>(number: Number) -> Self {
        let mut text = Self::new();
        if write!(text, "{number}").is_err() {
            text.len = 0;
            // The exponent form of any number fits the capacity, so this write cannot run out.
            let _ = write!(text, "{number:e}");
        }
        text
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl fmt::Write for Text {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let end = self.len + s.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// One field value, string-encoded and written under the field's name. `None` writes nothing.
struct ValueSerializer<'a, Store> {
    headers: &'a mut Store,
    field: &'static str,
}

impl<Store: HeaderStoreMut> ValueSerializer<'_, Store> {
    fn write(self, value: &[u8]) -> Result<(), WriteHeadersError<Store::InsertError>> {
        let field = self.field;
        self.headers
            .insert(field, value)
            .map_err(|source| WriteHeadersError::Insert { field, source })
    }

    const fn unsupported(&self, kind: &'static str) -> WriteHeadersError<Store::InsertError> {
        WriteHeadersError::UnsupportedValue {
            field: self.field,
            kind,
        }
    }
}

macro_rules! number {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method(self, v: $ty) -> Result<Self::Ok, Self::Error> {
                self.write(Text::of(v).as_bytes())
            }
        )*
    };
}

macro_rules! reject_value {
    ($($method:ident($($arg:ty),*) -> $ret:ty => $kind:literal),* $(,)?) => {
        $(
            fn $method(self, $(_: $arg),*) -> Result<$ret, Self::Error> {
                Err(self.unsupported($kind))
            }
        )*
    };
}

impl<Store: HeaderStoreMut> ser::Serializer for ValueSerializer<'_, Store> {
    type Ok = ();
    type Error = WriteHeadersError<Store::InsertError>;
    type SerializeSeq = Impossible<(), Self::Error>;
    type SerializeTuple = Impossible<(), Self::Error>;
    type SerializeTupleStruct = Impossible<(), Self::Error>;
    type SerializeTupleVariant = Impossible<(), Self::Error>;
    type SerializeMap = Impossible<(), Self::Error>;
    type SerializeStruct = Impossible<(), Self::Error>;
    type SerializeStructVariant = Impossible<(), Self::Error>;

    number! {
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
    }

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.write(if v { b"true" } else { b"false" })
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.write(v.encode_utf8(&mut [0; 4]).as_bytes())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.write(v.as_bytes())
    }

    fn collect_str<T: Display + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        let mut text = Text::new();
        if write!(text, "{value}").is_err() {
            return Err(WriteHeadersError::TextTooLong { field: self.field });
        }
        self.write(text.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.write(v)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
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
        self.write(variant.as_bytes())
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
        Err(self.unsupported("a data-carrying enum variant"))
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

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(self.unsupported("a struct variant"))
    }

    reject_value! {
        serialize_unit() -> () => "a unit",
        serialize_unit_struct(&'static str) -> () => "a unit struct",
        serialize_seq(Option<usize>) -> Self::SerializeSeq => "a sequence",
        serialize_tuple(usize) -> Self::SerializeTuple => "a tuple",
        serialize_tuple_struct(&'static str, usize) -> Self::SerializeTupleStruct
            => "a tuple struct",
        serialize_map(Option<usize>) -> Self::SerializeMap => "a map",
        serialize_struct(&'static str, usize) -> Self::SerializeStruct => "a nested struct",
    }
}

#[cfg(test)]
#[cfg(feature = "heapless")]
mod tests {
    use std::collections::BTreeMap;
    use std::string::ToString;
    use std::vec;
    use std::vec::Vec;

    use serde::Serialize;

    use super::*;
    use crate::fixed_headers::{FixedHeaderMap, InsertHeaderError};

    type Store = FixedHeaderMap<16, 512>;
    type WriteError = WriteHeadersError<InsertHeaderError>;

    /// A value that serializes as bytes, as `serde_bytes` drives it.
    struct Bytes<'a>(&'a [u8]);

    impl Serialize for Bytes<'_> {
        fn serialize<S: ser::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(self.0)
        }
    }

    fn write_err<T: Serialize>(value: &T) -> WriteError {
        Store::new().write_typed(value).unwrap_err()
    }

    #[test]
    fn a_top_level_value_that_is_not_a_struct_is_rejected() {
        #[derive(Serialize)]
        struct Unit;
        #[derive(Serialize)]
        struct Pair(u8, u8);
        #[derive(Serialize)]
        enum Shape {
            Point,
            Tuple(u8, u8),
        }
        let top = |kind| WriteHeadersError::TopLevel { kind };
        assert_eq!(write_err(&()), top("a unit"));
        assert_eq!(write_err(&Unit), top("a unit struct"));
        assert_eq!(write_err(&Shape::Point), top("a unit variant"));
        assert_eq!(write_err(&Shape::Tuple(1, 2)), top("a tuple variant"));
        assert_eq!(write_err(&vec![1]), top("a sequence"));
        assert_eq!(write_err(&(1, 2)), top("a tuple"));
        assert_eq!(write_err(&Pair(1, 2)), top("a tuple struct"));
        assert_eq!(write_err(&BTreeMap::from([("k", 1)])), top("a map"));
        assert_eq!(
            write_err(&()).to_string(),
            "typed headers serialize from a struct, not a unit"
        );
    }

    #[test]
    fn a_top_level_scalar_is_rejected() {
        let top = |kind| WriteHeadersError::TopLevel { kind };
        assert_eq!(write_err(&true), top("a boolean"));
        assert_eq!(write_err(&1_i8), top("an integer"));
        assert_eq!(write_err(&1_i16), top("an integer"));
        assert_eq!(write_err(&1_i32), top("an integer"));
        assert_eq!(write_err(&1_i64), top("an integer"));
        assert_eq!(write_err(&1_i128), top("an integer"));
        assert_eq!(write_err(&1_u8), top("an integer"));
        assert_eq!(write_err(&1_u16), top("an integer"));
        assert_eq!(write_err(&1_u32), top("an integer"));
        assert_eq!(write_err(&1_u64), top("an integer"));
        assert_eq!(write_err(&1_u128), top("an integer"));
        assert_eq!(write_err(&1.0_f32), top("a float"));
        assert_eq!(write_err(&1.0_f64), top("a float"));
        assert_eq!(write_err(&'c'), top("a character"));
        assert_eq!(write_err(&"s"), top("a string"));
        assert_eq!(
            Store::new().write_typed(&Bytes(b"b")).unwrap_err(),
            top("bytes")
        );
    }

    #[test]
    fn wrappers_around_the_struct_write_its_fields() {
        #[derive(Serialize)]
        struct Inner {
            n: u8,
        }
        #[derive(Serialize)]
        struct Wrapper(Inner);
        #[derive(Serialize)]
        enum Tagged {
            Fields { k: u16 },
        }
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Either {
            Plain(Inner),
            Fields { m: i16, small: f32 },
        }

        let mut headers = Store::new();
        headers.write_typed(&Wrapper(Inner { n: 1 })).unwrap();
        headers.write_typed(&Some(Inner { n: 2 })).unwrap();
        headers.write_typed(&None::<Inner>).unwrap();
        assert_eq!(headers.get_str("n"), Some("2"));
        headers.write_typed(&Either::Plain(Inner { n: 3 })).unwrap();
        headers
            .write_typed(&Either::Fields { m: -4, small: 0.5 })
            .unwrap();
        assert_eq!(headers.get_str("n"), Some("3"));
        assert_eq!(headers.get_str("m"), Some("-4"));
        assert_eq!(headers.get_str("small"), Some("0.5"));
        headers.write_typed(&Tagged::Fields { k: 5 }).unwrap();
        assert_eq!(headers.get_str("k"), Some("5"));
    }

    #[test]
    fn a_field_a_header_cannot_carry_names_the_field() {
        #[derive(Serialize)]
        struct Unit;
        #[derive(Serialize)]
        struct Pair(u8, u8);
        #[derive(Serialize)]
        struct Nested {
            n: u8,
        }
        #[derive(Serialize)]
        struct Id(u8);
        #[derive(Serialize)]
        enum Shape {
            Wrap(u8),
            Tuple(u8, u8),
            Fields { n: u8 },
        }
        #[derive(Serialize)]
        struct One<T> {
            field: T,
        }
        let value = |kind| WriteHeadersError::UnsupportedValue {
            field: "field",
            kind,
        };
        assert_eq!(write_err(&One { field: () }), value("a unit"));
        assert_eq!(write_err(&One { field: Unit }), value("a unit struct"));
        assert_eq!(write_err(&One { field: vec![1] }), value("a sequence"));
        assert_eq!(write_err(&One { field: (1, 2) }), value("a tuple"));
        assert_eq!(
            write_err(&One { field: Pair(1, 2) }),
            value("a tuple struct")
        );
        assert_eq!(
            write_err(&One {
                field: BTreeMap::from([("k", 1)])
            }),
            value("a map")
        );
        assert_eq!(
            write_err(&One {
                field: Nested { n: 1 }
            }),
            value("a nested struct")
        );
        assert_eq!(
            write_err(&One {
                field: Shape::Wrap(1)
            }),
            value("a data-carrying enum variant")
        );
        assert_eq!(
            write_err(&One {
                field: Shape::Tuple(1, 2)
            }),
            value("a tuple variant")
        );
        assert_eq!(
            write_err(&One {
                field: Shape::Fields { n: 1 }
            }),
            value("a struct variant")
        );
        assert_eq!(
            write_err(&One { field: () }).to_string(),
            "field `field` cannot be serialized into a header value: a unit is not a scalar"
        );

        let mut headers = Store::new();
        headers.write_typed(&One { field: Id(9) }).unwrap();
        assert_eq!(headers.get_str("field"), Some("9"));
        headers
            .write_typed(&One {
                field: (1_i8, 2_i16, 3_i32, 4_u8, 5_u16, 6_u64, -7_i128),
            })
            .unwrap_err();
    }

    #[test]
    fn every_number_kind_is_written_in_its_display_form() {
        #[derive(Serialize)]
        struct Numbers {
            a: i8,
            b: i16,
            c: i32,
            d: u8,
            e: u16,
            f: u64,
            g: i128,
        }
        let mut headers = Store::new();
        headers
            .write_typed(&Numbers {
                a: -1,
                b: -2,
                c: -3,
                d: 4,
                e: 5,
                f: 6,
                g: i128::MIN,
            })
            .unwrap();
        let text: Vec<_> = headers.iter().map(|(_, v)| v.to_vec()).collect();
        let expected: Vec<Vec<u8>> = ["-1", "-2", "-3", "4", "5", "6", &i128::MIN.to_string()]
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect();
        assert_eq!(text, expected);
    }

    /// Serializes through its display form, as addresses and timestamps do.
    struct Shown(&'static str);

    impl Serialize for Shown {
        fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(self.0)
        }
    }

    #[test]
    fn a_displayed_field_is_written_as_its_text() {
        #[derive(Serialize)]
        struct At {
            addr: Shown,
        }
        let mut headers = Store::new();
        headers
            .write_typed(&At {
                addr: Shown("10.0.0.1"),
            })
            .unwrap();
        assert_eq!(headers.get_str("addr"), Some("10.0.0.1"));

        let long = At {
            addr: Shown("a text that runs past the forty-eight bytes on the stack"),
        };
        let err = headers.write_typed(&long).unwrap_err();
        assert_eq!(err, WriteHeadersError::TextTooLong { field: "addr" });
        assert_eq!(
            err.to_string(),
            "field `addr` displays as more than 48 bytes"
        );
        assert_eq!(
            headers.write_typed(&Shown("x")).unwrap_err(),
            WriteHeadersError::TopLevel { kind: "a string" }
        );
    }

    #[test]
    fn a_full_store_names_the_field_that_did_not_fit() {
        #[derive(Serialize)]
        struct Meta {
            seq: u32,
            device: &'static str,
        }
        let mut headers = FixedHeaderMap::<4, 8>::new();
        let err = headers
            .write_typed(&Meta {
                seq: 1,
                device: "too long",
            })
            .unwrap_err();
        assert_eq!(
            err,
            WriteHeadersError::Insert {
                field: "device",
                source: InsertHeaderError::Bytes,
            }
        );
        assert_eq!(
            err.to_string(),
            "field `device` does not fit into the headers"
        );
        assert_eq!(
            StdError::source(&err).unwrap().to_string(),
            InsertHeaderError::Bytes.to_string()
        );
        // The fields before it stay written.
        assert_eq!(headers.get_str("seq"), Some("1"));
    }

    #[test]
    fn a_custom_serialize_failure_is_rejected() {
        struct Refuses;

        impl Serialize for Refuses {
            fn serialize<S: ser::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(S::Error::custom("no"))
            }
        }

        let err = write_err(&Refuses);
        assert_eq!(err, WriteHeadersError::Rejected);
        assert_eq!(
            err.to_string(),
            "the value refuses to serialize into headers"
        );
    }
}
