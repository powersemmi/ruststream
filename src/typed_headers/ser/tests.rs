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
