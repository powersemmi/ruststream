//! The message components of a document: a schema in the draft the document declares, the name
//! a type gets, one component shared by every side that carries it, and the header a set reads
//! as a `const` of its headers schema.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use tracing::warn;

use super::spec::MessageObject;
use crate::bindings::Bindings;
use crate::runtime::HeaderDiscriminator;

/// Takes the final path segment of a type name as the message name (`a::b::Order` -> `Order`).
pub(super) fn message_name(type_name: &str) -> String {
    type_name
        .rsplit("::")
        .next()
        .unwrap_or(type_name)
        .to_owned()
}

/// Adds the header a message set reads, with the value that names this model, to the model's
/// headers schema as a required `const` property: the `AsyncAPI` counterpart of an `OpenAPI`
/// discriminator mapping. A model without a header contract gets an object schema of that one
/// property.
pub(super) fn with_header_const(headers: Option<Value>, rule: HeaderDiscriminator) -> Value {
    let mut schema = match headers {
        Some(Value::Object(schema)) => schema,
        _ => Map::from_iter([("type".to_owned(), Value::String("object".to_owned()))]),
    };
    let mut properties = match schema.remove("properties") {
        Some(Value::Object(properties)) => properties,
        _ => Map::new(),
    };
    properties.insert(rule.header.to_owned(), json!({ "const": rule.value }));
    schema.insert("properties".to_owned(), Value::Object(properties));
    let mut required = match schema.remove("required") {
        Some(Value::Array(required)) => required,
        _ => Vec::new(),
    };
    if !required.iter().any(|name| name == rule.header) {
        required.push(Value::String(rule.header.to_owned()));
    }
    schema.insert("required".to_owned(), Value::Array(required));
    Value::Object(schema)
}

/// One side's contribution to a shared message component.
pub(super) struct MessageContribution {
    pub(super) name: String,
    pub(super) title: Option<String>,
    pub(super) description: Option<String>,
    pub(super) content_type: Option<String>,
    pub(super) payload: Option<Value>,
    pub(super) headers: Option<Value>,
    pub(super) schemaless: bool,
    pub(super) bindings: Bindings,
}

/// Merges one contribution into a shared message component: an absent title, description,
/// media type, payload, or headers schema fills in from a later contributor (component identity
/// is the message name, and several handlers may carry different slices of its metadata), while
/// a conflicting headers schema keeps the first one with a WARN - one component cannot carry two
/// contracts, and the conflict usually means two handlers read the same type with different
/// `Headers` declarations.
pub(super) fn merge_message(
    messages: &mut BTreeMap<String, MessageObject>,
    contribution: MessageContribution,
    context: &str,
) {
    let MessageContribution {
        name,
        title,
        description,
        content_type,
        payload,
        headers,
        schemaless,
        bindings,
    } = contribution;
    let entry = messages
        .entry(name.clone())
        .or_insert_with(|| MessageObject {
            name,
            title: None,
            description: None,
            content_type: None,
            payload: None,
            headers: None,
            schemaless: false,
            bindings: Bindings::new(),
        });
    entry.bindings.fill_from(&bindings);
    // A title repeating the machine name says nothing the reader does not already see.
    if entry.title.is_none() && title.as_deref().is_some_and(|title| title != entry.name) {
        entry.title = title;
    }
    if entry.description.is_none() {
        entry.description = description;
    }
    if entry.content_type.is_none() {
        entry.content_type = content_type;
    }
    // One schema-free-by-design contributor is enough: the shared component's missing schema
    // is then explained, whichever side contributed it.
    entry.schemaless |= schemaless;
    if entry.payload.is_none() {
        entry.payload = payload;
    }
    match (&entry.headers, headers) {
        (None, Some(headers)) => entry.headers = Some(headers),
        (Some(existing), Some(headers)) if *existing != headers => {
            warn!(
                target: "ruststream::asyncapi",
                message = %entry.name,
                context = %context,
                "conflicting headers schemas for one message component; keeping the first",
            );
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::runtime::HeaderDiscriminator;

    use super::with_header_const;

    /// A model with no header contract documents the header the set reads as the one property
    /// of its headers schema.
    #[test]
    fn the_header_rule_makes_a_headers_schema_of_its_own() {
        let schema = with_header_const(None, HeaderDiscriminator::new("event", "created"));
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": { "event": { "const": "created" } },
                "required": ["event"],
            })
        );
    }

    /// A model with a header contract keeps it, and the rule's header joins its properties; a
    /// header the contract already requires is listed once.
    #[test]
    fn the_header_rule_joins_an_existing_contract() {
        let contract = json!({
            "type": "object",
            "properties": { "tenant": { "type": "string" }, "event": { "type": "string" } },
            "required": ["tenant", "event"],
        });
        let schema =
            with_header_const(Some(contract), HeaderDiscriminator::new("event", "created"));
        assert_eq!(schema["properties"]["tenant"], json!({ "type": "string" }));
        assert_eq!(schema["properties"]["event"], json!({ "const": "created" }));
        assert_eq!(schema["required"], json!(["tenant", "event"]));
    }

    /// A contract whose `properties` or `required` is not the shape a schema gives them is
    /// rewritten around the rule rather than trusted.
    #[test]
    fn the_header_rule_replaces_malformed_parts_of_a_contract() {
        let odd = json!({ "type": "object", "properties": 7, "required": "event" });
        let schema = with_header_const(Some(odd), HeaderDiscriminator::new("event", "created"));
        assert_eq!(
            schema["properties"],
            json!({ "event": { "const": "created" } })
        );
        assert_eq!(schema["required"], json!(["event"]));
        let scalar = with_header_const(
            Some(json!(true)),
            HeaderDiscriminator::new("event", "created"),
        );
        assert_eq!(scalar["type"], json!("object"));
    }
}
