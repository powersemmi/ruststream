//! The message components of a document: a schema in the draft the document declares, the name
//! a type gets, one component shared by every side that carries it, and the header a set reads
//! as a `const` of its headers schema.

use std::collections::BTreeMap;

use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::{Map, Value, json};
use tracing::warn;

use crate::runtime::HeaderDiscriminator;

use super::bindings::Bindings;
use super::spec::MessageObject;

/// Serializes the JSON Schema of `T` in the draft the document's Schema Object is a superset of.
///
/// The `AsyncAPI` Schema Object extends JSON Schema Draft 07, while `schemars` generates
/// 2020-12 by default: a payload generated with the default settings claims one draft inside a
/// document that says another. Every schema in the document goes through here.
pub(crate) fn schema_json<T: JsonSchema + ?Sized>() -> Option<String> {
    let schema = SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<T>();
    serde_json::to_string(&schema).ok()
}

/// Takes the final path segment of a type name as the message name (`a::b::Order` -> `Order`).
pub(super) fn message_name(type_name: &str) -> String {
    type_name
        .rsplit("::")
        .next()
        .unwrap_or(type_name)
        .to_owned()
}

/// Joins the subscription's header contract (`shared`, what a `Headers<T>` parameter reads on
/// every delivery) with one model's own contract (`own`), for a model of a message set: the
/// properties of both, the model's winning where the two name one header, and the union of what
/// each requires. A contract that is not an object schema is left out.
pub(crate) fn join_headers(shared: &str, own: Option<&str>) -> Option<String> {
    let parse = |json: &str| match serde_json::from_str::<Value>(json) {
        Ok(Value::Object(schema)) => Some(schema),
        _ => None,
    };
    let joined = match (parse(shared), own.and_then(parse)) {
        (Some(shared), Some(own)) => join_objects(shared, own),
        (Some(shared), None) => shared,
        (None, Some(own)) => own,
        (None, None) => return None,
    };
    serde_json::to_string(&Value::Object(joined)).ok()
}

/// `own` laid over `shared`: every property of both, `own`'s where they overlap, the required
/// names of both, each once, and the subschemas both contracts reference.
fn join_objects(mut shared: Map<String, Value>, mut own: Map<String, Value>) -> Map<String, Value> {
    // The definitions a contract's `$ref`s point at stay inside the joined schema, under the key
    // the draft gives them, since that is where every schema of the document keeps them. The
    // model's keep their names. A subscription definition whose name the model also uses for a
    // different schema takes the smallest free `<name>_<n>` (n counted from 2), and every
    // reference the subscription makes to it is rewritten first, so a header that reads the
    // subscription's `Source` still does once the model's `Source` holds the plain name.
    for key in ["definitions", "$defs"] {
        let Some(Value::Object(theirs)) = shared.get(key) else {
            continue;
        };
        let mine = match own.get(key) {
            Some(Value::Object(mine)) => mine.clone(),
            _ => Map::new(),
        };
        let renames: Vec<(String, String)> = theirs
            .iter()
            .filter(|(name, schema)| mine.get(*name).is_some_and(|existing| existing != *schema))
            .map(|(name, _)| {
                let mut n = 2;
                let mut renamed = format!("{name}_{n}");
                while mine.contains_key(&renamed) || theirs.contains_key(&renamed) {
                    n += 1;
                    renamed = format!("{name}_{n}");
                }
                (name.clone(), renamed)
            })
            .collect();
        for (from, to) in &renames {
            let (from, to) = (format!("#/{key}/{from}"), format!("#/{key}/{to}"));
            for value in shared.values_mut() {
                rewrite_refs(value, &from, &to);
            }
        }
        let Some(Value::Object(theirs)) = shared.remove(key) else {
            continue;
        };
        let mut mine = mine;
        for (name, schema) in theirs {
            let name = renames
                .iter()
                .find(|(from, _)| *from == name)
                .map_or(name, |(_, to)| to.clone());
            mine.entry(name).or_insert(schema);
        }
        own.insert(key.to_owned(), Value::Object(mine));
    }
    let mut properties = match shared.get("properties") {
        Some(Value::Object(properties)) => properties.clone(),
        _ => Map::new(),
    };
    if let Some(Value::Object(theirs)) = own.remove("properties") {
        properties.extend(theirs);
    }
    let mut required = match shared.get("required") {
        Some(Value::Array(required)) => required.clone(),
        _ => Vec::new(),
    };
    if let Some(Value::Array(theirs)) = own.remove("required") {
        for name in theirs {
            if !required.contains(&name) {
                required.push(name);
            }
        }
    }
    own.entry("type".to_owned())
        .or_insert_with(|| Value::String("object".to_owned()));
    own.insert("properties".to_owned(), Value::Object(properties));
    own.insert("required".to_owned(), Value::Array(required));
    own
}

/// Points every `$ref` equal to `from` at `to`, through the whole of `value`.
fn rewrite_refs(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "$ref" && value.as_str() == Some(from) {
                    *value = Value::String(to.to_owned());
                } else {
                    rewrite_refs(value, from, to);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs(item, from, to);
            }
        }
        _ => {}
    }
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

    use super::{join_headers, with_header_const};

    /// A model without a contract of its own takes the subscription's; with one, the two join,
    /// the model's winning on a header both name, and each required name listed once.
    #[test]
    fn the_subscription_contract_joins_the_models() {
        let shared = r#"{"type":"object","properties":{"tenant":{"type":"string"},"event":{"type":"string"}},"required":["tenant","event"]}"#;
        let alone: serde_json::Value =
            serde_json::from_str(&join_headers(shared, None).expect("the shared contract"))
                .unwrap();
        assert_eq!(alone["properties"]["tenant"], json!({ "type": "string" }));

        let own =
            r#"{"type":"object","properties":{"event":{"enum":["created"]}},"required":["event"]}"#;
        let joined: serde_json::Value =
            serde_json::from_str(&join_headers(shared, Some(own)).expect("the joined contract"))
                .unwrap();
        assert_eq!(joined["properties"]["tenant"], json!({ "type": "string" }));
        assert_eq!(
            joined["properties"]["event"],
            json!({ "enum": ["created"] })
        );
        assert_eq!(joined["required"], json!(["tenant", "event"]));
    }

    /// The subschemas both contracts reference stay with the joined schema: a definition both
    /// name alike is kept once, and a subscription definition the model names otherwise takes
    /// the next free number, with every reference to it following, nested ones included.
    #[test]
    fn the_definitions_of_both_contracts_join() {
        let shared = r##"{"type":"object","properties":{"origin":{"$ref":"#/definitions/Source"},"channel":{"$ref":"#/definitions/Channel"},"legs":{"items":{"$ref":"#/definitions/Leg"}}},"required":["origin"],"definitions":{"Source":{"enum":["Web","Store"]},"Channel":{"enum":["Email","Sms"]},"Leg":{"properties":{"from":{"$ref":"#/definitions/Source"}}},"Source_2":{"enum":["Taken"]}}}"##;
        let own = r##"{"type":"object","properties":{"source":{"$ref":"#/definitions/Source"}},"required":["source"],"definitions":{"Source":{"enum":["Warehouse","Courier"]},"Channel":{"enum":["Email","Sms"]}}}"##;
        let joined: serde_json::Value =
            serde_json::from_str(&join_headers(shared, Some(own)).expect("the joined contract"))
                .unwrap();
        let definitions = &joined["definitions"];
        assert_eq!(
            definitions["Source"]["enum"],
            json!(["Warehouse", "Courier"])
        );
        assert_eq!(definitions["Channel"]["enum"], json!(["Email", "Sms"]));
        assert_eq!(definitions["Source_2"]["enum"], json!(["Taken"]));
        assert_eq!(definitions["Source_3"]["enum"], json!(["Web", "Store"]));
        assert_eq!(
            joined["properties"]["origin"]["$ref"],
            "#/definitions/Source_3"
        );
        assert_eq!(
            joined["properties"]["source"]["$ref"],
            "#/definitions/Source"
        );
        assert_eq!(
            definitions["Leg"]["properties"]["from"]["$ref"],
            "#/definitions/Source_3"
        );
        assert_eq!(definitions.as_object().map(serde_json::Map::len), Some(5));
    }

    /// The 2020-12 spelling of the definitions key joins the same way.
    #[test]
    fn the_defs_key_joins_like_definitions() {
        let shared = r##"{"type":"object","properties":{"channel":{"$ref":"#/$defs/Channel"}},"$defs":{"Channel":{"enum":["Email"]}}}"##;
        let own = r##"{"type":"object","properties":{"kind":{"$ref":"#/$defs/Kind"}},"$defs":{"Kind":{"enum":["Return"]}}}"##;
        let joined: serde_json::Value =
            serde_json::from_str(&join_headers(shared, Some(own)).expect("the joined contract"))
                .unwrap();
        assert_eq!(joined["$defs"]["Channel"]["enum"], json!(["Email"]));
        assert_eq!(joined["$defs"]["Kind"]["enum"], json!(["Return"]));
    }

    /// A contract that is not an object schema contributes nothing.
    #[test]
    fn a_contract_that_is_no_object_is_left_out() {
        let own = r#"{"type":"object","properties":{"event":{"type":"string"}}}"#;
        let kept: serde_json::Value =
            serde_json::from_str(&join_headers("true", Some(own)).expect("the own contract"))
                .unwrap();
        assert_eq!(kept["properties"]["event"], json!({ "type": "string" }));
        assert_eq!(join_headers("not json", None), None);
    }

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
