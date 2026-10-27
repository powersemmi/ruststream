//! The servers of a document: the brokers a service connects to, and the security schemes they
//! reference.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::SecurityScheme;
use crate::runtime::App;

use super::spec::{Reference, Server};

/// Renders the service's registered servers, collecting the security schemes they reference into
/// the components map as it goes.
pub(super) fn build_servers<A: App>(
    app: &A,
) -> (BTreeMap<String, Server>, BTreeMap<String, Value>) {
    let mut security_schemes = BTreeMap::new();
    let servers = app
        .servers()
        .iter()
        .map(|(name, spec)| {
            let security = spec
                .security
                .iter()
                .enumerate()
                .map(|(index, scheme)| {
                    let key = if index == 0 {
                        name.clone()
                    } else {
                        format!("{name}-{index}")
                    };
                    security_schemes.insert(key.clone(), security_scheme_object(scheme));
                    Reference::new(format!("#/components/securitySchemes/{key}"))
                })
                .collect();
            (
                name.clone(),
                Server {
                    host: spec.host.clone(),
                    protocol: spec.protocol.clone(),
                    protocol_version: spec.protocol_version.clone(),
                    description: spec.description.clone(),
                    security,
                    bindings: spec.bindings.clone(),
                },
            )
        })
        .collect();
    (servers, security_schemes)
}

/// The server a registration's channels live on: the label the broker was registered under, or
/// the single server of a one-broker document.
///
/// A document with several servers and an unlabeled registration gets no answer, and the channel
/// then says nothing rather than claiming a server it cannot name.
pub(super) fn resolve_server<'a>(
    label: Option<&'a str>,
    servers: &'a BTreeMap<String, Server>,
) -> Option<&'a str> {
    match label {
        Some(label) if servers.contains_key(label) => Some(label),
        _ if servers.len() == 1 => servers.keys().next().map(String::as_str),
        _ => None,
    }
}

/// Renders a [`SecurityScheme`](crate::SecurityScheme) as its `AsyncAPI` security scheme object.
fn security_scheme_object(scheme: &SecurityScheme) -> Value {
    use crate::capability::SecuritySchemeKind as Kind;

    // Raw payloads round-trip through the string the constructor serialized, so parsing them
    // back cannot fail; Null is the unreachable fallback, not an error path.
    let parse = |raw: &str| serde_json::from_str::<Value>(raw).unwrap_or(Value::Null);
    let mut object = match &scheme.kind {
        Kind::UserPassword => json!({ "type": "userPassword" }),
        Kind::ApiKey { location } => {
            json!({ "type": "apiKey", "in": location.as_api() })
        }
        Kind::X509 => json!({ "type": "X509" }),
        Kind::Plain => json!({ "type": "plain" }),
        Kind::ScramSha256 => json!({ "type": "scramSha256" }),
        Kind::ScramSha512 => json!({ "type": "scramSha512" }),
        Kind::Gssapi => json!({ "type": "gssapi" }),
        Kind::Http { scheme } => json!({ "type": "http", "scheme": scheme }),
        Kind::HttpApiKey { name, location } => json!({
            "type": "httpApiKey",
            "name": name,
            "in": location.as_api(),
        }),
        Kind::OpenIdConnect { url } => json!({
            "type": "openIdConnect",
            "openIdConnectUrl": url,
        }),
        Kind::Oauth2 { flows } => json!({ "type": "oauth2", "flows": parse(flows) }),
        Kind::Custom { object } => parse(object),
    };
    if let (Some(description), Some(fields)) = (&scheme.description, object.as_object_mut()) {
        fields.insert("description".to_owned(), Value::String(description.clone()));
    }
    object
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{ApiKeyLocation, HttpApiKeyLocation, SecurityScheme};

    use super::security_scheme_object;

    #[test]
    fn every_scheme_kind_renders_its_document_object() {
        let cases = [
            (
                SecurityScheme::user_password(),
                json!({ "type": "userPassword" }),
            ),
            (
                SecurityScheme::api_key(ApiKeyLocation::Password),
                json!({ "type": "apiKey", "in": "password" }),
            ),
            (SecurityScheme::x509(), json!({ "type": "X509" })),
            (SecurityScheme::plain(), json!({ "type": "plain" })),
            (
                SecurityScheme::scram_sha256(),
                json!({ "type": "scramSha256" }),
            ),
            (
                SecurityScheme::scram_sha512(),
                json!({ "type": "scramSha512" }),
            ),
            (SecurityScheme::gssapi(), json!({ "type": "gssapi" })),
            (
                SecurityScheme::http("bearer"),
                json!({ "type": "http", "scheme": "bearer" }),
            ),
            (
                SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
                json!({ "type": "httpApiKey", "name": "X-Api-Key", "in": "header" }),
            ),
            (
                SecurityScheme::open_id_connect("https://idp.example.com/.well-known"),
                json!({
                    "type": "openIdConnect",
                    "openIdConnectUrl": "https://idp.example.com/.well-known",
                }),
            ),
            (
                SecurityScheme::oauth2(json!({ "clientCredentials": {} })),
                json!({ "type": "oauth2", "flows": { "clientCredentials": {} } }),
            ),
            (
                SecurityScheme::custom(json!({ "type": "symmetricEncryption" })),
                json!({ "type": "symmetricEncryption" }),
            ),
        ];
        for (scheme, expected) in cases {
            assert_eq!(security_scheme_object(&scheme), expected);
        }
    }
}
