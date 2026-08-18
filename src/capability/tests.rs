use super::*;

#[test]
fn with_security_accumulates_schemes_in_order() {
    let spec = ServerSpec::new("kafka.example.com:9093", "kafka")
        .with_security(SecurityScheme::scram_sha512())
        .with_security(SecurityScheme::x509());
    assert_eq!(spec.security.len(), 2);
    assert_eq!(spec.security[0].kind, SecuritySchemeKind::ScramSha512);
    assert_eq!(spec.security[1].kind, SecuritySchemeKind::X509);
}

#[test]
fn in_process_spec_starts_without_security() {
    let spec = ServerSpec::in_process("memory");
    assert!(spec.security.is_empty());
}

#[test]
fn constructors_produce_their_kind() {
    let cases = [
        (
            SecurityScheme::user_password(),
            SecuritySchemeKind::UserPassword,
        ),
        (
            SecurityScheme::api_key(ApiKeyLocation::User),
            SecuritySchemeKind::ApiKey {
                location: ApiKeyLocation::User,
            },
        ),
        (SecurityScheme::x509(), SecuritySchemeKind::X509),
        (SecurityScheme::plain(), SecuritySchemeKind::Plain),
        (
            SecurityScheme::scram_sha256(),
            SecuritySchemeKind::ScramSha256,
        ),
        (
            SecurityScheme::scram_sha512(),
            SecuritySchemeKind::ScramSha512,
        ),
        (SecurityScheme::gssapi(), SecuritySchemeKind::Gssapi),
        (
            SecurityScheme::http("bearer"),
            SecuritySchemeKind::Http {
                scheme: "bearer".into(),
            },
        ),
        (
            SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
            SecuritySchemeKind::HttpApiKey {
                name: "X-Api-Key".into(),
                location: HttpApiKeyLocation::Header,
            },
        ),
        (
            SecurityScheme::open_id_connect("https://idp.example.com/.well-known"),
            SecuritySchemeKind::OpenIdConnect {
                url: "https://idp.example.com/.well-known".into(),
            },
        ),
    ];
    for (scheme, kind) in cases {
        assert_eq!(scheme.kind, kind);
        assert_eq!(scheme.description, None);
    }
}

#[cfg(feature = "json")]
#[test]
fn json_backed_constructors_serialize_their_payload() {
    let oauth2 = SecurityScheme::oauth2(serde_json::json!({ "clientCredentials": {} }));
    assert_eq!(
        oauth2.kind,
        SecuritySchemeKind::Oauth2 {
            flows: r#"{"clientCredentials":{}}"#.into(),
        }
    );

    let custom = SecurityScheme::custom(serde_json::json!({ "type": "symmetricEncryption" }));
    assert_eq!(
        custom.kind,
        SecuritySchemeKind::Custom {
            object: r#"{"type":"symmetricEncryption"}"#.into(),
        }
    );
}

#[test]
fn with_description_sets_the_description() {
    let scheme = SecurityScheme::plain().with_description("SASL over TLS");
    assert_eq!(scheme.description.as_deref(), Some("SASL over TLS"));
}

#[test]
fn api_key_locations_map_to_document_values() {
    assert_eq!(ApiKeyLocation::User.as_api(), "user");
    assert_eq!(ApiKeyLocation::Password.as_api(), "password");
}

#[test]
fn http_api_key_locations_map_to_document_values() {
    assert_eq!(HttpApiKeyLocation::Query.as_api(), "query");
    assert_eq!(HttpApiKeyLocation::Header.as_api(), "header");
    assert_eq!(HttpApiKeyLocation::Cookie.as_api(), "cookie");
}
