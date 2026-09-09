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
fn the_host_survives_a_scheme_userinfo_a_path_and_a_query() {
    let cases = [
        ("amqp://localhost:5672", "localhost:5672"),
        ("amqp://user:pass@broker:5672", "broker:5672"),
        ("amqps://broker:5671/vhost", "broker:5671"),
        ("nats://broker:4222/?tls=true", "broker:4222"),
        ("redis://:secret@cache:6379", "cache:6379"),
        // No scheme at all: a bare address is already the coordinate.
        ("broker:5672", "broker:5672"),
        // A password may contain '@', so the boundary is the last one, not the first.
        ("amqp://user:p@ss@broker:5672", "broker:5672"),
        // A host without a port stays a host.
        ("mqtt://user:pass@broker", "broker"),
    ];
    for (url, expected) in cases {
        assert_eq!(ServerSpec::host_from_url(url), expected, "url {url:?}");
    }
}

/// The document a service generates is meant to be published, so a URL that authenticates the
/// connection must not describe the server it reaches.
#[test]
fn a_url_carrying_credentials_describes_a_server_without_them() {
    let spec = ServerSpec::from_url("amqp://svc:secret@broker.example.com:5672/prod", "amqp");

    assert_eq!(spec.host.as_deref(), Some("broker.example.com:5672"));
    assert_eq!(spec.protocol, "amqp");

    let host = spec.host.expect("a networked broker describes a host");
    assert!(!host.contains("secret"), "the description leaked {host:?}");
    assert!(!host.contains("svc"), "the description leaked {host:?}");
    assert!(!host.contains('@'), "the description leaked {host:?}");
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
