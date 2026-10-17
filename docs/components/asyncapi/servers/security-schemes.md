---
sidebar_position: 3
title: Security
description: "ServerSpec::security attaches a SecurityScheme that says how clients authenticate; each scheme becomes a component the server references."
---

# Security

`ServerSpec::security` attaches a `SecurityScheme` that says how clients authenticate; each scheme becomes a component the server references.

Security is the service author's statement, never the broker's: one broker runs with different authentication in different deployments.
To secure a server a [labeled broker](./with-broker-labeled.md) describes, the service declares it again under the same label with the scheme attached.
Each call of [`security`](api:ruststream::ServerSpec::security) adds an alternative, in order.
The first scheme's component takes the server's name as its key, the next ones `-1`, `-2` and on.
[`SecurityScheme`](api:ruststream::SecurityScheme) has a constructor per scheme the specification lists: `user_password`, `api_key` with an [`ApiKeyLocation`](api:ruststream::ApiKeyLocation), `x509`, `plain`, `scram_sha256`, `scram_sha512`, `gssapi`, `http`, `http_api_key` with an [`HttpApiKeyLocation`](api:ruststream::HttpApiKeyLocation), `open_id_connect` and `oauth2`.
[`custom`](api:ruststream::SecurityScheme::custom) takes a scheme object as JSON, and [`description`](api:ruststream::SecurityScheme::description) adds a note.
The example runs with `cargo test --example asyncapi_servers --features macros,memory,asyncapi`.

## Example

{/* single-form: a security scheme is a plain value, the same with and without the macros */}
```rust reference="examples/asyncapi_servers.rs#security"
```
