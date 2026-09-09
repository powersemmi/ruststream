# AsyncAPI

With the `asyncapi` feature, RustStream generates an [AsyncAPI 3.0](https://www.asyncapi.com/)
document from the application's handlers. Each subscriber becomes a channel and a `receive`
operation, and payload types contribute schemas. Several handlers can share one channel. The
document then shows one operation per handler: each has its own subscription.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "asyncapi"] }
```

## Generating the document

The quickest path is the CLI. It runs your service's generator and prints the document:

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

In code, `build_spec` builds the specification from the application, and `to_json` or `to_yaml`
serializes it:

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` connects the `asyncapi gen` command to `build_spec` for you, so the CLI and a
call from your own code produce the same document.

## Payload schemas

A handler's payload type appears as a schema when it derives `JsonSchema`. RustStream re-exports
`schemars`, so you do not need a direct dependency:

=== "Macros"

    ```rust
    --8<-- "examples/asyncapi_http.rs:payload"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:payload"
    ```

On the `#[subscriber]` path a type without `JsonSchema` still works as a handler payload, but
contributes no schema to the document. The generator logs a `WARN` for every such gap: one per
handler or per declared outgoing message, naming the subscription or the channel and the type.
`Spec::messages_without_schema()` lists the affected message components. Assert in a test that the
list is empty, and CI will not let a message without a schema through.

A manual registration, the `subscriber(..)` chain, is stricter. It is documented by default, so
under the `asyncapi` feature it demands a schema from its message types. On this path a type
without `JsonSchema` gives a compile error, and the error names the missing derive.
`.undocumented()` takes one registration out of the document and lifts its schema obligation.

A message with its own wire format is the deliberate exception. A
[`Deserialized`](subscribers.md#raw-subscribers) input and an outgoing message published already
serialized (a [`#[derive(Serialized)]`](subscribers.md#raw-subscribers) reply, or a `Serialized`
member of a slot's `#[publishes(..)]` list) appear in the document under their own names and
without a payload schema. The generator does not warn about them, and `messages_without_schema()`
does not list them: the bytes are the format, so a schema would have nothing to say.

Beyond payloads, the document also contains **headers schemas** (from a handler's `Headers<T>`
parameter or from a contract declared on the message type itself) and **`send` operations** for
every declared outgoing message: the reply of a `publish(..)` form and every message type an `Out`
slot declares. See [typed headers](headers.md).

A message type whose declared name is a template (`#[outgoing(name = "orders.{tenant}.v1")]`)
appears at that templated address, and its placeholders fill the channel's **parameters** block. A
type that declares no destination contributes no channel. See
[publishing](publishing.md#declaring-where-a-message-goes).

## Message names and descriptions

A payload type with a schema defines its message component itself: the type's doc comment becomes
the message description, and `#[schemars(title = "...")]` or a rename names the component. Without
a schema, the component is named after the payload type, and the description comes from the
handler's doc comment, which also describes the `receive` operation. In the manual chain,
`.describe(..)` sets the operation's description.

You can set a message's metadata explicitly, including for a type without `JsonSchema`, through the
`MessageInfo` trait: it takes precedence over the schema. The `MessageInfo` derive uses the type's
name and its doc comment:

<!-- inline-rust: minimal MessageInfo-derive sketch; the compiled form (asyncapi_http.rs:payload) also derives JsonSchema, which would obscure the point that MessageInfo takes precedence over the schema -->
```rust
use ruststream::MessageInfo;

/// An order placed by a customer.
#[derive(MessageInfo, serde::Deserialize)]
struct Order {
    id: u64,
}
// In the document: components.messages.Order with that description.
```

A manual `impl MessageInfo` can name the component differently from the Rust type
(`const NAME: &'static str = "CustomOrder";`), so renaming the type does not change the wire
contract.

## Servers

Describe the servers your service connects to so they appear in the document's `servers` section.
You build a `ServerSpec` directly:

=== "Macros"

    ```rust
    --8<-- "examples/asyncapi_http.rs:server"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:server"
    ```

A broker crate may implement the `DescribeServer` capability. Then `broker.describe_server()`
produces the server specification, and `with_broker_labeled` records it under the broker's label.
Every shipped broker has this capability.

## Server security

`ServerSpec::with_security` declares how clients authenticate. Each scheme goes into
`components.securitySchemes`, and the server's `security` list references it:

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

`SecurityScheme` has constructors for the AsyncAPI 3.0 scheme kinds: `user_password`, `plain`,
`scram_sha256` / `scram_sha512`, `gssapi`, `api_key`, `x509`, `http`, `http_api_key`,
`open_id_connect`, and `oauth2`, which takes the flows object as raw JSON. A scheme that is not
among them is declared with `SecurityScheme::custom(json)`.

Security is the service author's statement, not the broker's: `DescribeServer` never reports it. To
secure a server the broker registered automatically (`with_broker_labeled`), declare that server
explicitly: `.server(label, broker.describe_server().with_security(..))` with the same label.

## Serving the document

`build_spec` and `to_json` / `to_yaml` give you the document's bytes, and you serve them with the
HTTP stack you already run: axum, actix, or any other.

`render_viewer_html` returns an interactive viewer: a self-contained HTML page that loads the
AsyncAPI React component and shows your specification from its URL:

<!-- inline-rust: two-line API-shape fragment; the compiled call lives in asyncapi_http.rs:generate -->
```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

Serve that HTML and the specification JSON from two routes of your own server. By default the
viewer loads its assets from a CDN. For an offline or locked-down deployment you can set another
base URL with `ViewerOptions::with_cdn_base`, and `with_title` sets the page title.

## A complete server

The [`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
example serves the document and the viewer with [axum](https://github.com/tokio-rs/axum). Run it
with `cargo run --example asyncapi_http --features macros,memory,asyncapi` and open
<http://127.0.0.1:8080/>.

=== "Macros"

    ```rust
    --8<-- "examples/asyncapi_http.rs"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs"
    ```
