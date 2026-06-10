# AsyncAPI

With the `asyncapi` feature, RustStream generates an [AsyncAPI 3.0](https://www.asyncapi.com/)
document from the application's handlers: each subscriber becomes a channel and a `receive`
operation, and payload types contribute schemas.

```toml
ruststream = { version = "0.2", features = ["macros", "memory", "asyncapi"] }
```

## Generating the document

The quickest path is the CLI, which runs your service's generator and prints the document:

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

In code, build the spec from the application with `build_spec`, then serialize it with `to_json` or
`to_yaml`:

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` wires the `asyncapi gen` command to `build_spec` for you, so the CLI and a
hand-written call produce the same document.

## Payload schemas

A handler's payload type appears as a schema when it derives `JsonSchema`. RustStream re-exports
`schemars`, so you do not need a direct dependency:

```rust
--8<-- "examples/asyncapi_http.rs:payload"
```

A type without `JsonSchema` still works as a handler payload; it just contributes no schema to the
document.

## Servers

Record the servers your service connects to so they appear in the document's `servers` section.
Build a `ServerSpec` directly:

```rust
use ruststream::ServerSpec;

let app = RustStream::new(info)
    .server("production", ServerSpec::new("nats.example.com:4222", "nats"))
    .with_broker(broker, |b| b.include(handle));
```

A broker crate may also implement the `DescribeServer` capability, in which case
`broker.describe_server()` produces the spec for you (none of the shipped brokers do yet).

## Serving the document

Hosting is intentionally not part of the framework. `build_spec` and `to_json` / `to_yaml` give you
the bytes; you mount them in whatever HTTP stack you already run (axum, actix, or any other).

For an interactive viewer, `render_viewer_html` returns a self-contained HTML page that loads the
AsyncAPI React component and points it at your spec URL:

```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

Serve that HTML and the spec JSON from two routes in your own server. By default the viewer loads its
assets from a CDN; override the base URL with `ViewerOptions::with_cdn_base` for offline or
locked-down deployments (`with_title` sets the page title).

## A complete server

The [`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
example serves the document and the viewer with [axum](https://github.com/tokio-rs/axum). Run it with
`cargo run --example asyncapi_http --features macros,memory,asyncapi`, then open
<http://127.0.0.1:8080/>.

```rust
--8<-- "examples/asyncapi_http.rs"
```
