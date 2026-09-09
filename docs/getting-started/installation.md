# Installation

RustStream ships as a single crate, `ruststream`, whose surface is enabled by additive cargo
features. Add it to your `Cargo.toml`:

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` is a direct dependency of your service because your message types derive `Deserialize` /
`Serialize`.

!!! note "Edition and MSRV"
    RustStream targets **edition 2024** and a minimum supported Rust version of **1.88**. Set
    `edition = "2024"` in your `Cargo.toml`.
    A broker crate may need a newer Rust than the core when its client library does. The broker
    crate's own `rust-version` field states the exact minimum version.

## Features

The core traits, the `RustStream` application object, the `Router`, middleware and message dispatch
are always compiled. Everything else is an additive feature you enable when you need it.

| Feature | Pulls in | What it gives you |
|---|---|---|
| `json` *(default)* | `serde_json` | `JsonCodec` |
| `msgpack` | `rmp-serde` | `MsgpackCodec` |
| `cbor` | `ciborium` | `CborCodec` |
| `memory` | - | `MemoryBroker`, the in-memory reference broker |
| `macros` | `ruststream-macros` | `#[subscriber]`, `#[ruststream::app]`, and the derives (`Outgoing`, `OutSlot`, `OutMessages`, `Deserialized`, `Serialized`, `FromRef`, `MessageInfo`) |
| `asyncapi` | `schemars`, `serde_norway` | AsyncAPI generation and the HTML viewer |
| `metrics` | `prometheus` | Prometheus middleware and exporter |
| `logging` | `tracing-subscriber` | `ruststream::logging`, a colored console logger ([Logging](../guides/logging.md)) |
| `otel` | `opentelemetry`, `opentelemetry-otlp` | OTLP export for traces and metrics, and W3C trace-context propagation ([OpenTelemetry](../guides/opentelemetry.md)) |
| `testing` | `inventory` | `TestApp` and the assertion builders ([Testing](../guides/testing.md)) |
| `conformance` | `inventory` | the broker-author conformance harness |
| `cli` | `clap`, `anyhow` | the `ruststream` binary |

You can enable several codecs at once (see [Codecs](../guides/codecs.md)). To drop the bundled JSON
codec (for example in a broker crate that needs only the traits and the runtime), disable the
default features:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## The CLI

The `ruststream` binary ships with the crate behind the `cli` cargo feature. It runs `cargo` with
the framework's subcommands (`run`, `asyncapi gen`); installation and commands are in the
[CLI guide](../guides/cli.md). `cargo generate` scaffolds a new project from a template, covered in
the [quick start](quickstart.md).

## Concrete brokers

The `memory` broker is built into the crate and needs no external service. To reach a broker outside
the process, depend on that broker's crate; it re-exports from `ruststream` everything it needs.

Each broker has its own version and its own release cycle, so the exact dependency line, with the
current version and the `testing` feature for handler tests, is in the broker's own documentation.
The same documentation describes the broker's `Config` and its capabilities.

The available brokers are listed under [Brokers](../brokers/index.md); the link there leads to each
broker's documentation and its installation instructions. To write your own broker, see
[Broker authors](../broker-authors/index.md).
