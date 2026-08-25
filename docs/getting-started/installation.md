# Installation

RustStream ships as a single crate, `ruststream`, whose surface is gated behind additive cargo
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
    `edition = "2024"` in your `Cargo.toml`. CI builds and tests the crate on the floor and on
    current stable, and builds it on beta, so any floor at or above 1.88 works.
    Broker crates may require a newer toolchain than the core when their underlying clients do;
    check the broker crate's own `rust-version`.

## Features

The core traits, the `RustStream` application object, the `Router`, middleware, and dispatch are
always compiled. Everything else is an additive, opt-in feature.

| Feature | Pulls in | What it gives you |
|---|---|---|
| `json` *(default)* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | - | `MemoryBroker`, the in-memory reference broker |
| `macros` | ruststream-macros | `#[subscriber]`, `#[ruststream::app]`, `#[derive(Message)]` |
| `asyncapi` | schemars, serde_norway | AsyncAPI generation and the HTML viewer |
| `metrics` | prometheus | Prometheus middleware and exporter |
| `logging` | tracing-subscriber | `ruststream::logging`, a colored console logger ([Logging](../guides/logging.md)) |
| `otel` | opentelemetry, opentelemetry-otlp | OTLP export for traces and metrics, and W3C trace-context propagation ([OpenTelemetry](../guides/opentelemetry.md)) |
| `testing` | inventory | `TestApp` and the assertion builders ([Testing](../guides/testing.md)) |
| `conformance` | - | the broker-author conformance harness |
| `cli` | clap, anyhow | the `ruststream` binary |

Codec features are mutually compatible; enable as many as you need (see
[Codecs](../guides/codecs.md)). To drop the bundled JSON codec
(for a broker crate that only needs the trait surface and runtime), disable defaults:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## The CLI

The optional `ruststream` binary ships with the crate behind the `cli` cargo feature and drives
`cargo` with the framework's subcommands (`run`, `asyncapi gen`); installation and commands are in
the [CLI guide](../guides/cli.md). Scaffolding a new project does not need it - that is `cargo
generate` against a template, covered in the [quick start](quickstart.md).

## Concrete brokers

The `memory` broker is built in and needs no external service. To reach a broker outside the
process, depend on a broker crate, which re-exports what it needs from `ruststream`. Each broker is
versioned and released independently, so its own documentation carries the exact dependency line
(including the current version and the `testing` feature for handler tests) alongside its `Config`
and capabilities.

The available brokers are listed under [Brokers](../brokers/index.md); follow the link there to each
broker's documentation for installation. To write one yourself, see
[Broker authors](../broker-authors/index.md).
