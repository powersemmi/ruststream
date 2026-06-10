# Installation

RustStream ships as a single crate, `ruststream`, whose surface is gated behind additive cargo
features. Add it to your `Cargo.toml`:

```toml
[dependencies]
ruststream = { version = "0.2", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` is a direct dependency of your service because your message types derive `Deserialize` /
`Serialize`.

!!! note "Edition and MSRV"
    RustStream targets **edition 2024** and a minimum supported Rust version of **1.85** (native
    `async fn in trait`). Set `edition = "2024"` in your `Cargo.toml`.

## Features

The core traits, the `RustStream` application object, the `Router`, middleware, and dispatch are
always compiled. Everything else is an additive, opt-in feature.

| Feature | Pulls in | What it gives you |
|---|---|---|
| `json` *(default)* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | tokio-stream | `MemoryBroker`, the in-memory reference broker |
| `macros` | ruststream-macros | `#[subscriber]`, `#[ruststream::app]`, `#[derive(Message)]` |
| `asyncapi` | schemars, serde_norway | AsyncAPI generation and the HTML viewer |
| `metrics` | prometheus | Prometheus middleware and exporter |
| `logging` | tracing-subscriber | `ruststream::logging`, a colored console logger ([Logging](../guides/logging.md)) |
| `conformance` | - | the broker-author conformance harness |
| `cli` | clap, anyhow | the `ruststream` binary |

Codec features are mutually compatible; enable as many as you need (see
[Codecs](../guides/codecs.md)). To drop the bundled JSON codec
(for a broker crate that only needs the trait surface and runtime), disable defaults:

```toml
[dependencies]
ruststream = { version = "0.2", default-features = false }
```

## The CLI

The `ruststream` binary ships with the crate behind the `cli` feature. Install it to scaffold
projects and drive `cargo` with the framework's subcommands:

```bash
cargo install ruststream --features cli
```

See the [CLI guide](../guides/cli.md), or jump straight to the [quick start](quickstart.md).

## Concrete brokers

The `memory` broker is for local development and tests. For production, depend on a broker crate,
which re-exports what it needs from `ruststream`:

```toml
[dependencies]
ruststream-nats = "0.2"

[dev-dependencies]
# the broker's in-memory test client, for handler tests
ruststream-nats = { version = "0.2", features = ["testing"] }
```

Each broker crate documents its own `Config` and capabilities; the available brokers are listed
under [Brokers](../brokers/index.md). To write one yourself, see
[Broker authors](../broker-authors/index.md).
