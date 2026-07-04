<h1 align="center">RustStream</h1>

<p align="center">
  <i>An async messaging framework for Rust: broker-agnostic traits, a router runtime, codecs, AsyncAPI generation, Prometheus metrics, and a conformance harness for broker authors.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/v/ruststream.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream"><img src="https://img.shields.io/docsrs/ruststream" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/unsafe-none-success.svg" alt="100% safe Rust">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
  <a href="https://context7.com/powersemmi/ruststream"><img src="https://img.shields.io/badge/Context7-Ask_AI-ff5722" alt="Ask AI"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream/">Documentation</a></b>
</p>

---

RustStream connects your service to a message broker through a small set of generic traits, then
gives you a router, middleware, codecs, and tooling on top. The core depends on no broker, so each
broker is an independent crate held to one contract; broker-specific configuration never leaks into
the framework.

The core is 100% safe Rust: every crate carries `#![forbid(unsafe_code)]` and CI rejects any `unsafe`
block, so the guarantee cannot regress.

## Features

- **Broker-agnostic core.** Just traits and types, zero broker dependencies. Brokers are separate
  crates, and the contract is checked by a conformance harness.
- **Fully async on tokio.** No blocking APIs in the public surface.
- **Subscribers are `Stream`s, not callbacks.** Back-pressure comes for free.
- **Ack consumes `self`.** Double-ack is a compile error.
- **Pluggable codecs:** JSON, MessagePack, and CBOR behind cargo features.
- **Zero-boilerplate binaries.** `#[ruststream::app]` generates `main`; the `ruststream` CLI
  scaffolds projects, runs them, and generates the AsyncAPI document.
- **AsyncAPI 3.0 and Prometheus metrics,** served from your own HTTP stack.
- **Colored console logging** behind the `logging` feature; the generated CLI installs it on `run`,
  with verbosity driven by `RUST_LOG`.
- **Capability traits** for optional features (batch subscribe, transactions, request-reply,
  partitioning); a broker implements only what it supports.

## Install

```toml
[dependencies]
ruststream = { version = "0.5", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
schemars = "1"
```

The CLI ships with the crate behind the `cli` feature:

```bash
cargo install ruststream --features cli
```

## Write a service

```rust
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

`#[ruststream::app]` generates `main`, so there is no runtime boilerplate.

## Injecting dependencies

Declare app state, derive `FromRef`, and take a dependency as a `State<T>` handler argument instead of
reaching through `ctx.state()`. The state is built once in `on_startup`; `#[derive(FromRef)]` makes
each field injectable, so no extractor is written by hand.

```rust
use ruststream::runtime::State;
use ruststream::FromRef;

#[derive(FromRef)]
struct AppState {
    create_order: CreateOrder,
}

#[subscriber("orders")]
async fn handle(order: &Order, State(create_order): State<CreateOrder>) -> HandlerResult {
    create_order.execute(order);
    HandlerResult::Ack
}
```

Full compiling example: `examples/from_context.rs`.

## Run it

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
```

Scaffold a fresh project with `cargo generate --git https://github.com/powersemmi/ruststream
templates/memory --name my-service` (each broker crate ships its own template). See the
[quick start](https://powersemmi.github.io/ruststream/latest/getting-started/quickstart/).

## Testing the service

Unit-test a built service against the in-memory broker, with no external service. `MemoryBroker` is a
real broker here, not a test double: the `TestApp` harness drives it through the same dispatch path
the production runtime uses, so you assert on handler behaviour, middleware, and decoding exactly as
in production.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(service()).await?;

// Inject an order; the harness drives the handler to completion before returning.
tb.broker::<MemoryBroker>()
    .publish("orders", &Order { id: 42 })
    .await?;

// The handler ran once, decoded the order, and acked.
tb.broker::<MemoryBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerResult::Ack);

// It published the matching receipt downstream.
tb.broker::<MemoryBroker>()
    .published::<Receipt>("receipts")
    .assert_called_once()
    .with(&Receipt { order_id: 42 });
```

Full compiling example: `examples/testing.rs`. See the
[testing guide](https://powersemmi.github.io/ruststream/latest/guides/testing/).

## Project documentation

Build the AsyncAPI spec and the interactive viewer HTML programmatically from a built service, then
serve them from your own HTTP stack. The CLI `ruststream asyncapi gen` (see `Run it` above) prints the
same document to stdout; this is the in-process path.

```rust
use ruststream::asyncapi::{build_spec, render_viewer_html, ViewerOptions};

let spec = build_spec(&service()).to_json()?;
let viewer = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
// serve `viewer` at `/` and `spec` at `/asyncapi.json` from your own HTTP stack
```

Full compiling example: `examples/asyncapi_http.rs`.

- Guide and tutorials: <https://powersemmi.github.io/ruststream/latest>
- API reference: <https://docs.rs/ruststream>
- Writing a broker: <https://powersemmi.github.io/ruststream/latest/broker-authors/>

## Ecosystem

- [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats): the NATS broker (Core NATS and
  JetStream).
- [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred): the Redis broker (Redis Streams
  with consumer groups; standalone, cluster, and sentinel topologies) via the `fred` client.
- [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin): the RabbitMQ broker (AMQP
  0.9.1: topology descriptors, native dead-letter and delayed retry, keyed worker lanes, publisher
  confirms and server-side transactions) via the `lapin` client.

Concrete brokers live in their own crates and pull `ruststream` from crates.io.

## Minimum supported Rust version

The MSRV is **1.85** (edition 2024, native `async fn in trait`). CI builds the crate on every
stable toolchain from 1.85 up to current stable, so any floor in that range works.

The policy:

- The published `rust-version` stays at the floor. Raising it is a breaking change (a minor
  version bump pre-1.0) and is reviewed against the broker crates' client requirements at each
  minor release.
- Broker crates (`ruststream-nats`, ...) may require a newer toolchain than the core when their
  underlying clients do; cargo allows a dependent crate to have a stricter floor than its
  dependency. Check the broker crate's own `rust-version` for its floor.

## Contributing

```bash
just check    # fmt, clippy, and feature checks
just test     # the test suite
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.

<sub>Inspired by [FastStream](https://github.com/ag2ai/faststream).</sub>
