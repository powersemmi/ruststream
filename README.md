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
ruststream = { version = "0.3", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
schemars = "0.8"
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

## Run it

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
ruststream new my-service      # scaffold a new project
```

## Test it

Unit-test handlers against the in-memory broker, with no external service. It does core routing
only, so you assert on handler behaviour, middleware, and decoding exactly as in production. See the
[testing guide](https://powersemmi.github.io/ruststream/guides/testing/).

## Documentation

- Guide and tutorials: <https://powersemmi.github.io/ruststream/latest>
- API reference: <https://docs.rs/ruststream>
- Writing a broker: <https://powersemmi.github.io/ruststream/latest/broker-authors/>

## Ecosystem

- [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats): the NATS broker (Core NATS and
  JetStream).
- [`ruststream-py`](https://github.com/powersemmi/ruststream-py): Python bindings.

Concrete brokers live in their own crates and pull `ruststream` from crates.io.

## Contributing

```bash
just check    # fmt, clippy, and feature checks
just test     # the test suite
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.

<sub>Inspired by [FastStream](https://github.com/ag2ai/faststream).</sub>
