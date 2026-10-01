<h1 align="center">RustStream</h1>

<p align="center">
  <i>An async messaging framework for Rust: broker-agnostic traits, a router runtime, codecs, AsyncAPI generation, Prometheus and OpenTelemetry observability, and a conformance harness for broker authors.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/powersemmi/ruststream?branch=main"><img src="https://coveralls.io/repos/github/powersemmi/ruststream/badge.svg?branch=main" alt="Coverage"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/v/ruststream.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/dr/ruststream" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream"><img src="https://img.shields.io/docsrs/ruststream" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/unsafe-none-success.svg" alt="100% safe Rust">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
  <a href="https://context7.com/powersemmi/ruststream"><img src="https://img.shields.io/badge/Context7-Ask_AI-ff5722" alt="Ask AI"></a>
  <a href="https://www.greptile.com/?utm_source=oss_badge&amp;utm_medium=readme&amp;utm_campaign=greptile_for_open_source"><img src="https://www.greptile.com/badge.svg" alt="Greptile: The War on Bugs"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream/">Documentation</a></b>
</p>

---

RustStream connects your service to a message broker through a small set of generic traits, then
gives you a router, middleware, codecs, and tooling on top. The core depends on no broker: each
broker is an independent crate held to one contract. The core is 100% safe Rust.

## Features

- **Broker-agnostic core.** Brokers are separate crates, checked by a conformance harness.
- **Misuse does not compile.** Double acks, out-of-order lifecycle calls and under-specified
  publishes are compile errors that name the fix.
- **Pluggable codecs:** JSON, MessagePack, and CBOR behind cargo features, or raw bytes with none.
- **Zero-boilerplate binaries.** `#[subscriber]` and `#[ruststream::app]` macros, and a CLI that
  scaffolds, runs and documents a service.
- **Observability:** AsyncAPI 3.1, Prometheus metrics, a health probe, and OpenTelemetry.
- **Tests without a broker.** The service's own app runs in process under a test harness.
- **Capability traits** for batches, transactions, request-reply, partitioning and repositioning;
  a broker implements only what it supports.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

The CLI ships with the crate behind the `cli` feature:

```bash
cargo install ruststream --features cli
```

## Write a service

```rust
use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| { b.include(handle); })
}
```

`#[ruststream::app]` generates `main`, so there is no runtime boilerplate.

## Run it

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
```

Scaffold a fresh project with `cargo generate --git https://github.com/powersemmi/ruststream
templates/memory --name my-service` (each broker crate ships its own template). See the
[quick start](https://powersemmi.github.io/ruststream/latest/getting-started/quickstart/).

## Test it

`TestApp` runs the service's own app in process, with no external broker.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(service()).await?;

// Inject an order; the harness drives the handler to completion before returning.
tb.broker::<MemoryBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

// The handler ran once, decoded the order, and acked.
tb.broker::<MemoryBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());
```

Full compiling example: `examples/testing.rs`.

## Brokers

| Broker | Crate |
|---|---|
| NATS | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) |
| Redis / Valkey | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) |
| RabbitMQ (AMQP 0.9.1) | [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) |
| Apache Kafka | [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka) |
| AMQP 1.0 | [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp) |
| Google Cloud Pub/Sub | [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub) |
| Amazon SQS / SNS | [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns) |
| Apache Pulsar | [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar) |
| MQTT 5 | [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) |
| Files and stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) |
| Amazon Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) |

What each broker supports is on the
[broker index](https://powersemmi.github.io/ruststream/latest/brokers/). To write a broker, see
the [broker-authors guide](https://powersemmi.github.io/ruststream/latest/broker-authors/).

## Documentation

- Site: <https://powersemmi.github.io/ruststream/latest>
- API reference and topic overviews: <https://docs.rs/ruststream>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024. Raising it is a breaking change. A broker crate may require a
newer toolchain when its client does.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.

<sub>Inspired by [FastStream](https://github.com/ag2ai/faststream).</sub>
