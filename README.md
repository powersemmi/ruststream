<h1 align="center">RustStream</h1>

<p align="center">
  <b>English</b> | <a href="https://github.com/powersemmi/ruststream/blob/main/docs/readme/README.ru.md">Русский</a> | <a href="https://github.com/powersemmi/ruststream/blob/main/docs/readme/README.zh.md">中文</a>
</p>

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
  <a href="https://context7.com/powersemmi/ruststream"><img src="https://img.shields.io/badge/Context7-Ask_AI-6200ee" alt="Ask AI"></a>
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
- **Three runtimes.** `tokio` (the default), `compio` with one runtime per core on `io_uring` where
  the kernel has it, and `embassy` for a device, with or without a heap. The handlers are the same
  on each, and the app picks its runner.
- **Pluggable codecs:** JSON, MessagePack and CBOR behind cargo features, postcard and JSON over
  `serde-json-core` for a device without a heap, or raw bytes with none.
- **Zero-boilerplate binaries.** `#[subscriber]` and `#[ruststream::app]` macros, and a CLI that
  runs a service and prints its AsyncAPI document.
- **Observability:** AsyncAPI 3.1, Prometheus metrics, a health probe, and OpenTelemetry.
- **Tests without a broker.** The service's own app runs in process under a test harness.
- **Publish errors keep the message.** A refused publish hands the unsent message back wherever
  the broker's client did not take the payload, so the caller can send it elsewhere.
- **Capability traits** for batches, transactions, request-reply, partitioning and repositioning;
  a broker implements only what it supports.

## Install

```toml
[dependencies]
ruststream = { version = "0.8", features = ["macros", "memory", "json", "logging"] }
serde = { version = "1", features = ["derive"] }
```

The CLI ships with the crate behind the `cli` feature:

```bash
cargo install ruststream --features cli
```

## Agent skills

The repository ships skills that teach a coding agent to write RustStream services, their tests
and broker crates the recommended way. In Claude Code:

```text
/plugin marketplace add powersemmi/ruststream
/plugin install ruststream@ruststream
```

Cursor and Codex read them from
[`.agents/skills/`](https://github.com/powersemmi/ruststream/tree/main/.agents/skills); the
[Agent skills](https://powersemmi.github.io/ruststream/latest/guides/getting-started/agent-skills/)
page lists them.

## Write a service

```rust
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
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
        .logger(TracingLogger)
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

`#[ruststream::app]` generates `main`, so there is no runtime boilerplate. `.logger(TracingLogger)`
sends the framework's log to `tracing`, and the `logging` feature prints it on the terminal. The
handler needs only `Deserialize`; the other derives are for the test below.

## Run it

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
```

The service logs its startup and runs until you press <kbd>Ctrl</kbd>+<kbd>C</kbd> (the timestamp
that opens each line is left out here):

```text
INFO ruststream::lifecycle starting service service=orders version=0.1.0 brokers=1 subscribers=1
INFO ruststream::lifecycle broker connected broker="ruststream_memory::broker::MemoryBroker"
INFO ruststream::dispatch subscriber started subscriber=orders input="orders::Order"
INFO ruststream::lifecycle service running subscribers=1
```

The in-memory broker carries messages inside the process: the handler prints `got order` for an
order the service itself publishes to `orders`, and the test below places one.

`asyncapi gen` needs the `asyncapi` feature of `ruststream`.

To start from a template instead, scaffold a project with `cargo generate`:

```bash
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
```

`templates/compio-memory` starts a service on the compio runtime, and `templates/embassy-memory` a
Cortex-M firmware on embassy with no heap. Each template ships tests that run its app in process.
Several broker crates ship templates of their own and name them in their documentation. The
[quick start](https://powersemmi.github.io/ruststream/latest/guides/getting-started/quickstart/)
walks through the first run.

## Test it

`TestApp` runs the service's own app in process, with no external broker. It comes with the
`testing` feature, which a service turns on for its tests:

```toml
[dev-dependencies]
ruststream = { version = "0.8", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The test places an order through the broker and checks how the handler settled it:

```rust
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Inject an order; the publish drives the handler to a standstill before returning.
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
        Ok(())
    }
}
```

The [testing](https://powersemmi.github.io/ruststream/latest/components/application/testing/)
pages cover the rest of the harness.

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
| MQTT 5 on embassy and compio, with a device variant without a heap | [`ruststream-rust-mqtt`](https://github.com/powersemmi/ruststream-rust-mqtt) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) |
| Files and stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) |
| Amazon Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) |
| SQL databases (Postgres, MySQL / MariaDB, SQLite) | [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx) |

What each broker supports is on the
[broker index](https://powersemmi.github.io/ruststream/latest/brokers/). To write a broker, start at
the [`Broker` trait](https://docs.rs/ruststream/latest/ruststream/trait.Broker.html).

## Documentation

- Site: <https://powersemmi.github.io/ruststream/latest>
- API reference and topic overviews: <https://docs.rs/ruststream>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024. The policy is a rolling one, as in tower: the MSRV rises only
to a Rust release at least six months old, and only in a minor release. A broker crate may require
a newer toolchain when its client does.

## Contributing

See [CONTRIBUTING.md](https://github.com/powersemmi/ruststream/blob/main/CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](https://github.com/powersemmi/ruststream/blob/main/LICENSE) license.

<sub>Inspired by [FastStream](https://github.com/ag2ai/faststream).</sub>
