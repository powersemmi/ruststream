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
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
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

- **Broker-agnostic core.** Traits and types only, zero broker dependencies. Brokers are separate
  crates, and the contract is checked by a conformance harness.
- **Fully async on tokio.** No blocking APIs in the public surface.
- **Subscribers are `Stream`s, not callbacks.** Back-pressure comes for free.
- **Misuse does not compile.** Ack consumes `self` (no double-ack); the broker lifecycle is a
  ladder of consuming transitions (`connect(self)` yields the connected form, `shutdown(self)`
  a terminal witness), so out-of-order lifecycle calls are compile errors; transactions settle
  by consuming their scope.
- **Publishers pair at startup.** Reply wiring and the `Out(..)` handler parameter attach a
  publish policy where the handler is included; the runtime pairs it against the connected
  broker, so a handler never sees a "not connected" publisher. The parameter states a
  capability (`Out<impl Publisher>`), never a broker type, so the same handler mounts on a
  production broker and its in-process test transport unchanged.
- **Pluggable codecs:** JSON, MessagePack, and CBOR behind cargo features - or none at all:
  `raw` subscribers and `publish_raw` replies move payload bytes untouched.
- **Zero-boilerplate binaries.** `#[ruststream::app]` generates `main`; the `ruststream` CLI
  scaffolds projects, runs them, and generates the AsyncAPI document. Console logging ships
  behind the `logging` feature, installed on `run` with verbosity driven by `RUST_LOG`.
- **AsyncAPI 3.0, Prometheus metrics, and a health probe,** served from your own HTTP stack.
- **OpenTelemetry** behind the `otel` feature: OTLP export for traces and metrics, per-handler
  dispatch metrics following the messaging semantic conventions, and W3C trace-context
  propagation across the consume-transform-produce chain.
- **Capability traits** for optional features (batch subscribe, borrowed and owned transactions,
  request-reply, partitioning, repositioning a live subscription in a replayable log); a broker
  implements only what it supports.

## Install

```toml
[dependencies]
ruststream = { version = "0.6", features = ["macros", "memory", "json"] }
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
use ruststream::prelude::*;
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

Unit-test a built service against the in-memory broker, with no external service. The `TestApp`
harness drives it through the same dispatch path the production runtime uses, so you assert on
handler behaviour, middleware, and decoding exactly as in production; what the memory broker does
and does not model is on
[its own page](https://powersemmi.github.io/ruststream/latest/brokers/memory/).

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
- [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka): the Apache Kafka broker
  (consumer groups, tracked and transactional commits, retry and dead-letter topics, partition-scoped
  transactions and exactly-once pipelines, a service template) via the `rdkafka` client.
- [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp): the AMQP 1.0 broker (ActiveMQ
  Artemis, RabbitMQ 4.x, Azure Service Bus, and the wider AMQP 1.0 family; request/reply and
  transactions) via the `fe2o3-amqp` client.
- [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub): the Google Cloud
  Pub/Sub broker (ordering keys, exactly-once acknowledgement, dead-letter policies) via the
  official `google-cloud-pubsub` client.
- [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns): the Amazon SQS broker
  with SNS fan-out (FIFO groups, visibility management, native deferred retry) via the AWS SDK.
- [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar): the Apache Pulsar broker
  (subscription modes, patterns, dead-letter policies, repositioning) via the `pulsar` client.
- [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc): the MQTT 5 broker (QoS
  levels, shared groups, retained messages) via the `rumqttc` client.
- [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq): the brokerless ZeroMQ
  transport (PUSH/PULL, PUB/SUB, and DEALER/ROUTER request/reply over TCP and IPC) via the
  pure-Rust `zeromq` client.
- [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file): the file and stdio
  transport (persistent replayable stream files, shell pipelines, repositioning) via the
  `sea-streamer` clients.
- [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis): the Amazon Kinesis
  broker (shard leasing, checkpointing, repositioning) via the AWS SDK.

Concrete brokers live in their own crates and pull `ruststream` from crates.io.

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024. CI builds and tests the crate on the floor and on current
stable, and builds it on beta; Rust's stability guarantee carries the releases in between, so any
floor at or above 1.88 works.

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
