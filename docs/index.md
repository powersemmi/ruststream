# RustStream

**RustStream** is an async messaging framework for Rust: a broker-agnostic core of traits and a
router runtime, plus codecs, AsyncAPI generation, Prometheus metrics, and a conformance harness for
broker authors.

Two architectural commitments shape the framework:

1. **A real interface for third-party brokers.** The core holds only traits and types, with zero
   broker dependencies. Each broker is an independent crate, and the contract is checked by the
   `conformance` harness.
2. **Broker-specific config stays in broker crates.** The core carries no broker-specific config or
   defaults. Each broker crate owns its own `Config`, so an upstream change hits one broker crate,
   not the framework.

```rust
--8<-- "examples/quickstart.rs"
```

`#[ruststream::app]` generates `main`, so `cargo run -- run` starts the service and
`cargo run -- asyncapi gen` prints its AsyncAPI document - no runtime boilerplate.

## Design principles

- **Fully async, tokio-based.** No blocking APIs in the public surface.
- **Generic core, no `dyn` in the contract.** Associated types and native `async fn in trait`;
  object-safe erasure lives in consumers (the Python bindings), not the core.
- **Subscribers are `Stream`s, not callbacks.** Back-pressure for free; the callback DX is built on
  top in the runtime.
- **Ack consumes `self`.** You cannot ack twice - the compiler enforces it.
- **Capability traits for optional features** (`BatchSubscriber`, `TransactionalPublisher`,
  `RequestReply`, `Partitioned`) - never forced into the mandatory interface.

## Where to go next

<div class="grid cards" markdown>

- :material-download: **[Installation](getting-started/installation.md)** - features and crate setup.
- :material-rocket-launch: **[Quick start](getting-started/quickstart.md)** - scaffold a service with `cargo generate`.
- :material-school: **[Tutorial](getting-started/tutorial.md)** - build a service step by step.
- :material-test-tube: **[Testing](guides/testing.md)** - test handlers in-process, no server needed.
- :material-transit-connection-variant: **[Brokers](brokers/index.md)** - the in-memory broker, NATS, and Redis.
- :material-server-network: **[Broker authors](broker-authors/index.md)** - implement the contract and pass conformance.

</div>

## Scope of this repository

This site documents `ruststream-rs`, the pure-Rust core (no PyO3, no concrete brokers). Concrete
brokers (NATS, Kafka, RabbitMQ, Redis, MQTT) live in their own crates and pull `ruststream` from
crates.io. The Python bindings live in the
[`ruststream-py`](https://github.com/powersemmi/ruststream-py) repository.

The Rust API reference is published on [docs.rs](https://docs.rs/ruststream) - see
[API reference](reference.md).
