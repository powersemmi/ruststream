# RustStream

**RustStream** subscribes a Rust service to event streams and publishes messages to them. The
service is not bound to one message broker. The core is traits and a router runtime. Codecs,
AsyncAPI generation, Prometheus metrics, and a conformance harness for broker authors ship with it.

Two architectural commitments shape the framework:

1. **A real interface for third-party brokers.** The core holds only traits and types, with zero
   broker dependencies. Each broker is an independent crate. The `conformance` harness checks the
   contract.
2. **Broker-specific config stays in broker crates.** The core carries no broker-specific config or
   defaults. Each broker crate owns its own `Config`. An upstream change affects only that crate,
   not the framework.

=== "Macros"

    ```rust
    --8<-- "examples/quickstart.rs"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/quickstart.rs"
    ```

`#[ruststream::app]` generates `main` with all the runtime boilerplate. `cargo run -- run` starts
the service, and `cargo run -- asyncapi gen` prints its AsyncAPI document.

## Design principles

- **Fully async, tokio-based.** The public API has no blocking calls.
- **Generic core, no `dyn` in the contract.** The contract is built on associated types and native
  `async fn in trait`. The runtime performs type erasure where a service needs it.
- **Subscribers are `Stream`s, not callbacks.** The `Stream` itself provides back-pressure. The
  runtime builds callbacks on top of it.
- **Ack consumes `self`.** A second ack is a compile error.
- **Capability traits for optional features.** `BatchSubscriber`, `TransactionalPublisher`,
  `RequestReply`, `Partitioned`, and `Seekable` are not part of the mandatory interface.

## Where to go next

<div class="grid cards" markdown>

- :material-download: **[Installation](getting-started/installation.md)** - features and crate setup.
- :material-rocket-launch: **[Quick start](getting-started/quickstart.md)** - scaffold a service with `cargo generate`.
- :material-school: **[Tutorial](getting-started/tutorial.md)** - build a service step by step.
- :material-test-tube: **[Testing](guides/testing.md)** - test handlers in-process, no server needed.
- :material-web: **[HTTP frameworks](guides/http.md)** - run beside axum with a transactional outbox.
- :material-transit-connection-variant: **[Brokers](brokers/index.md)** - the in-memory broker and the broker crates.
- :material-server-network: **[Broker authors](broker-authors/index.md)** - implement the contract and pass conformance.

</div>

## Scope of this repository

This site documents `ruststream`, the broker-agnostic core crate. Concrete brokers (NATS, Kafka,
RabbitMQ, Redis, MQTT, and more) ship as separate crates. Each of them depends on `ruststream` from
crates.io.

The Rust API reference is published on [docs.rs](https://docs.rs/ruststream) - see
[API reference](reference.md).
