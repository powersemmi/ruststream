---
name: ruststream-broker
description: "Use when writing, changing or debugging a RustStream broker crate, the adapter between `ruststream` and a broker client: the connect and shutdown lifecycle, publishers and per-message options, subscriptions and retry copies, batching and other capabilities, the crate prelude, the in-process testing mode and the conformance suites. Not for services that use an existing broker crate."
---

# Writing a RustStream broker crate

A broker crate depends on `ruststream = { version = "0.7", default-features = false }` and
implements the core's traits over one client library. The contract is checked by the
`conformance` suites, which run in process and against a real server. The hub of the contract is
the [`Broker` trait on docs.rs](https://docs.rs/ruststream/latest/ruststream/trait.Broker.html);
`MemoryBroker` (`src/memory/` of the core) is the reference implementation, and
[`ruststream-nats`](https://docs.rs/ruststream-nats/latest/ruststream_nats/) is a worked crate over
a real client. This guidance targets the 0.7 line; a broker crate's version tracks the core's
minor line.

Depth:

- [references/contract.md](references/contract.md): every rule of the contract, one per topic
- [references/code.md](references/code.md): the shapes of the main impls, checked against the core's signatures

## The order of work

1. **The lifecycle ladder.** `MyBroker::new(addrs)` is synchronous and does no I/O.
   `Broker::connect(self) -> Result<Connected, Error>` opens the client;
   `ConnectedBroker::shutdown(self) -> Result<Closed, Error>` tears it down and never blocks or
   panics. Keep `tokio::runtime::Handle::current()` from `connect` in the connected form and spawn
   the broker's own tasks (timers, reply dispatchers) through it.
2. **Deliveries.** `Subscriber::stream(&mut self)` yields `IncomingMessage`s. `ack(self)` and
   `nack(self, requeue)` consume the delivery; a transport without acks answers
   `AckError::Unsupported`. Override `redelivery_count(&self) -> Option<u64>` where the transport
   counts deliveries (the first delivery answers 1). The trait has no error type of its own.
3. **Subscribing.** `Subscribe` on the connected form for `#[subscriber("name")]`, and a
   descriptor implementing `SubscriptionSource<Connected>` for the broker's own options. Both
   declare `type Copies`: `AddressedCopies` (implement `RedeliveryAddressed` on the descriptor),
   `NamedCopies`, or `BrokerMoves`.
4. **Publishing.** A policy type (configuration only) implements `PublishPolicy<Connected>`, whose
   async `pair` builds the live `Publisher`. `DefaultPublish` on the connected form names the
   policy replies and retry copies use. Per-message settings are `Publisher::Options`, adjusted
   by a step trait over `PublishBuilder`.
5. **Capabilities**, only where the transport has them: `BatchSubscriber` (offer it, assembled with
   the core's `BufferedSubscriber` when the transport delivers one at a time),
   `TransactionalPublisher`, `OwnedTransactions`, `RequestReply`, `Partitioned`,
   `Seekable` / `Seeker`, `Positioned`, `Carries`, `DescribeServer`.
6. **The prelude**: `pub use ruststream::prelude::*;`, the crate's service-facing types, and the
   policies under the uniform names `Publish`, `TransactionalPublish`, `Request`.
7. **The in-process mode** under a `testing` feature: `InProcess` on the production broker,
   `TestableBroker` on its connected form, `register_testable_broker!(MyBroker)`.
8. **Conformance**, in process and live, then the author checklist at the end of the
   [conformance overview](https://docs.rs/ruststream/latest/ruststream/conformance/index.html).

## Rules that are easy to get wrong

### `pair` is async and fallible

Wrong: `fn pair(self, conn: &Connected) -> MyPublisher`.

Correct: `async fn pair(self, connected: &Connected) -> Result<MyPublisher, PairError>`, with
`type Live = MyPublisher;`. `DefaultPublish` is `impl DefaultPublish for Connected { type Policy = MyPublish; }`,
and the policy is `Default`.

### Every subscription form declares where retry copies go

`type Copies` on `Subscribe` and on each `SubscriptionSource`. `AddressedCopies` pairs with
`impl RedeliveryAddressed<Connected> for MyOptions`, whose async `redelivery_address` answers the
name a publish uses to reach the subscription again. `BrokerMoves` is for a server that moves
the delivery itself (a dead-letter exchange, a redrive policy). `declare_retry` hands every
descriptor the registration's `max_attempts` and `dead_letter` before it subscribes; apply them
where the server has a mechanism for both.

### Never succeed where the server fails

A publisher made before `shutdown` errors after it; a value of `Options` the transport cannot
honour is a publish error; the in-process transport rejects what the server rejects and keeps
what the server keeps (`Backlog::Delivered` for a queue or a log). A silent success is a defect.

### A setting is an option, never a header or a wrapper

A priority, a QoS or an ordering key is a field of `Publisher::Options` (every field optional)
and a step on `PublishBuilder` bounded on `PublishSink<Options = MyOptions>`. It never rides a
header and never wraps the publisher: a wrapper loses the mount site's codec and the harness's
attribution.

### Run the suites with all their arguments

```rust
use ruststream::conformance::harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routing() {
    // In process: the production broker, connected through `connect_in_process`.
    harness::run_suite(|| MyBroker::new("my://localhost")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle() {
    let Ok(url) = std::env::var("MY_BROKER_TEST_URL") else { return };
    // Closures, not method paths: the bounds are higher-ranked.
    harness::lifecycle(
        move || MyBroker::new(url.clone()),
        |name| MyOptions::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
```

### Report a coordinate, never credentials

`DescribeServer` builds its `ServerSpec` with `ServerSpec::from_url` (`host_from_url` per address),
which drops the userinfo. The generated AsyncAPI document is published.

## Common mistakes

| Symptom | Fix |
|---|---|
| `async fn new(..)` that connects | sync `new`, I/O in `connect` |
| `tokio::spawn` in a publish or settle path | spawn through the `Handle` kept from `connect` |
| `redelivery_address` written on `SubscriptionSource` | `impl RedeliveryAddressed` with `type Copies = AddressedCopies` |
| a `pub use` in the prelude that shadows a core name | rename it; an explicit re-export silently beats the glob |
| an emulated capability | implement only what the transport supports |
| a hand-written test double broker | the production broker's `InProcess` mode |
