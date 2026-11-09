---
name: ruststream-broker
description: "Use when writing, changing or debugging a RustStream broker crate, the adapter between `ruststream` and a broker client: the connect and shutdown lifecycle, publishers and per-message options, subscriptions and retry copies, batching and other capabilities, the crate prelude, the in-process testing mode and the conformance suites. Not for services that use an existing broker crate."
---

# Writing a RustStream broker crate

A broker crate implements the core's traits over one client library. The contract is checked by
the `conformance` suites, which run in process and against a real server. The route through it is
the rustdoc of the [`Broker` trait on docs.rs](https://docs.rs/ruststream/latest/ruststream/trait.Broker.html);
this skill follows the same order and keeps the depth in its references. `MemoryBroker`
(`crates/memory` of the core) is the reference implementation, and
[`ruststream-nats`](https://docs.rs/ruststream-nats/latest/ruststream_nats/) is a worked crate over
a real client. This guidance targets the 0.8 line; a broker crate's version tracks the core's
minor line.

Depth:

- [references/contract.md](references/contract.md): every rule of the contract, one per topic
- [references/code.md](references/code.md): the shapes of the main impls, checked against the core's signatures

## The order of work

1. **The crate and its dependencies.** The crate depends on the facade alone:
   `ruststream = { version = "0.8", default-features = false, features = ["tokio"] }`. The runner
   feature is part of the line; a crate whose client holds `Rc` or `RefCell` serves the embedded
   runner instead (see below). A `testing = ["ruststream/testing"]` feature carries the in-process
   mode, `asyncapi` forwards `ruststream/asyncapi`, and `conformance` is a dev-dependency:
   `ruststream = { version = "0.8", features = ["conformance", "testing", "macros", "json"] }`.
2. **The lifecycle.** `MyBroker::new(addrs)` is synchronous and does no I/O.
   `Broker::connect(self) -> Result<Connected, Error>` opens the client;
   `ConnectedBroker::shutdown(self) -> Result<Closed, Error>` tears it down and never blocks or
   panics. A handle that outlives the connection (a publisher made earlier, a clone) errors when
   used after shutdown. Keep `tokio::runtime::Handle::current()` from `connect` in the connected
   form and spawn the broker's own tasks (timers, reply dispatchers) through it.
3. **Subscribing.** `Subscribe` on the connected form for `#[subscriber("name")]`, and a
   descriptor implementing `SubscriptionSource<Connected>` for the broker's own options. Both
   declare `type Copies`: `AddressedCopies` (implement `RedeliveryAddressed` on the descriptor),
   `NamedCopies`, or `BrokerMoves`.
4. **The delivery.** `Subscriber::stream(&mut self)` yields deliveries. A delivery implements
   `IncomingMessage` (payload, headers, `partition_key`, `redelivery_count`, `decode_error`; no
   `Send` supertrait) and `Settle: IncomingMessage` (`ack(self)`, `nack(self, requeue)`,
   `supports_nack_after`, `nack_after`). A transport without acks answers
   `AckError::Unsupported`. Override `redelivery_count(&self) -> Option<u64>` where the transport
   counts deliveries (the first delivery answers 1). A device broker whose deliveries wait in a
   channel or a socket returns a `ReadyHint` from `Subscriber::ready_hint`, and
   `harness::local_readiness` holds it to that.
5. **Publishing.** A policy type (configuration only) implements `PublishPolicy<Connected>`, whose
   async `pair` builds the live `Publisher`; `Publisher: Clone`. `Publisher::Payload` is `Lend`
   (the client only reads the bytes) or `Take` (the client keeps a `BytesMut`).
   `Publisher::Options` holds the per-message settings, every field optional. `publish` returns
   `PublishOutcome<'msg, Self::Error, Self::Payload>`: a refusal hands the unsent message back
   with `PublishMessageError::with_message(error, msg)`, or a bare error converts with `.into()` or
   `?`. `DefaultPublish` on the connected form names the policy replies and retry copies use.
6. **Batches**, where the transport has them. `BatchSubscriber::Batch: BatchSlot` (a `Vec` of
   deliveries is one). `batches(size)` returns a `BatchStream` whose `poll_fill` fills the slot the
   runtime lends, and the runtime takes each batch out with `BatchSlot::hand_over`. A transport
   that delivers one message at a time assembles batches with the core's `BufferedSubscriber`.
7. **Capabilities**, only where the transport has them: `TransactionalPublisher`,
   `OwnedTransactions`, `RequestReply`, `Partitioned`, `Seekable` / `Seeker`, `Positioned`,
   `Carries` / `CarriesBatch`. Each passes its `conformance::capabilities` suite.
8. **`DescribeServer` and bindings.** The server spec is a coordinate, built with
   `ServerSpec::from_url`; channel, operation and message bindings ride the descriptor.
9. **The prelude and the mount steps.** `pub use ruststream::prelude::*;`, the crate's
   service-facing types, and the policies under the uniform names `Publish`,
   `TransactionalPublish`, `Request`. The broker's settings in its own vocabulary are extension
   traits over the mount chain: a trait on `SubscriberBuilder` for the settings that follow the
   source, and a trait bounded on `IntoDeclared<Mk>` for a step written right after the handler.
10. **The in-process mode** under a `testing` feature: `InProcess` on the production broker,
    `TestableBroker` on its connected form, `register_testable_broker!(MyBroker)`.
11. **Conformance**, in process and live: `harness::run_suite`, `harness::lifecycle`,
    `lifecycle::refused_publish_returns_message` (with `Unsent::Returned` or `Unsent::Consumed`),
    `lifecycle::shutdown_flushes`, `settlement::matches_in_process`, the retry, message-shape and
    capability suites, then the author checklist at the end of the
    [conformance overview](https://docs.rs/ruststream/latest/ruststream/conformance/index.html).
12. **Benchmarks and the zero-cost floor.** A delivery and a publish allocate nothing the client
    does not, make no `dyn` call, take no extra atomic and copy no payload per message, whatever
    the payload size. The crate's benchmarks count instructions and allocations per message
    against the raw client, built with fat LTO and without `testing`.

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

### A client whose types are not `Send` serves the embedded runner

A client that holds `Rc` or `RefCell` (a thread-per-core client, an embedded network stack) has
no `Send` futures to promise. The lifecycle traits (`Broker`, `ConnectedBroker`, `Subscribe`,
`SubscriptionSource`, `PublishPolicy`) carry no `Send`, so it implements them as they are. The
per-message traits (`Subscriber`, `Settle`, `Publisher` and the capability traits) are one trait
per concept for every runtime: their futures ask for `Send` when `tokio` or `compio` is on and for nothing on
the embedded runner alone, so the same impl serves a device and a server. `IncomingMessage` gives
the accessors of a delivery. Error types stay `Send + Sync + 'static`. A build with both runtime
features takes the stricter bound, so a type that holds `Rc` does not compile next to tokio. The
tokio and compio runners ask for `Send + Sync` where they hand a delivery to a task or a core. The suites of the local family
(`run_local_suite`, `local_lifecycle`) come with the facade's `conformance` feature, which turns
tokio on, so they check such a client in a build where its types are `Send`.

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
| `default-features = false` with no runner feature | `features = ["tokio"]` (or the embedded runner) |
| `publish` that drops the message on a refusal | `PublishMessageError::with_message(error, msg)`; `lifecycle::refused_publish_returns_message` holds it |
| `impl IncomingMessage` with an `ack` in it | `ack` and `nack` live in `impl Settle` |
| a batch type that only has a `drain` | implement `BatchSlot::with_capacity` and `hand_over` |
