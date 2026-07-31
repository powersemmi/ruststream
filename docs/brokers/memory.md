# Memory

The `memory` feature provides `MemoryBroker`, an in-process broker. It needs no external service,
which makes it ideal for examples, unit tests, and prototypes; the default `cargo generate` template
(`templates/memory`) uses it so a fresh project runs with zero dependencies.

```toml
ruststream = { version = "0.6", features = ["macros", "memory", "json"] }
```

<!-- inline-rust: two-line constructor sketch; the broker in context is exercised by every memory-feature example (e.g. quickstart.rs:app) -->
```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

## Semantics

- **Exact name matching.** A subscription to `orders` receives messages published to `orders`; no
  wildcard or pattern matching (those are broker-specific; the NATS test broker has real subject
  matching).
- **Fan-out.** Every subscriber of a name receives every message published to it after the
  subscription opened; messages published earlier are not delivered by default, though the
  `Seekable` capability can replay them from the publish log.
- **Ack is a no-op; `nack(requeue: true)` redelivers** the same payload to the same subscriber.
- **Cheap to clone.** Clones share state, so a clone held by a test observes everything the app
  publishes.

It does core routing only and does not emulate any real broker's delivery semantics (durable
cursors, redelivery timers, partitions, dead-letter routing).

## Capabilities

Every capability trait has a native implementation - a first-class feature of the broker's own
in-process semantics, not a simulation of another broker's:

- **Request / reply.** `broker.requester()` returns a `MemoryRequester` whose `request` publishes
  with a unique in-process inbox in the `reply-to` header and resolves on the first message
  delivered there. A responder reads `reply-to` from the request and publishes its reply to that
  name. Requests nobody answers fail with `RequestError::Timeout`.
- **Batches.** `MemorySubscriber` implements `BatchSubscriber`: a batch is the first awaited
  delivery plus everything already buffered, capped by `set_batch_limit` (default 64). Partial
  batches ship immediately, so no deadline timer is involved.
- **Transactions.** `MemoryPublisher` implements `TransactionalPublisher`: publishes between
  `begin_transaction` and `commit` are buffered and fan out together in publish order; `abort`
  discards them. Misuse errors with `MemoryError` per the trait contract: a second
  `begin_transaction` returns `TransactionBusy` (the open transaction is untouched), and
  `commit` / `abort` without one return `NoTransaction`. Clones of a publisher handle do not
  share its transaction.
- **Partition keys.** `MemoryMessage` implements `Partitioned`, reading the key from the
  well-known `partition-key` header (`memory::PARTITION_KEY_HEADER`).
- **Seeking.** `MemorySubscriber` implements `Seekable` over the broker's per-name publish log:
  mint a `MemorySeeker` before opening the stream, then `seek` to a `MemoryPosition` - captured
  from a delivered message (`Positioned::position`, which redelivers exactly that message) or
  constructed (`MemoryPosition::start()` / `sequence(n)`). Seeking forward skips the queued
  deliveries before the target; seeking at or past the end of the log skips everything published
  so far. The scope is one subscriber instance, and a seek through a handle aliasing a shut-down
  bus errors with `MemoryError::ShutDown`. Inside an application, reach the seeker through a
  `WithSeeker` token or a `Seek` handler parameter (see
  [Seeking](../guides/subscribers.md#seeking)).
- **Shutdown.** The ladder is fully typed: `MemoryBroker::connect(self)` yields
  `ConnectedMemoryBroker`, and its consuming `shutdown` yields `ClosedMemoryBroker`, a witness
  reporting how many subscriber registrations the teardown dropped. The bus itself is a single
  enum (so the lifecycle and the registrations cannot disagree): aliased handles used after the
  shutdown - publishers, transaction commits, requests - error with `MemoryError::ShutDown` /
  `RequestError::ShutDown` instead of silently succeeding.

`DescribeServer` stays deliberately unimplemented: the in-memory broker has no network
coordinates, and that asymmetry is part of the contract documentation.

## Subscription source

`ConnectedMemoryBroker` implements `Subscribe`, so `#[subscriber("orders")]` works directly. The
descriptor type is `MemorySource` - it carries no extra options (the in-memory broker has none) but keeps the
descriptor form uniform across brokers. From the
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service)
example:

```rust
use ruststream::memory::MemorySource;

--8<-- "examples/routed_service/orders.rs:descriptor"
```

## For testing

`ConnectedMemoryBroker` implements `TestableBroker` and is registered with
`register_testable_broker!` (the harness connects every broker before recovering its in-process
transport), so the [`TestApp`](../guides/testing.md) harness drives it directly: build an app on a
`MemoryBroker`, hand it to `TestApp::start`, publish, and assert on what the handlers received and
published. See
[Testing](../guides/testing.md#unit-testing-a-service-with-testapp) for the full pattern.
