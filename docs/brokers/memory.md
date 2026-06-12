# Memory

The `memory` feature provides `MemoryBroker`, an in-process broker. It needs no external service,
which makes it ideal for examples, unit tests, and prototypes; the CLI scaffold uses it by default
so a fresh project runs with zero dependencies.

```toml
ruststream = { version = "0.3", features = ["macros", "memory", "json"] }
```

```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

## Semantics

- **Exact name matching.** A subscription to `orders` receives messages published to `orders`; no
  wildcard or pattern matching (those are broker-specific; the NATS test broker has real subject
  matching).
- **Fan-out.** Every subscriber of a name receives every message published to it after the
  subscription opened; messages published earlier are not buffered.
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
  discards them. Clones of a publisher handle do not share its transaction.
- **Partition keys.** `MemoryMessage` implements `Partitioned`, reading the key from the
  well-known `partition-key` header (`memory::PARTITION_KEY_HEADER`).

`DescribeServer` stays deliberately unimplemented: the in-memory broker has no network
coordinates, and that asymmetry is part of the contract documentation.

## Subscription source

`MemoryBroker` implements `Subscribe`, so `#[subscriber("orders")]` works directly. Its descriptor
type is `MemorySource` - it carries no extra options (the in-memory broker has none) but keeps the
descriptor form uniform across brokers. From the
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service)
example:

```rust
use ruststream::memory::MemorySource;

--8<-- "examples/routed_service/orders.rs:descriptor"
```

## As a test client

`MemoryBroker` implements the `TestClient` trait itself: `MemoryBroker::start()` gives a client
whose `publish` and `expect_published` drive and observe the broker from the outside. See
[Testing](../guides/testing.md#testing-handlers-with-memorybroker) for the full pattern.
