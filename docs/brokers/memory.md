# Memory

`MemoryBroker`, behind the `memory` feature, is a complete broker that runs entirely inside your
process: the one to reach for when a queue belongs to a single application rather than to a
network, with no external service involved. The default `cargo generate` template
(`templates/memory`) uses it, and a fresh project runs with zero dependencies.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
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

It is a real broker rather than a test double: the runtime drives it through the same dispatch path
it drives a production broker through, so a handler, its middleware and its decoding behave here
exactly as they will in production. What it does not do is emulate any particular broker's delivery
semantics - durable cursors, redelivery timers, partitions, dead-letter routing - so a test passing
here does not say the same code passes against Kafka.

## Capabilities

Every capability trait has a native implementation over the broker's own in-process semantics, not
a simulation of another broker's:

- **Request / reply.** `broker.requester()` returns a `MemoryRequester` whose `request` publishes
  with a unique in-process inbox in the `reply-to` header and resolves on the first message
  delivered there; the `MemoryRequest` policy pairs into it, so a slot bound with
  `Out<impl RequestReply, ..>` binds to `MemoryRequest`. A responder reads `reply-to` from the
  request and publishes its reply to that name. Requests nobody answers fail with
  `RequestError::Timeout`.
- **Batches.** `MemorySubscriber` implements `BatchSubscriber`: a batch is the first awaited
  delivery plus everything already buffered, capped by `set_batch_limit` (default 64). Partial
  batches ship immediately, so no deadline timer is involved.
- **Transactions.** `MemoryPublisher`, what the `MemoryPublish` policy pairs into, carries both
  transaction kinds, so a slot or wiring bound with `TransactionalPublisher` or
  `OwnedTransactions` binds to `MemoryPublish`. Publishes inside a scope are buffered and
  fan out together in publish order on commit; an abort discards them; every owned transaction
  buffers on its own. Misuse on the raw handle errors with `MemoryError` per the broker contract:
  a second begin while one is open returns `TransactionBusy` (the open transaction is untouched),
  and a commit or abort without one returns `NoTransaction`. Clones of a publisher handle do not
  share its transaction.
- **Partition keys.** `MemoryMessage` implements `Partitioned`, reading the key from the
  well-known `partition-key` header (`memory::PARTITION_KEY_HEADER`).
- **Seeking.** `MemorySubscriber` implements `Seekable` over the broker's per-name publish log:
  mint a `MemorySeeker` before opening the stream, then `seek` to a `MemoryPosition` - captured
  from a delivered message (`Positioned::position`, which redelivers exactly that message) or
  constructed (`MemoryPosition::start()` / `sequence(n)`). Seeking forward skips the queued
  deliveries before the target; seeking at or past the end of the log skips everything published
  so far. The scope is one subscriber instance, and a seek through a handle aliasing a shut-down
  bus errors with `MemoryError::ShutDown`. Inside an application, the delivery context
  (`MemoryContext`) carries the position and the seeker, read by the `Position` / `SeekHandle`
  keys (see [Seeking](../guides/subscribers.md#seeking)). A page body names `MemoryBatchContext`
  instead: it carries the subscription's seeker under that same `SeekHandle` key and no position,
  because a page spans many deliveries.
- **Shutdown.** The ladder is fully typed: `MemoryBroker::connect(self)` yields
  `ConnectedMemoryBroker`, and its consuming `shutdown` yields `ClosedMemoryBroker`, a witness
  reporting how many subscriber registrations the teardown dropped. Aliased handles used after the
  shutdown - publishers, transaction commits, requests - error with `MemoryError::ShutDown` /
  `RequestError::ShutDown` instead of silently succeeding.

`DescribeServer` is not implemented: the in-memory broker has no network coordinates to report.

## Subscription source

`ConnectedMemoryBroker` implements `Subscribe`, so `#[subscriber("orders")]` works directly. The
descriptor type is `MemorySource` - it carries no extra options (the in-memory broker has none) but keeps the
descriptor form uniform across brokers. From the
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service)
example:

=== "Macros"

    ```rust
    use ruststream::memory::MemorySource;

    --8<-- "examples/routed_service/orders.rs:descriptor"
    ```

=== "Manual"

    ```rust
    use ruststream::memory::{MemoryPublish, MemorySource};

    --8<-- "examples/manual/routed_service_orders.rs:descriptor"
    ```

## For testing

`ConnectedMemoryBroker` implements `TestableBroker` and is registered with
`register_testable_broker!` (the harness connects every broker before recovering its in-process
transport), so the [`TestApp`](../guides/testing.md) harness drives it directly: build an app on a
`MemoryBroker`, hand it to `TestApp::start`, publish, and assert on what the handlers received and
published. See
[Testing](../guides/testing.md#unit-testing-a-service-with-testapp) for the full pattern.
