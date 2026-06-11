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
