# Testing

RustStream services are tested at two levels:

1. **In-process tests** drive your real handlers, middleware, and codecs through an in-memory
   broker - no server, no docker, no network. This is the default test path and it covers handler
   logic end to end: decode, dispatch, ack, and any replies your handlers publish.
2. **Integration tests** run against a real broker, gated behind an environment variable, and cover
   the semantics only a real server has (durable consumers, redelivery timers, partitions).

!!! warning "What the test client does and does not do"
    An in-memory test broker is a handler-stub dispatcher: publishing fans the message out to the
    subscribers whose subject matches, runs your handler, and treats ack/nack as broker-side no-ops
    (a nack with requeue redelivers the same payload to the same subscriber). It does **not** model
    JetStream durable cursors, `ack_wait` redelivery, `max_ack_pending`, retention, Kafka offsets or
    consumer groups, or RabbitMQ exchanges and dead-letter routing. Those are real-broker concerns;
    test them in the [integration suite](#integration-tests-against-a-real-broker).

## The `TestClient` contract

Every in-memory test transport implements the `TestClient` trait from `ruststream::testing`:

| Method | Purpose |
|---|---|
| `start()` | starts a fresh, isolated in-memory broker |
| `broker()` | the broker handle, for registering with a `RustStream` app |
| `publish(name, payload)` | publishes as if from an external producer |
| `subscribe(name)` | opens a raw subscription, for asserting on deliveries directly |
| `publisher()` | a bound publisher, for publishing with headers or in a loop |
| `expect_published(name, count, timeout)` | waits until `count` messages were published to `name` |
| `shutdown()` | tears the in-memory broker down |

`expect_published` resolves as soon as `count` messages have been observed on `name`; when the
timeout elapses first it returns the messages observed so far, so assert on the returned
messages, not just on `Ok`. It records **all** publishes - the ones your test sends and the ones
your handlers send - which is what makes reply assertions one-liners.

Two implementations ship today:

- `MemoryBroker` (the `memory` feature of `ruststream`) implements `TestClient` itself - exact name
  matching, broker-agnostic.
- `ruststream-nats` ships `NatsTestClient` / `NatsTestBroker` under its `testing` feature - real
  NATS subject matching (`*` and `>` wildcards), request-reply, header propagation.

## Testing handlers with `MemoryBroker`

The handlers, middleware, and codecs under test are the production ones; only the broker is
swapped. The test starts the in-memory broker, runs the app on it, publishes a message as an
external producer would, and asserts on what the handler published back.

The handler under test (in a real service it lives in your handler module and the test imports
it):

```rust
--8<-- "tests/doc_testing_memory.rs:handler"
```

The test:

```rust
--8<-- "tests/doc_testing_memory.rs:test"
```

!!! info "This test runs in this repository's CI"
    The code above is embedded from
    [`tests/doc_testing_memory.rs`](https://github.com/powersemmi/ruststream/blob/main/tests/doc_testing_memory.rs),
    which `cargo test --all-features` runs on every change - the example cannot silently rot.

Three details carry the pattern:

- **Readiness.** The in-memory broker does not buffer: a message published before the subscription
  opens is lost. `after_startup` runs once every subscription is open, so a `Notify` signalled
  there is the cheapest reliable "handlers are live" gate (no sleeps, no polling).
- **Shutdown.** `run_until` resolves when the supplied future does, so the test owns the service's
  lifetime; `service.await` then surfaces any startup or shutdown error.
- **Assertions.** For a handler that publishes, assert through `expect_published`. For a handler
  with side effects only, assert on its observable effect - a value pushed to a channel, a counter,
  a stubbed repository inserted via [shared state](lifespan.md#shared-state).

## Testing handlers against in-memory NATS

For a NATS service, test against the NATS-flavoured test broker instead, so subject semantics are
the real ones: `orders.*` matches one token, `orders.>` matches the tail, queue-group names are
accepted, and request-reply works. Enable the broker crate's `testing` feature in
dev-dependencies:

```toml title="Cargo.toml"
[dev-dependencies]
ruststream-nats = { version = "0.3", features = ["testing"] }
```

The same pattern as above, with a wildcard subscriber that audits every order event (this file
also runs in this repository's CI):

```rust
--8<-- "tests/doc_testing_nats.rs:test"
```

The test broker also implements `RequestReply`, so a handler that responds to requests is testable
in-process: publish with `publisher().request(..)` and assert on the reply.

### Handlers declared with a JetStream descriptor

A descriptor like `SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("workers")` names
a real JetStream consumer; it implements `SubscriptionSource` for the real `NatsBroker` only,
because durable names and ack waits have no in-process meaning. To unit-test that handler's logic,
mount the same definition on the test broker with an explicit by-name source - `include_on`
overrides the macro's source (the codec resolves the same way as for `include`):

<!-- inline-rust: include_on override fragment; the handler under test is declared with a real NatsBroker JetStream descriptor, which has no in-process compiled home, so the by-name remount is shown inline -->
```rust
use ruststream::Name;

.with_broker(client.broker().clone(), |b| {
    b.include_on(Name::new("orders.created"), handle);
})
```

What that mount does **not** cover - durable resume, `ack_wait` redelivery, `max_ack_pending` - is
exactly what the integration suite is for.

## Integration tests against a real broker

Behaviour that depends on real broker semantics belongs in a separate suite gated behind an
environment variable, so the default `cargo test` stays fast and offline:

<!-- inline-rust: integration-test skeleton with a pseudocode body; it drives a real NatsBroker (external crate) behind an env gate, so it has no compiled home here (the real gated suite is doc_conformance_nats.rs) -->
```rust title="tests/integration_nats.rs"
fn test_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_consumer_resumes_after_restart() {
    let Some(url) = test_url() else {
        eprintln!("skipping: set NATS_TEST_URL to run");
        return;
    };
    // connect NatsBroker::new(url), drive the real JetStream consumer ...
}
```

Run it explicitly against a live server:

```bash
docker run -d -p 4222:4222 nats:latest -js
NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --test integration_nats
```

This mirrors faststream's `with_real=True` split: handler logic on the in-memory path, broker
semantics on the real one. Keep both suites over the same handler modules so the production code
has a single source of truth.

## For broker authors

If you are implementing a broker, ship a `TestClient` under a `testing` feature and prove it with
the conformance harness: `run_suite` checks the routing surface of the test client, `lifecycle`
checks the lazy-startup contract against the real transport. See
[Conformance](../broker-authors/conformance.md).
