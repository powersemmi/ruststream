# Testing

RustStream services are tested at two levels:

1. **In-process unit tests** drive your real handlers, middleware, and codecs with the
   [`TestApp`](#unit-testing-a-service-with-testapp) harness - no server, no docker, no network, no
   `connect`. This is the default path and it covers handler logic end to end: decode, dispatch, the
   outcome (ack / nack / drop / panic / decode failure), and any messages the handler publishes.
2. **Integration tests** run against a real broker, gated behind an environment variable, and cover
   the semantics only a real server has (durable consumers, redelivery timers, partitions).

!!! warning "What the harness does and does not model"
    The harness drives a broker's **in-process transport**: publishing fans a message out to the
    subscribers whose subject matches, runs your handler through the real dispatch path, and records
    the outcome and any downstream publishes. It does **not** model JetStream durable cursors,
    `ack_wait` redelivery, `max_ack_pending`, retention, Kafka offsets or consumer groups, or
    RabbitMQ exchanges and dead-letter routing. Those are real-broker concerns; test them in the
    [integration suite](#integration-tests-against-a-real-broker).

    `MemoryBroker` is a real broker (local in-process queues), not a test double - the harness drives
    it through the same dispatch path the production runtime uses.

## Unit-testing a service with `TestApp`

`TestApp` takes a built `RustStream` application, mounts its handlers on the broker's in-process bus
with no `connect`, and records every delivery. You publish input, and the publish drives the whole
reaction (the handler, its downstream publishes, any cross-broker cascade) to a standstill before it
returns - then you assert.

The handler under test (in a real service it lives in your handler module and the test imports it):

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

Enable the `testing` feature in your dev-dependencies:

```toml
[dev-dependencies]
ruststream = { version = "0.5", features = ["testing", "memory", "macros", "json"] }
```

### Addressing brokers

`tb.broker::<MemoryBroker>()` addresses the broker by type; `tb.broker_named("ingress")` addresses
it by the label from [`with_broker_labeled`](asyncapi.md) when a service mounts several brokers and
their subjects collide. The unscoped `tb.publish(name, &value)` is a convenience for single-broker
apps and returns `TestError::Ambiguous` when more than one broker is registered.

### Asserting on a handler

`tb.broker::<B>().subscriber(name)` returns a fluent builder over what that handler received:

| Method | Asserts |
|---|---|
| `assert_called_once()` / `assert_called(n)` / `assert_not_called()` | the delivery count |
| `with(&value)` | the most recent delivery decodes to `value` (with the default codec) |
| `with_raw(bytes)` | the most recent raw payload |
| `settled(HandlerResult::Ack)` | how it settled |
| `assert_outcome(Outcome::Drop)` | the classified outcome (ack / nack / drop / decode-failure / panic) |
| `panicked()` | the handler panicked on the last delivery |
| `assert_last_failed_to_decode()` | the payload failed to decode |

`tb.broker::<B>().published::<T>(name)` asserts on what the handler published downstream, read from
the broker's publish log: `.assert_called_once().with(&Receipt { id: 1 })`.

Beyond the assertions, the messages themselves are retrievable for custom checks:
`subscriber(name).received::<T>()` / `.received_raw()` returns what the handler received, and
`published::<T>(name).decoded()` / `.messages()` returns every message published to the channel - both
in order.

The decoding helpers (`with`, `received`, `decoded`) use the default codec. If a handler or publisher
was mounted with a different codec (`include_with` / `with_broker_codec`), pass it explicitly with the
`_with` / `with_codec` variants - `subscriber(name).with_codec(&CborCodec, &expected)`,
`.received_with(&CborCodec)`, `published::<T>(name).with_codec(&CborCodec, &expected)`,
`.decoded_with(&CborCodec)` - while `with_raw` / `received_raw` / `messages` stay codec-free.

### Failure policy, panic, and shutdown

The harness runs dispatch under the application's real `FailurePolicy`, so a negative test is a
first-class path. Under the default `panic = fail_fast`, a handler panic tears the service down just
as in production:

```rust
--8<-- "tests/testing_harness.rs:panic"
```

Under `on_failure(panic = skip)` the panic is acked and consumption continues, so `tb.assert_running()`
holds. `run_result()` returns what the real [`run`](lifespan.md) would: `Ok` while healthy, an error
once a fail-fast failure shut the service down.

!!! note "Panic catching needs unwinding"
    The harness rides the runtime's `catch_unwind`, so a deliberate panic does not kill the test
    thread. A build compiled with `panic = "abort"` cannot catch handler panics.

### Delayed redelivery (`retry_after`)

A handler that returns `retry_after(delay)` schedules a delayed redelivery. `publish` records the
immediate `NackAfter` settlement and returns; the redelivery is driven separately by advancing a
paused clock:

```rust
--8<-- "tests/testing_harness.rs:retry_after"
```

## Integration tests against a real broker

Behaviour that depends on real broker semantics belongs in a separate suite gated behind an
environment variable, so the default `cargo test` stays fast and offline:

<!-- inline-rust: integration-test skeleton with a pseudocode body; it drives a real NatsBroker (external crate) behind an env gate, so it has no compiled home here -->
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
semantics on the real one. Keep both suites over the same handler modules so the production code has
a single source of truth.

## For broker authors

A broker crate ships an in-process transport - a normal `Broker` that routes in memory, emulating
the broker's Core routing (subjects, wildcards, groups) - and implements the one
[`TestableBroker`](../broker-authors/conformance.md) contract on it:

The reference is `MemoryBroker`'s own implementation:

```rust
--8<-- "src/memory/mod.rs:testable"
```

The transport calls `Coordinator::enqueued` on every enqueue into a subscriber and
`Coordinator::consumed` when a delivery is settled or dropped (so the harness can tell when the
reaction has settled), and routes delayed redeliveries through `Coordinator::schedule_redelivery`.
That one type then works with both `TestApp` (above) and the conformance suite. Prove its routing
with `conformance::harness::run_suite`:

```rust
--8<-- "tests/conformance_self.rs:run_suite"
```

See [Conformance](../broker-authors/conformance.md) for the full contract and the lazy-startup
`lifecycle` check.
