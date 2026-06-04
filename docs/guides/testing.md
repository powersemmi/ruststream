# Testing

RustStream's test support exercises **core routing only**: it dispatches messages to your registered
handlers so you can assert on handler behaviour, middleware, decoding, and any database stubs. It
does not simulate broker-specific semantics.

!!! warning "What the test client does and does not do"
    The test client is a handler-stub dispatcher. Publishing a message fans it out to the
    subscribers whose name matches, runs your handler, and treats ack/nack as effectively a no-op.
    It does **not** model JetStream durable cursors, `ack_wait` redelivery, `max_ack_pending`,
    retention, Kafka offsets or consumer groups, or RabbitMQ exchanges and dead-letter routing.
    Those are real-broker concerns. Test them end to end against a real broker, not the stub.

## Unit-testing handlers

For local tests use `MemoryBroker` (the `memory` feature), or, for a real broker crate, the
`TestClient` it ships under its `testing` feature. The handlers, middleware, and codecs are declared
once on the production setup and shared with the test.

A test starts the in-memory broker, publishes a message as if from an external producer, and asserts
the handler observed it. The `TestClient` trait provides `start`, `publish`, `subscribe`, and
`shutdown`:

```rust
use ruststream::memory::MemoryBroker;
use ruststream::testing::TestClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_acks_orders() {
    let client = MemoryBroker::start().await.unwrap();

    // register handlers on client.broker(), drive the app, then:
    client.publish("orders", br#"{"id":1}"#).await.unwrap();

    // assert via your handler's observable effect (a channel, a counter, a stubbed DB)
    client.shutdown().await.unwrap();
}
```

Assert on what your handler does (a value pushed to a channel, a counter, a stub database call),
exactly as you would in production. That is the behaviour the stub is there to verify.

## End-to-end tests

Behaviour that depends on real broker semantics (durable resume, redelivery on timeout, partition
assignment, dead-letter routing) belongs in a separate integration suite gated behind an environment
variable, so it runs only when a real broker is available:

```rust
#[tokio::test]
#[ignore = "requires a running broker; set BROKER_TEST_URL"]
async fn redelivers_after_ack_wait() {
    // connect to the real broker via BROKER_TEST_URL
}
```

Run it explicitly with `cargo test -- --ignored` against a live server. This mirrors the
`with_real = true` path: the default test run stays fast and offline, and real semantics are checked
where they actually live.

## For broker authors

If you are implementing a broker, the conformance harness proves your implementation honours the core
contract. See [Conformance](../broker-authors/conformance.md).
