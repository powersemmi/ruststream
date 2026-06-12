# Conformance

The conformance harness proves a broker honours the core contract. It has two entry points, both of
which panic with a descriptive message on the first failure:

- `harness::run_suite` checks the **routing surface** against your in-process `TestClient`.
- `harness::lifecycle` checks the **lazy-startup contract** end to end against a connected broker.

Run both: `run_suite` for the dispatch guarantees, `lifecycle` to prove `new` -> `connect` ->
subscribe -> publish -> ack -> `shutdown` works on the real transport.

```toml
[dev-dependencies]
ruststream = { version = "0.3", features = ["conformance"] }
```

Enable your crate's own `testing` feature alongside it, since `run_suite` drives the `TestClient` you
ship there.

## The routing suite

`harness::run_suite` takes a factory that builds a fresh client per scenario. The factory is
fallible (it returns the `TestClient::start` result) and is invoked once per scenario, so scenarios
cannot leak state into each other:

```rust
use ruststream::conformance::harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_conformance() {
    harness::run_suite(|| async { MyTestClient::start().await }).await;
}
```

### What it checks

| Scenario | Asserts |
|---|---|
| ordering | messages are delivered in publish order |
| publish after subscribe | a subscriber receives only messages published after it attached; earlier publishes are not buffered |
| ack consumes delivery | an acked message is not redelivered |
| nack with requeue redelivers | `nack(requeue = true)` delivers the message again |
| nack without requeue drops | `nack(requeue = false)` does not redeliver |
| headers propagate | message headers survive the round trip |
| expect_published observes publishes | the test client records published messages |

These are core-routing guarantees, the contract every broker must meet. The harness does **not** test
broker-specific semantics (durable resume, redelivery on timeout, partition assignment); those are
not part of the contract and are verified in your own end-to-end suite against a real server.

## The lifecycle check

`run_suite` exercises routing through the `TestClient`; `harness::lifecycle` exercises the
**lazy-startup contract** through the real `Broker`: synchronous construction with no I/O, then
`connect`, a subscription opened through the broker's own `SubscriptionSource`, a publish the
subscription receives and acks, and finally `shutdown`. It takes three factories that keep it
broker-agnostic:

```rust
use ruststream::conformance::harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_lifecycle() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    harness::lifecycle(
        || NatsBroker::new(url.clone()),         // sync construction (no I/O)
        |subject| SubscribeOptions::new(subject), // the broker's SubscriptionSource
        |broker| broker.publisher(),              // a publisher from the connected broker
    )
    .await;
}
```

- **`make_broker`** is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
  cannot satisfy it - which is the point: construct cheaply, connect lazily in `Broker::connect`.
- **`make_source`** builds the subscription descriptor for a subject (the macro-subscriber path).
- **`make_publisher`** produces a publisher from the connected broker.

A broker with no ack semantics (Core NATS) passes by returning `AckError::Unsupported` from `ack`;
the check accepts that as well as a successful ack. Because `lifecycle` performs a real `connect`,
run it against a live server (gate it behind an env var like `NATS_TEST_URL`); the in-memory broker
can run it in-process.

## Author checklist

Before publishing a broker crate:

- [ ] `Broker`, `Subscribe` (or a `SubscriptionSource`), `Subscriber`, `IncomingMessage`, and
      `Publisher` are implemented.
- [ ] `shutdown` performs all fallible teardown and never blocks or panics.
- [ ] Ack consumes `self`; nack honours the `requeue` flag.
- [ ] The crate owns its `Config`; fields without a sane default do not get a `Default`.
- [ ] Capability traits are implemented only where the broker genuinely supports them.
- [ ] A `TestClient` is shipped under a `testing` feature, doing core routing only.
- [ ] `harness::run_suite` passes (the routing surface).
- [ ] `harness::lifecycle` passes against a real server, gated behind an environment variable (the
      lazy-startup contract: sync `new`, lazy `connect`, subscribe, ack, `shutdown`).
- [ ] An end-to-end suite covers broker-specific semantics, also gated behind that variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
