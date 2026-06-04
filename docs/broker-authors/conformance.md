# Conformance

The conformance harness proves a broker honours the core contract. It runs a fixed set of scenarios
against your `TestClient`, starting from a fresh instance each time, and panics with a descriptive
message on the first failure.

```toml
[dev-dependencies]
ruststream = { version = "0.1", features = ["conformance"] }
```

Enable your crate's own `testing` feature alongside it, since the harness drives the `TestClient` you
ship there.

## Running the suite

`harness::run_suite` takes a factory that builds a fresh client per scenario:

```rust
use ruststream::conformance::harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_conformance() {
    harness::run_suite(|| async { MyTestClient::start().await }).await;
}
```

The factory is invoked once per scenario, so scenarios cannot leak state into each other.

## What it checks

| Scenario | Asserts |
|---|---|
| ordering | messages are delivered in publish order |
| publish after subscribe | a message published before a subscriber attaches is still delivered |
| ack consumes delivery | an acked message is not redelivered |
| nack with requeue redelivers | `nack(requeue = true)` delivers the message again |
| nack without requeue drops | `nack(requeue = false)` does not redeliver |
| headers propagate | message headers survive the round trip |
| expect_published observes publishes | the test client records published messages |

These are core-routing guarantees, the contract every broker must meet. The harness does **not** test
broker-specific semantics (durable resume, redelivery on timeout, partition assignment); those are
not part of the contract and are verified in your own end-to-end suite against a real server.

## Author checklist

Before publishing a broker crate:

- [ ] `Broker`, `Subscribe` (or a `SubscriptionSource`), `Subscriber`, `IncomingMessage`, and
      `Publisher` are implemented.
- [ ] `shutdown` performs all fallible teardown and never blocks or panics.
- [ ] Ack consumes `self`; nack honours the `requeue` flag.
- [ ] The crate owns its `Config`; fields without a sane default do not get a `Default`.
- [ ] Capability traits are implemented only where the broker genuinely supports them.
- [ ] A `TestClient` is shipped under a `testing` feature, doing core routing only.
- [ ] `harness::run_suite` passes.
- [ ] An end-to-end suite covers broker-specific semantics, gated behind an environment variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
