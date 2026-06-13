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
cannot leak state into each other. This is the in-memory reference broker's own suite run,
verbatim; substitute your `TestClient::start` in the factory:

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
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
use ruststream_nats::{NatsBroker, SubscribeOptions};

--8<-- "tests/doc_conformance_nats.rs:lifecycle"
```

- **`make_broker`** is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
  cannot satisfy it - which is the point: construct cheaply, connect lazily in `Broker::connect`.
- **`make_source`** builds the subscription descriptor for a subject (the macro-subscriber path).
- **`make_publisher`** produces a publisher from the connected broker.

A broker with no ack semantics (Core NATS) passes by returning `AckError::Unsupported` from `ack`;
the check accepts that as well as a successful ack. Because `lifecycle` performs a real `connect`,
run it against a live server (gate it behind an env var like `NATS_TEST_URL`); the in-memory broker
can run it in-process.

## Capability suites

If your broker implements a capability trait, run the matching suite from
`conformance::capabilities` to prove the implementation honours the trait contract; brokers
without the capability simply do not call it. Each suite takes the same factory shape as
`lifecycle` and performs a real `connect`, so gate it the same way:

| Suite | Requires | Asserts |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | the request reaches a responder with a usable `reply-to` header, the correlated reply resolves the request, an unanswered request fails after its timeout |
| `capabilities::batches` | `BatchSubscriber` | every published message arrives in publish order, distributed over non-empty batches |
| `capabilities::transactions` | `TransactionalPublisher` | nothing inside a transaction is visible before `commit`, a commit publishes the buffer in order, an abort discards it |

```rust
use ruststream::conformance::capabilities;
use ruststream_nats::{NatsBroker, SubscribeOptions};

--8<-- "tests/doc_conformance_nats.rs:request_reply"
```

The in-memory broker implements every capability natively and passes all three suites in-process
(see [Memory](../brokers/memory.md#capabilities)); it is the executable reference for what each
suite expects.

## Author checklist

Before publishing a broker crate:

- [ ] `Broker`, `Subscribe` (or a `SubscriptionSource`), `Subscriber`, `IncomingMessage`, and
      `Publisher` are implemented.
- [ ] `shutdown` performs all fallible teardown and never blocks or panics.
- [ ] Ack consumes `self`; nack honours the `requeue` flag.
- [ ] The crate owns its `Config`; fields without a sane default do not get a `Default`.
- [ ] Capability traits are implemented only where the broker genuinely supports them, and each
      implemented capability passes its `conformance::capabilities` suite.
- [ ] A `TestClient` is shipped under a `testing` feature, doing core routing only.
- [ ] `harness::run_suite` passes (the routing surface).
- [ ] `harness::lifecycle` passes against a real server, gated behind an environment variable (the
      lazy-startup contract: sync `new`, lazy `connect`, subscribe, ack, `shutdown`).
- [ ] An end-to-end suite covers broker-specific semantics, also gated behind that variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
