# Conformance

The conformance harness proves a broker honours the core contract. It has two entry points, both of
which panic with a descriptive message on the first failure:

- `harness::run_suite` checks the **routing surface** against your in-process transport (the
  [`TestableBroker`](../guides/testing.md#for-broker-authors) you ship).
- `harness::lifecycle` checks the **lazy-startup contract** end to end against a connected broker.

Run both: `run_suite` for the dispatch guarantees, `lifecycle` to prove `new` -> `connect` ->
subscribe -> publish -> ack -> `shutdown` works on the real transport.

```toml
[dev-dependencies]
ruststream = { version = "0.5", features = ["conformance"] }
```

The `conformance` feature pulls in `testing`, so the one `TestableBroker` your crate ships works with
both `run_suite` here and the [`TestApp`](../guides/testing.md) harness users write.

## The routing suite

`harness::run_suite` takes a synchronous factory (`Fn() -> B`) that builds a fresh in-process
transport per scenario, so scenarios cannot leak state into each other. `B` is your
`TestableBroker` (it also implements `Subscribe`). This is the in-memory reference broker's own suite
run, verbatim; substitute your transport's constructor in the factory:

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
| published log observes publishes | `published(name)` records every published message |

These are core-routing guarantees, the contract every broker must meet. The harness does **not** test
broker-specific semantics (durable resume, redelivery on timeout, partition assignment); those are
not part of the contract and are verified in your own end-to-end suite against a real server.

## The lifecycle check

`run_suite` exercises routing through the in-process transport; `harness::lifecycle` exercises the
**lazy-startup contract** through the real `Broker`: synchronous construction with no I/O, then
`connect`, a subscription opened through the broker's own `SubscriptionSource`, a publish the
subscription receives and acks, and finally `shutdown`. It takes three factories that keep it
broker-agnostic:

<!-- inline-rust: worked lifecycle check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::harness;
use ruststream_nats::{NatsBroker, SubscribeOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_lifecycle() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    harness::lifecycle(
        || NatsBroker::new(url.clone()), // sync construction (no I/O)
        |subject| SubscribeOptions::new(subject), // the broker's SubscriptionSource
        |broker| broker.publisher(),     // a publisher from the connected broker
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

<!-- inline-rust: worked request-reply capability check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::capabilities;
use ruststream_nats::{NatsBroker, SubscribeOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_request_reply() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    capabilities::request_reply(
        || NatsBroker::new(url.clone()),
        |subject| SubscribeOptions::new(subject),
        |broker| broker.publisher(), // the RequestReply publisher under test
        |broker| broker.publisher(), // the plain publisher the responder replies through
    )
    .await;
}
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
- [ ] An in-process transport implementing `TestableBroker` is shipped under a `testing` feature
      (core routing only) and registered with `register_testable_broker!`.
- [ ] `harness::run_suite` passes (the routing surface).
- [ ] `harness::lifecycle` passes against a real server, gated behind an environment variable (the
      lazy-startup contract: sync `new`, lazy `connect`, subscribe, ack, `shutdown`).
- [ ] An end-to-end suite covers broker-specific semantics, also gated behind that variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
