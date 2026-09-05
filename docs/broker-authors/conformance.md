# Conformance

The conformance harness proves a broker honours the core contract. It has two entry points, both of
which panic with a descriptive message on the first failure:

- `harness::run_suite` checks the **routing surface** against your in-process transport (the
  [`TestableBroker`](index.md#test-support) you ship).
- `harness::lifecycle` checks the **lifecycle ladder** end to end against the real broker.

Run both: `run_suite` for the dispatch guarantees, `lifecycle` to prove `new` -> `connect(self)`
-> subscribe -> publish -> ack -> `shutdown(self)` works on the real transport.

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

The `conformance` feature pulls in `testing`, so the one `TestableBroker` your crate ships works with
both `run_suite` here and the [`TestApp`](../guides/testing.md) harness users write.

## The routing suite

`harness::run_suite` takes a synchronous factory (`Fn() -> B`) that builds a fresh in-process
transport per scenario, so scenarios cannot leak state into each other. Each scenario connects the
broker and drives its connected form, which is your `TestableBroker` (it also implements
`Subscribe`). This is the in-memory reference broker's own suite run, verbatim; substitute your
transport's constructor in the factory:

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

Nor does it reach a single capability. Each of those has a suite of its own below, and it is your
crate that calls the ones it implements - `capabilities::batches` included, which is the only
check that a batch never comes back larger than the size it was opened with. Folding it in here is
not open: a `BatchSubscriber` bound on `run_suite` would shut out every broker that declines the
capability, and the in-process transport is not where your batches are built anyway. So a crate
that runs `run_suite` alone has not checked its batches.

## The lifecycle check

`run_suite` exercises routing through the in-process transport; `harness::lifecycle` exercises the
**lifecycle ladder** through the real `Broker`: synchronous construction with no I/O, then the
consuming `connect` producing the typed connected form, a subscription opened through the broker's
own `SubscriptionSource`, a publish the subscription receives and acks, and the consuming
`shutdown` producing the terminal witness. Owner-side misuse after shutdown does not compile under
the ladder, so the runtime rule the check keeps verifying is the **aliased-handle contract**: a
publisher created before the shutdown must error afterwards, never silently succeed against a
dead connection. It takes three factories that keep it broker-agnostic:

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
        |connected| connected.publisher(), // a publisher from the connected form
    )
    .await;
}
```

- **`make_broker`** is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
  cannot satisfy it: construct cheaply, connect in `Broker::connect`.
- **`make_source`** builds the subscription descriptor for a subject (the macro-subscriber path).
- **`make_publisher`** produces a publisher from the connected form.

A broker with no ack semantics (Core NATS) passes by returning `AckError::Unsupported` from `ack`;
the check accepts that as well as a successful ack. Because `lifecycle` performs a real `connect`,
run it against a live server (gate it behind an env var like `NATS_TEST_URL`); the in-memory broker
can run it in-process.

## Capability suites

If your broker implements a capability trait, run the matching suite from
`conformance::capabilities` to prove the implementation honours the trait contract; brokers
without the capability do not call it. Each suite takes the same factory shape as
`lifecycle` and performs a real `connect`, so gate it the same way:

| Suite | Requires | Asserts |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | the request reaches a responder with a usable `reply-to` header, the correlated reply resolves the request, an unanswered request fails after its timeout |
| `capabilities::batches` | `BatchSubscriber` | every published message arrives in publish order, distributed over non-empty batches |
| `capabilities::transactions` | `TransactionalPublisher` | nothing inside a transaction is visible before `commit`, a commit publishes the buffer in order, an abort discards it; misuse errors - `commit` / `abort` with no open transaction, a second `begin_transaction` while one is open (which must leave it untouched) |
| `capabilities::owned_transactions` | `OwnedTransactions`, its `Transaction` | nothing published into an open transaction is visible before `commit`, a commit delivers the whole buffer in publish order, an abort discards it, two transactions open at once on one handle settle independently, and the handle keeps publishing directly while one is open |
| `capabilities::seeking` | `Seekable`, messages `Positioned` | a seek back to a position captured from a delivered message redelivers exactly that message and the ordered suffix after it, a seek forward skips the queued deliveries before the target, and the subscription keeps delivering new publishes after repositioning |

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
        |connected| connected.publisher(), // the RequestReply publisher under test
        |connected| connected.publisher(), // the plain publisher the responder replies through
    )
    .await;
}
```

Every suite names its subject per run rather than fixing one, so a run reads only what it
published. A fixed subject passes against a fresh server and fails the second time on any broker
that keeps what the first run left: a retained log replays both runs into one subscription, a
durable queue still holds the earlier messages, a key namespace still holds the earlier type. The
suites call `conformance::helpers::unique_subject` for that, and your own end-to-end suite has the
same problem and the same answer.

The in-memory broker implements every capability natively and passes all five suites in-process
(see [Memory](../brokers/memory.md#capabilities)); it is the executable reference for what each
suite expects.

## Author checklist

Before publishing a broker crate:

- [ ] `Broker`, `ConnectedBroker`, `Subscribe` (or a `SubscriptionSource`), `Subscriber`,
      `IncomingMessage`, `Publisher`, and a `PublishPolicy` pairing into it are implemented.
- [ ] `shutdown` performs all fallible teardown and never blocks or panics.
- [ ] Ack consumes `self`; nack honours the `requeue` flag.
- [ ] The crate owns its `Config`; fields without a sane default do not get a `Default`.
- [ ] Capability traits are implemented only where the broker genuinely supports them, and each
      implemented capability passes its `conformance::capabilities` suite.
- [ ] An in-process transport implementing `TestableBroker` on its connected form is shipped under
      a `testing` feature (core routing only) and registered with `register_testable_broker!`.
- [ ] `harness::run_suite` passes (the routing surface).
- [ ] `harness::lifecycle` passes against a real server, gated behind an environment variable (the
      ladder: sync `new`, consuming `connect`, subscribe, ack, consuming `shutdown`, and the
      aliased-handle error after it).
- [ ] An end-to-end suite covers broker-specific semantics, also gated behind that variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
