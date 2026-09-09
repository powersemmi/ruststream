# Conformance

The conformance harness proves your broker honours the core contract. It has two entry points, and
both panic with a descriptive message on the first contract violation:

- `harness::run_suite` checks the **routing surface** against your in-process transport (the
  [`TestableBroker`](index.md#test-support) you ship).
- `harness::lifecycle` checks the **lifecycle ladder** end to end against the real broker.

Run both: `run_suite` for the dispatch guarantees, `lifecycle` to prove `new` -> `connect(self)`
-> subscribe -> publish -> ack -> `shutdown(self)` works on the real transport.

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

The `conformance` feature enables `testing`, so the single `TestableBroker` your crate ships serves
`run_suite` here and the [`TestApp`](../guides/testing.md) harness users write.

## The routing suite

`harness::run_suite` takes a synchronous factory (`Fn() -> B`) that builds a fresh in-process
transport for each scenario, so no scenario sees another's state. Each scenario connects the broker
and works with its connected form, your `TestableBroker`, which also implements `Subscribe`. Below
is the reference in-memory broker's own run of the suite, verbatim; put your transport's constructor
in the factory:

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
```

### What it checks

| Scenario | Asserts |
|---|---|
| ordering | messages are delivered in publish order |
| publish after subscribe | a subscriber receives only messages published after it subscribed; earlier publishes are not buffered |
| ack consumes delivery | an acked message is not redelivered |
| nack with requeue redelivers | `nack(requeue = true)` delivers the message again |
| nack without requeue drops | after `nack(requeue = false)` there is no redelivery |
| headers propagate | message headers reach the subscriber unchanged |
| published log observes publishes | `published(name)` records every published message |

A transport that cannot acknowledge (ZeroMQ, MQTT `QoS 0`, Redis pub/sub, Core NATS) returns
`AckError::Unsupported` from `ack` and `nack`, and the suite accepts that answer everywhere it
settles a delivery. Make your in-process transport answer exactly as the real one answers, instead
of claiming a settlement production never performs. The redelivery scenario is the one exception:
`nack(requeue = true)` returning `Unsupported` ends that scenario, because a transport that takes
nothing back has no redelivery to observe. The suite still asserts everything else, the drop
scenario included: a delivery nobody can settle must still not come back.

The suite reads the answer from the delivery, not from the broker, so the answer differs per
subscription and per message the way your transport differs: a requeue that is advisory under one
commit mode and rewinding under another, an acknowledgement available at one quality of service and
not at another. What stays fixed is the meaning of a success: `Ok(())` from `nack(requeue = true)`
promises the message comes back, and the runtime's retry path reads that answer the same way.

These are the core routing guarantees, the contract every broker must meet. Broker-specific
semantics (durable resume, redelivery on timeout, partition assignment) stay outside that contract:
your own end-to-end suite verifies them against a real server.

Each capability has a suite of its own, described below. Your crate calls the suites for the
capabilities it implements. Among them, `capabilities::batches` is the only check that a batch never
comes back larger than the size it was opened with. A crate that runs `run_suite` alone has not
checked its batches.

## The lifecycle check

`harness::lifecycle` runs the **lifecycle ladder** on the real `Broker`: synchronous construction
with no I/O, then the consuming `connect` that yields the typed connected form, a subscription
opened through the broker's own `SubscriptionSource`, a publish the subscription receives and acks,
and the consuming `shutdown` that yields the terminal witness.

The owner of the connected form cannot reach it after shutdown: that code does not compile. The
runtime rule is the **aliased-handle contract**, and the check watches exactly that: a publisher
created before the shutdown must return an error afterwards, never silently succeed against a
connection that is already closed.

The check takes three factories, and they keep it broker-agnostic:

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
  does not satisfy it: construct cheaply, connect in `Broker::connect`.
- **`make_source`** builds the subscription descriptor for a subject (the macro-subscriber path).
- **`make_publisher`** produces a publisher from the connected form.

A broker with no ack semantics (Core NATS) passes by returning `AckError::Unsupported` from `ack`:
the check accepts that answer as well as a successful ack. `lifecycle` performs a real `connect`, so
run it against a live server and enable it only when an environment variable like `NATS_TEST_URL` is
set; the in-memory broker passes it in process.

## Capability suites

If your broker implements a capability trait, run the matching suite from
`conformance::capabilities`: it proves the implementation honours the trait contract. A broker
without that capability does not call it. Each suite takes factories of the same shape as
`lifecycle` and performs a real `connect`, so enable it by the same environment variable:

| Suite | Requires | Asserts |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | the request reaches a responder with a usable `reply-to` header, the correlated reply resolves the request, a request with no answer returns an error after its timeout |
| `capabilities::batches` | `BatchSubscriber` | every published message arrives in publish order, distributed over non-empty batches |
| `capabilities::transactions` | `TransactionalPublisher` | nothing inside a transaction is visible before `commit`, a commit publishes the buffer in order, an abort discards it; misuse returns an error - `commit` / `abort` with no open transaction, a second `begin_transaction` while one is open (which must leave it untouched) |
| `capabilities::owned_transactions` | `OwnedTransactions`, its `Transaction` | nothing published into an open transaction is visible before `commit`, a commit delivers the whole buffer in publish order, an abort discards it, two transactions open at once on one publisher settle independently, and that publisher keeps publishing directly while one is open |
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

Every suite names its subject anew on each run, so a run reads only what it published itself. A
fixed subject passes against a fresh server and does not pass on the second run against any broker
that keeps what the first run left: a retained log replays both runs into one subscription, a
durable queue still holds the earlier messages, a key namespace still holds the earlier type. The
suites call `conformance::helpers::unique_subject` for that, and your own end-to-end suite has the
same problem and the same answer.

The in-memory broker implements every capability natively and passes all five suites in process
(see [Memory](../brokers/memory.md#capabilities)); it is the executable reference for what each
suite expects.

## Author checklist

Before publishing a broker crate:

- [ ] `Broker`, `ConnectedBroker`, `Subscribe` (or a `SubscriptionSource`), `Subscriber`,
      `IncomingMessage`, `Publisher`, and the `PublishPolicy` that constructs it are implemented.
- [ ] `shutdown` performs every teardown step that can return an error, and never blocks or panics.
- [ ] Ack consumes `self`; nack honours the `requeue` flag.
- [ ] The crate owns its `Config`; fields with no sane default do not get a `Default`.
- [ ] Capability traits are implemented only where the broker genuinely supports them, and each
      implemented capability passes its `conformance::capabilities` suite.
- [ ] An in-process transport implementing `TestableBroker` on its connected form is shipped under
      a `testing` feature (core routing only) and registered with `register_testable_broker!`.
- [ ] `harness::run_suite` passes (the routing surface).
- [ ] `harness::lifecycle` passes against a real server, enabled by an environment variable (the
      ladder: sync `new`, consuming `connect`, subscribe, ack, consuming `shutdown`, and the
      aliased-handle error after it).
- [ ] An end-to-end suite covers broker-specific semantics, enabled by that same variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
