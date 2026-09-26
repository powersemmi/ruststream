# Conformance

The conformance harness proves your broker honours the core contract. It has two entry points, and
both panic with a descriptive message on the first contract violation:

- `harness::run_suite` checks the **routing surface** of your broker's
  [in-process mode](index.md#test-support): it builds your production broker and connects it
  through `InProcess::connect_in_process`.
- `harness::lifecycle` checks the **lifecycle ladder** end to end against the real broker.

Run both: `run_suite` for the dispatch guarantees, `lifecycle` to prove `new` -> `connect(self)`
-> subscribe -> publish -> ack -> `shutdown(self)` works on the real transport.

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

The `conformance` feature enables `testing`, so the one in-process mode your crate ships serves
`run_suite` here and the [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html)
harness users write.

## The routing suite

`harness::run_suite` takes a synchronous factory (`Fn() -> B`) that builds a fresh production broker
for each scenario, so no scenario sees another's state. Each scenario connects it through
`connect_in_process` and works with the connected form, which implements `TestableBroker` and
`Subscribe`. Below is the reference in-memory broker's own run of the suite, verbatim; put your
broker's constructor in the factory, configured the way a service configures it:

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
```

### What it checks

| Scenario | Asserts |
|---|---|
| ordering | messages are delivered in publish order |
| publish before subscribe | a message published before the subscription opened arrives first where the broker declares `Backlog::Delivered`, and never where it declares `Backlog::Missed` (the default); the message published after it arrives either way |
| ack consumes delivery | an acked message is not redelivered |
| nack with requeue redelivers | `nack(requeue = true)` delivers the message again |
| nack without requeue drops | after `nack(requeue = false)` there is no redelivery |
| headers propagate | message headers reach the subscriber unchanged |
| published log observes publishes | `published(name)` records every published message |
| the publish log records your own publisher | what your default publisher sends is in `published(name)` next to what the test injected, in order and with its headers (for a broker registered with `register_testable_broker!`) |
| two subscriptions of one name | each name receives every message as often as `TestableBroker::routes` answers: every message each where the broker fans out, each message once between them where they compete, shared between them unless the answer names the one it goes to |
| the harness counts balance | on a paused clock, as `TestApp` drives it: a delivery is counted in flight before the publish returns and released once when settled, a requeue counts it again, a publish no subscription receives is not counted, a delayed redelivery is counted when its timer falls due |

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

Every step comes from where a service reaches the broker:

- The constructor runs on a plain thread with no Tokio runtime and must return within a second. A
  constructor that spawns, calls `Handle::current` or waits on the network fails.
- The subscription is opened, the first message published and every settlement made from a
  current-thread runtime on a thread of its own, which stops before the check goes on. The
  subscription must keep receiving. The publisher of the first message publishes again, from the
  broker's runtime and from a new runtime, and both messages must arrive. A `nack(requeue = true)`
  that answers `Ok` must bring the message back, and `nack(requeue = false)` must not. This holds
  the broker to running its internal tasks on the runtime it connected on (see
  [Writing a broker](index.md)).
- Where the delivery offers `nack_after`, the check settles with a delay of 1.5 s. The message must
  come back no sooner than that and within ten seconds after it: a delay that is ignored, rounded
  down or run on the settling thread's runtime fails.
- `shutdown` must return within ten seconds.

Before the ladder, on a connection of its own, the check holds your publisher to what a message
carries. Thirty-two messages published one after another must arrive in that order on one
subscription. Four more carry headers: many entries, an empty value, a value that is not UTF-8, and
the framework's own retry count and trace context. Each header must come back byte for byte, or the
publish carrying it must fail. A message whose publish failed must never arrive, and a header
dropped or rewritten on the way fails the check.

The owner of the connected form cannot reach it after shutdown: that code does not compile. The
runtime rule is the **aliased-handle contract**. After the shutdown, a publish must return an error
through a publisher paired before it and never used, through one used on the broker's runtime and
through one used from the stopped runtimes. A delivery received before the shutdown and requeued
after it must return an error or come back.

The descriptor, the subscriber, the publisher and the delivery move to other threads, so all of
them are `'static`.

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

- **`make_broker`** is **synchronous** (`Fn() -> B`) and `Sync`, because the check calls it from
  another thread. A broker that can only be built asynchronously does not satisfy it: construct
  cheaply, connect in `Broker::connect`.
- **`make_source`** builds the subscription descriptor for a subject (the macro-subscriber path).
- **`make_publisher`** produces a publisher from the connected form.

A broker with no ack semantics (Core NATS) passes by returning `AckError::Unsupported` from `ack`:
the check accepts that answer as well as a successful ack. `lifecycle` performs a real `connect`, so
run it against a live server and enable it only when an environment variable like `NATS_TEST_URL` is
set.

## Shutdown and shared handles

Two more lifecycle checks need an answer only your broker can give, so your crate calls each of
them itself, in process and against a real server:

| Check | Takes | Asserts |
|---|---|---|
| `lifecycle::shutdown_flushes` | the broker's `Backlog` answer | an acknowledgement and a publish made right before `shutdown` are finished by it: the acknowledged message does not come back on a new connection, and the published one reaches another connection (a new one under `Backlog::Delivered`, one subscribed beforehand under `Backlog::Missed`); an acknowledgement made after the shutdown that answers `Ok` must hold too; `shutdown` returns within ten seconds |
| `lifecycle::shared_handle_closes` | a connected form that is `Clone` | once the original shuts down, every use of a clone errors: a publisher paired from it before or after the shutdown, and a subscription it opens (refused, or ended at once) |

`shutdown_flushes` connects more than once, so `make_broker` must reach the same broker every time:
a server, or in process one world shared by every broker it builds. The in-memory broker's own run
shares one bus between clones:

```rust
use ruststream::conformance::lifecycle;
use ruststream::testing::Backlog;

--8<-- "tests/conformance_self.rs:shutdown_flushes"
```

## The same suites in process

`lifecycle`, `redelivery_address` and the capability suites connect the broker they are handed with
`Broker::connect`. Wrap your production broker in `harness::InProcessBroker` and they connect it
through `connect_in_process` instead, so every suite runs a second time with no server, over your
own descriptors and publish policies. Run both passes: the in-process one is what keeps the
in-process mode honest, the live one proves the real transport. Below, a broker whose `connect`
dials a server passes the routing suite and the ladder in process:

```rust
--8<-- "tests/in_process.rs:suites"
```

## The settlement suite

`settlement::suite` holds each settlement to its meaning on the transport the broker connects to,
the live server included:

| Check | Asserts |
|---|---|
| ack consumes | an acked message does not come back, neither within the redelivery timeout nor on a new connection to the same subscription |
| nack without requeue drops | the same after `nack(requeue = false)` |
| nack with requeue returns | `Ok(())` from `nack(requeue = true)` brings the message back |
| out-of-order settlement | with three messages in flight, acking the third and dropping the first two unsettled brings the first two back: on the subscription, on it opened again, or on a new connection to it |
| unsettled drop | a message dropped without a settlement, from a runtime that stops right after, comes back |

A settlement that answers `AckError::Unsupported` is checked on its own subscription only: the
transport never learned of it, so what a new connection reads depends on where it starts. The last
two checks end where `nack(requeue = true)` answers `AckError::Unsupported`: a transport
that takes nothing back has no redelivery to observe. A log that commits a contiguous prefix may
deliver the acked third message again with the first two; a duplicate passes, a loss does not.

```rust
--8<-- "src/conformance/settlement/tests.rs:matches_in_process"
```

- **`make_broker`** is called once per connection, and a check connects twice, so every broker it
  returns reaches the same server: the same address live, clones of one broker in process.
- **`make_source`** opens the same subscription on both connections: a durable consumer, a queue
  or a consumer group where the broker has one. It lets three deliveries be in flight at once.
- **The redelivery timeout** is how long the broker takes to hand back a delivery nobody settled:
  the ack wait, visibility timeout or ack deadline the descriptor configures. A broker that hands
  such a delivery back only when the connection closes passes `Duration::ZERO`.

`settlement::suite` returns what each settlement answered. `settlement::matches_in_process` runs
it against the server and through `harness::InProcessBroker`, and fails when the two answer
differently: an in-process transport that claims a settlement the server refuses passes a
handler's retry in a test and loses the message in production.

## Retry checks

A descriptor's `type Copies` says how a registration retries, and `conformance::retry` holds each
answer that promises the service something to that promise.

`harness::redelivery_address` takes an `AddressedCopies` descriptor. It opens two subscriptions from
it, the way two replicas read one subscription, and publishes a copy to the address the descriptor
reports. The copy leaves from a current-thread runtime that stops right after, which is where a
handler on dedicated threads publishes a `retry_after` copy. It must reach exactly one subscription
of a group, or each of them where your transport hands every subscription everything, and arrive
with its headers and `RETRY_COUNT_HEADER` intact. The check then requeues it twice, and a delivery
that reports `redelivery_count` must count 1, 2 and 3. Run it a second time with the bare `Name`
source when your connected form answers `Subscribe::Copies = AddressedCopies`: that answer is the
address of every `#[subscriber("orders")]` registration.

```rust
--8<-- "src/conformance/retry/tests.rs:redelivery_address"
```

`retry::broker_moves` takes a `BrokerMoves` descriptor. It declares `max_attempts(n)` and a
dead-letter destination, requeues the message until the cap is spent, and expects it in the
dead-letter destination once, after exactly `n` deliveries. Each half of the declaration on its own
must be refused at startup. Pass an `n` your broker accepts. `make_source(name)` opens the
dead-letter destination too, so it creates whatever a publish to `name` needs.

<!-- inline-rust: worked dead-letter check against the external ruststream-lapin crate; its real suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::retry;
use ruststream::nonzero;
use ruststream_lapin::{LapinBroker, LapinPublish, RabbitQuorumQueue};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_queue_dead_letters_at_the_cap() {
    retry::broker_moves(
        || InProcessBroker::new(LapinBroker::new("amqp://localhost:5672").declare_topology(true)),
        |name| RabbitQuorumQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
        nonzero!(2u32),

## The in-process transport against the server

`conformance::in_process` holds what your in-process transport declares to what your server does.
Both suites connect the broker twice per probe, with `Broker::connect` and with
`connect_in_process`, so run them where your live suites run:

<!-- inline-rust: worked check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::in_process::{self, Refusal};
use ruststream_nats::{CoreSubject, NatsBroker, NatsPublish};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn in_process_matches_the_server() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    in_process::backlog_matches_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
    in_process::refuses_like_the_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
        [
            Refusal::PayloadOver { name: "conformance.payload".to_owned(), limit: 1024 * 1024 },
            Refusal::Publish { name: "conformance.bad subject".to_owned() },
            Refusal::Subscription { source: CoreSubject::new("conformance..empty") },
        ],
    )
    .await;
}
```

- **`backlog_matches_server`** publishes to a fresh name, opens a subscription by name and
  publishes again, on both transports. The first message arrives first where
  `TestableBroker::backlog` declares `Backlog::Delivered`, and never where it declares
  `Backlog::Missed`. A server that refuses the first publish (a queue or a topic nothing declared)
  must see it refused in process too.
- **`refuses_like_the_server`** takes the refusals you know of, each a `Refusal`: a payload one
  byte over the limit, a refused destination, a refused subscription, a subscription refused
  beside another one. Both transports must refuse each. A probe your server accepts fails as a
  probe that proves nothing.


## What a message carries

Four checks in `conformance::message_shape` need an input only your broker can supply, so your crate
calls them itself, live and in process like the other suites:

| Check | Your input | Asserts |
|---|---|---|
| `keyed_order` | a subject, and where a key goes: a header or an options field | every delivery reports the key it was published under from `partition_key`, and the messages of one key arrive in publish order |
| `publish_options` | the publish policy, the cases, and a way to read a setting off a delivery | a publish with no options shows the policy's setting; a call's options win over it for that call alone; a value the transport cannot honour fails the publish and never arrives |
| `publishes_without_credentials` | a publish policy configured with a password | no binding the policy adds to the document carries the password |
| `describes_addresses_without_credentials` | a broker built from several addresses, each with a user and a password | the server description carries none of that userinfo |

A transport with no keys does not call `keyed_order`: `None` is its honest answer. Configure the
policy you pass to `publish_options` away from the transport's default, so a publisher that forgets
the policy cannot pass by landing on the default.

```rust
--8<-- "tests/conformance_message_shape.rs:keyed_order"
```

## Capability suites

If your broker implements a capability trait, run the matching suite from
`conformance::capabilities`: it proves the implementation honours the trait contract. A broker
without that capability does not call it. Each suite takes factories of the same shape as
`lifecycle` and performs a real `connect`, so enable it by the same environment variable:

| Suite | Requires | Asserts |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | the request reaches a responder with a usable `reply-to` header, the correlated reply resolves the request, a request with no answer returns an error after its timeout; a request made after an earlier one came from a runtime that has since stopped still resolves |
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
- [ ] The in-process mode ships under a `testing` feature: `InProcess` on the broker,
      `TestableBroker` on its connected form, and `register_testable_broker!(YourBroker)`. The
      in-process transport reads every setting from the production broker and never succeeds
      where the real broker fails.
- [ ] `harness::run_suite` passes (the routing surface), and the other suites pass in process
      through `harness::InProcessBroker` as well as against a real server.
- [ ] `harness::lifecycle` passes against a real server, enabled by an environment variable (the
      ladder: sync `new`, consuming `connect`, subscribe, ack, consuming `shutdown`, and the
      aliased-handle error after it).
- [ ] `lifecycle::shutdown_flushes` passes with the broker's `Backlog` answer, and
      `lifecycle::shared_handle_closes` passes where the connected form is `Clone`.
- [ ] `settlement::matches_in_process` passes against a real server, enabled by the same variable
      (ack, nack, out-of-order settlement and an unsettled drop mean the same live and in process).
- [ ] `harness::redelivery_address` passes for every `AddressedCopies` descriptor, and for the bare
      `Name` where `Subscribe::Copies` is `AddressedCopies`; `retry::broker_moves` passes for every
      `BrokerMoves` descriptor.
- [ ] Where the transport has keys or per-message settings, `message_shape::keyed_order` and
      `message_shape::publish_options` pass, in process and against a real server.
- [ ] An end-to-end suite covers broker-specific semantics, enabled by that same variable.
- [ ] `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
      `categories`), and CI checks `--no-default-features` and `--all-features`.

See [Writing a broker](index.md) for the trait contract.
