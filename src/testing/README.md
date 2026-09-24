Application testing support, behind the `testing` feature.

[`TestApp`] runs the service's production app: the same builder `main` runs, handed to the
harness unchanged. [`TestApp::start`] connects every broker of the app in process, with no
server; [`TestApp::start_live`] connects them to a running stand. The harness records what each
handler received (raw and decoded), how it settled, and what it published, and a test addresses
each broker by its production type: `tb.broker::<KafkaBroker>()`. The same test body runs in
both modes; only the start call differs.

In process, every broker connects through its [`InProcess`] transition instead of `connect`.
The transition yields the broker's own connected form over an in-process transport, so the
routes, publish policies and descriptors resolve exactly as in production. The in-process mode
is part of each broker's contract. A broker crate provides it under its `testing` feature, so a
service enables that feature in its `[dev-dependencies]`, and a broker without it makes
[`TestApp::start`] fail with [`TestError::NoTransport`] naming the broker. The transport reads
its settings from the production broker and has none of its own, and it never succeeds where
the real broker fails. [`MemoryBroker`](crate::memory::MemoryBroker) has no server, so its
in-process mode is simply its `connect`. A broker author implements [`InProcess`] and
[`TestableBroker`] and registers the broker with
[`register_testable_broker!`](crate::register_testable_broker); the
[`conformance`](crate::conformance) suite checks the same transport.

# Examples

Enable the feature in the dev-dependencies, hand the harness the app production builds, and
publish an input. The publish drives the whole reaction to completion before
it returns: the handler, its downstream publishes, any cross-broker cascade. Then assert.

```
# #[cfg(all(feature = "testing", feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
    accepted: bool,
}

#[subscriber("orders", publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// The app `main` runs, and the one the tests hand the harness.
pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm).out_reply(Publish);
    })
}

pub async fn confirms_valid_orders() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(app()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1, quantity: 2 })
        .to("orders")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1, quantity: 2 })
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            id: 1,
            accepted: true,
        });
    Ok(())
}
# }
# #[cfg(all(feature = "testing", feature = "macros", feature = "memory", feature = "json"))]
# fn main() {
#     tokio::runtime::Builder::new_multi_thread()
#         .enable_all()
#         .build()
#         .unwrap()
#         .block_on(demo::confirms_valid_orders())
#         .unwrap();
# }
# #[cfg(not(all(
#     feature = "testing", feature = "macros", feature = "memory", feature = "json"
# )))]
# fn main() {}
```

# What a test can say

[`TestApp::broker`] addresses a broker by its production type,
[`broker_named`](TestApp::broker_named) by the label of `with_broker_labeled`; a single-broker
app may leave it out and publish through [`TestApp::message`]. Input goes in through the same
builder the service publishes with: `message(&value)` for an
[`Outgoing`](macro@crate::Outgoing) value, `with_headers(&meta)` for a typed contract,
`to(name)` when the type names no destination. Bytes that are no model ride
a `#[derive(Outgoing, Serialized)]` newtype, which is how a test injects an undecodable
payload or the input of a handler that decodes the bytes itself.

`subscriber(name)` asserts on what a handler received. `assert_called_once`,
`assert_called(n)` and `assert_not_called` count handler calls, one per delivery or one per
batch; `with(&value)` and `with_raw(bytes)` read the most recent call's payload;
`settled(outcome)` and `assert_outcome(..)` how it settled; `assert_batch_sizes(&[2, 1])` how
the stream was cut; `panicked()` and `assert_last_failed_to_decode()` the failures.
`received::<T>()`, `received_raw()`, `batches::<T>()` and `outcomes()` return the lists for a
check of your own. `published::<T>(name)` reads what was published there with the same
vocabulary, plus `with_header(key, value)` for what a transform or a publish layer added.
[`TestApp::out`] reads exactly what left through one `Out` slot, across brokers, and
`with_options` and `assert_options_default` read the per-message settings a publish carried.
The decoding assertions use the default codec; a mounting with another codec passes it
through the `with_codec`, `received_with` and `decoded_with` variants.

The harness runs dispatch under the app's real failure policy. A panic under the default
`fail_fast` shuts the service down, [`run_result`](TestApp::run_result) returns what `run`
would, and [`assert_running`](TestApp::assert_running) states the opposite. A handler
answering `retry_after` records the immediate outcome, and [`advance`](TestApp::advance)
lets the delay pass to drive the redelivery. A copy the runtime publishes for a zero delay
goes out at once, so it arrives in the same reaction, without `advance`.
[`settle`](TestApp::settle) drives to completion a reaction the test started through a bare
publisher, which is how a batch gets more than one element on a broker that assembles its
batches on the client.

# Against a running broker

[`TestApp::start_live`] takes the same app and connects each broker through its ordinary
`connect`, so the test body above runs against a stand. A live test needs nothing from a broker
crate beyond its connected form; the test's input reaches the broker through the connected
form's default publish policy.

```no_run
# #[cfg(all(feature = "testing", feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use ruststream::testing::{TestApp, TestError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
#[outgoing(name = "orders")]
pub struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

pub fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(accept);
    })
}

/// One body for both modes.
pub async fn accepts(tb: TestApp<()>) -> Result<(), Box<dyn std::error::Error>> {
    tb.broker::<MemoryBroker>().message(&Order { id: 1 }).publish().await?;
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.shutdown().await?;
    Ok(())
}

pub async fn in_process() -> Result<(), Box<dyn std::error::Error>> {
    accepts(TestApp::start(app()).await?).await
}

pub async fn live() -> Result<(), Box<dyn std::error::Error>> {
    accepts(TestApp::start_live(app()).await?).await
}
# }
# fn main() {}
```

What changes live is how the harness knows the reaction is over. It cannot read a broker's
queues, so it records what is published through the app's own wiring: the test's publishes, and
every message a publisher the runtime paired for the app's wiring hands a broker. That covers
replies, `Out` slot publishes and requests, retry copies, dead letters, and a `Bound` token's
publishes through any of them. Each is recorded on the
broker it goes to: the one the test named, or the one the publisher was paired against. A
handler on one broker holding a `Bound` token for another publishes to the other, and the
publish is that broker's.

A publish, a settle and an advance then wait until every subscription has handled each recorded
publish its broker delivers to it, and every redelivery that fell due, and no handler is
running. Which subscriptions a publish reaches is the broker's own routing, which the broker
answers through [`TestableBroker::routes`]: an equal name for [`MemoryBroker`](crate::memory::MemoryBroker),
every matching subscription for a broker that fans out, the one it picks for a broker that picks
one. A subscription of another broker never owes it. The wait is bounded: ten seconds by
default, [`start_live_within`](TestApp::start_live_within) sets another, and past it the call
fails with [`TestError::NotSettled`] naming the subscription it was waiting on. A live
[`advance`](TestApp::advance) lets the time pass for real, because the delay is the broker's own
timer, so a live test runs on the running clock, and `start_live` refuses a paused one.

A publisher the service uses itself is outside this record: one kept in its state, one an
`after_startup` hook is handed or pairs, one from [`Bound::live`](crate::runtime::Bound::live) or
[`RunningApp::publisher`](crate::runtime::RunningApp::publisher), and one a broker crate builds
outside the runtime's pairing. The harness neither waits for its publishes nor lists them; a
test sends its own input through `tb.broker::<B>().message(..)`.

`subscriber(..)` and `settled(..)` read the same record in both modes. `published::<T>(name)`
reads the broker's log in process and the harness's record live: the test's publishes onto that
broker, and the app's publishes that went to it, each as the framework handed it over. A test
that runs in both modes asserts on those. What only the broker does is asserted in process: a
setting its publisher turns into a header or a prefix, and a transaction, which commits on the
broker. What a broker moves on its own (a native delayed redelivery, its own dead-lettering) is
asserted through the subscription that receives it. Each mode covers what the other cannot: in process
the broker's server-side semantics are the transport's model of them, and live they are the
server's own.

Either way, every subscription loop and every worker of `workers(n)` runs as a task on the
test's own runtime, whatever runtime the generated `main` builds, so
[`advance`](TestApp::advance) reaches every timer the app arms and [`settle`](TestApp::settle)
sees every delivery.
