---
name: ruststream-testing
description: "Use when writing, fixing or reviewing tests of a service built on RustStream: asserting what a handler received, how it settled and what it published, retries and delays without waiting, swapping state for a test double, testing without a running broker or against one. Use it instead of mocking broker traits or sleeping."
---

# Testing a RustStream service

`TestApp` (the `testing` feature) runs the service's production app, the same builder `main`
runs, with every broker connected in process: no server, no network, no second app for tests.
A publish from the test returns once everything it set off has settled, so the next line can
assert on the result. This guidance targets ruststream 0.7.

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "test-util"] }
# a networked broker crate brings its in-process mode under its own `testing` feature:
# ruststream-nats = { version = "0.7", features = ["testing"] }
```

## A whole test module

The tests sit beside `fn app()` (`#[cfg(test)] mod tests` in the same file, or `tests/` when the
crate has a library target that exports `app`).

```rust
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmed {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Audit {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Audit)]
struct Trail;

#[subscriber("orders.incoming", reply)]
async fn confirm(
    event: &Event,
    Out(trail): Out<impl Publisher, Trail>,
) -> Result<Confirmed, HandlerOutcome> {
    if event.id == 0 {
        return Err(HandlerOutcome::drop());
    }
    if trail.message(&Audit { id: event.id }).to("audit").publish().await.is_err() {
        return Err(HandlerOutcome::retry());
    }
    Ok(Confirmed { id: event.id })
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(confirm).out(Trail, Publish))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Audit, Confirmed, Event, Trail, app};

    #[tokio::test]
    async fn an_event_is_confirmed_and_audited() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker.message(&Event { id: 5 }).to("orders.incoming").publish().await?;

        broker
            .subscriber("orders.incoming")
            .assert_called_once()
            .with(&Event { id: 5 })
            .settled(HandlerOutcome::ack());
        broker
            .published::<Confirmed>("orders.confirmed")
            .assert_called_once()
            .with(&Confirmed { id: 5 });
        // What left through one `Out` slot, addressed by its marker.
        tb.out::<Trail>()
            .assert_called_once()
            .decoded_as::<Audit>()
            .with(&Audit { id: 5 });
        Ok(())
    }

    #[tokio::test]
    async fn event_zero_is_dropped_and_nothing_leaves() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker.message(&Event { id: 0 }).to("orders.incoming").publish().await?;

        broker
            .subscriber("orders.incoming")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        broker.published::<Confirmed>("orders.confirmed").assert_not_called();
        tb.out::<Trail>().assert_not_called();
        Ok(())
    }
}
```

## Rules

### Address everything through the harness

Records are read from the broker handle by name: `broker.subscriber("subject")` for what a
handler received and how it settled, `broker.published::<T>("destination")` for what reached a
destination, `tb.out::<Marker>()` for what left through one `Out` slot. A handler function is not
an assertion target, and `tb.out` takes a slot marker, not a message type.

Wrong:

```rust
confirm.assert_called(1);
assert_eq!(tb.out::<Confirmed>().received(), vec![Confirmed { id: 5 }]);
```

Correct:

```rust
broker.subscriber("orders.incoming").assert_called(1);
broker.published::<Confirmed>("orders.confirmed").assert_called_once().with(&Confirmed { id: 5 });
```

### Test the production app, addressed by the production broker type

`TestApp::start(app())` takes the builder `main` runs. A networked broker is addressed by its own
type, `tb.broker::<NatsBroker>()`; its crate's `testing` feature runs it in process. Never build
a second app for tests, mock a broker trait, or name a stand-in broker type.

### Never sleep: settle, and advance a paused clock

`publish().await?` already drives the reaction to a standstill. `tb.settle().await?` does the same
for a reaction the test started some other way. A delay (`retry_after`) is tested on a paused
clock: `#[tokio::test(start_paused = true)]`, then `tb.advance(delay).await?`. On a running clock
the in-process `advance` panics.

To act at the moment something happens (count calls while a live stand runs, release the test on
the n-th delivery), register a hook: `.subscriber("name").on_call(async move |calls| ..)` or
`.published::<T>("name").on_publish(async move |published| ..)`. The hook gets the same
assertions, reading the record at that moment, and the publish returns after it finished.

```rust
#[tokio::test(start_paused = true)]
async fn a_failing_charge_lands_on_the_dead_letter() -> Result<(), Box<dyn Error>> {
    // `with_state` replaces what `on_startup` builds; the closure receives the app's own state type.
    let tb = TestApp::with_state(app(), |_| Shop { gateway: Gateway::failing() }).await?;
    tb.message(&Payment { id: 7 }).to("payments").publish().await?;
    let broker = tb.broker::<MemoryBroker>();
    broker
        .subscriber("payments")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(Duration::from_secs(60)));

    tb.advance(Duration::from_secs(60)).await?;
    tb.advance(Duration::from_secs(60)).await?;
    broker.subscriber("payments").assert_called(3);
    broker
        .published::<Payment>("payments.dead")
        .assert_called_once()
        .with(&Payment { id: 7 });
    Ok(())
}
```

### Replace state with `with_state`, not with a second builder

`TestApp::with_state(app(), |_| state)` starts the same app with a state the test built in place
of `on_startup`. Builder methods such as `app().state(..)` do not exist for this.

### Every harness call is fallible

`start`, `publish`, `settle` and `advance` return `Result`; the test returns
`Result<(), Box<dyn Error>>` and uses `?`.

## Quick reference

| Need | Call |
|---|---|
| Start | `TestApp::start(app()).await?`, `TestApp::with_state(app(), \|_\| state).await?` |
| Input | `broker.message(&v).to("name").publish().await?`; `tb.message(&v)` in a single-broker app; a type with a declared name needs no `.to(..)` |
| Headers | `broker.publish_with_headers("name", &v, &meta).await?`; `.with_headers(&meta)` on the input builder only for a type declaring `#[outgoing(headers = Meta)]` |
| Handler calls | `.assert_called_once()`, `.assert_called(n)`, `.assert_not_called()` |
| What it got | `.with(&v)`, `.received::<T>()`, `.batches::<T>()`, `.with_value(&v)` for a value the delivery already held |
| How it settled | `.settled(HandlerOutcome::ack())`, `.outcomes()`, `.panicked()`; `.assert_last_failed_to_decode()` ends the chain (a payload or a `Headers<T>` contract that failed) |
| Published | `broker.published::<T>("name")`, `.with(&v)`, `.with_header(..)`, `.with_options(&opts)`, `.assert_options_default()` |
| A slot | `tb.out::<Marker>()`, then `.decoded_as::<T>()` |
| Time | `#[tokio::test(start_paused = true)]` and `tb.advance(d).await?`; `tb.drain().await` for `and_after` work |
| At the event | `.subscriber("name").on_call(async move \|calls\| ..)`, `.published::<T>("name").on_publish(async move \|published\| ..)`: runs at each call or publish, before the publish returns |
| A real broker | `TestApp::start_live(app())`: the same body against a running stand |

Every function has its own page under
<https://powersemmi.github.io/ruststream/latest/components/application/testing/>, and the
[testing module](https://docs.rs/ruststream/latest/ruststream/testing/index.html) on docs.rs is
the API reference.
