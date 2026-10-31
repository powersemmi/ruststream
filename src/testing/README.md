Application testing support, behind the `testing` feature.

[`TestApp`] runs the service's production app, the same builder `main` runs, with every broker
connected in process and no server; the same test body runs against a running stand. A test
addresses each broker by its production type, `tb.broker::<KafkaBroker>()`.
[Testing](https://powersemmi.github.io/ruststream/latest/components/application/testing/) on the site walks through it.

A broker crate provides the in-process mode under its own `testing` feature: it implements
[`InProcess`] and [`TestableBroker`] and registers the broker with
[`register_testable_broker!`](crate::register_testable_broker), and the
[`conformance`](crate::conformance) suite checks the same transport.

# Starting and stopping

- [Starting and stopping](https://powersemmi.github.io/ruststream/latest/components/application/testing/starting-and-stopping/): how a test starts the production app, gives it state, and shuts it down.
- [start](https://powersemmi.github.io/ruststream/latest/components/application/testing/starting-and-stopping/start/): [`TestApp::start`] runs the app with every broker connected in process.
- [with_state](https://powersemmi.github.io/ruststream/latest/components/application/testing/starting-and-stopping/with-state/): [`TestApp::with_state`] starts it with a state the test builds in place of `on_startup`.
- [shutdown](https://powersemmi.github.io/ruststream/latest/components/application/testing/starting-and-stopping/shutdown/): [`TestApp::shutdown`] tears the service down and returns what `run` would return.

# Driving

- [Driving](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/): how a test publishes to a broker of the app and moves the app forward.
- [broker](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/broker/): [`TestApp::broker`] addresses a broker by its production type.
- [broker_named](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/broker-named/): [`TestApp::broker_named`] addresses one by the label it was registered under.
- [message](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/message/): [`BrokerHandle::message`] and [`TestApp::message`] start an input on the service's own publish builder.
- [publish](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/publish/): `publish()` sends the input and returns once the whole reaction has settled.
- [Shortcuts](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/publish-shortcuts/): [`TestApp::publish`], [`BrokerHandle::publish`] and [`BrokerHandle::publish_with_headers`] send a serde value in one call.
- [settle](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/settle/): [`TestApp::settle`] drives a reaction the test started without the harness to a standstill.
- [advance](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/advance/): [`TestApp::advance`] lets a delay pass and drives the redeliveries now due.
- [drain](https://powersemmi.github.io/ruststream/latest/components/application/testing/driving/drain/): [`TestApp::drain`] waits for the post-settle continuations.

# What a test can say

- [Records](https://powersemmi.github.io/ruststream/latest/components/application/testing/records/): the records a test reads.
- [subscriber](https://powersemmi.github.io/ruststream/latest/components/application/testing/records/subscriber/): [`BrokerHandle::subscriber`] opens what one subscription received and how it settled, as [`SubscriberAssertions`].
- [published](https://powersemmi.github.io/ruststream/latest/components/application/testing/records/published/): [`BrokerHandle::published`] opens what was published to one destination, as [`PublishedAssertions`].
- [out](https://powersemmi.github.io/ruststream/latest/components/application/testing/records/out/): [`TestApp::out`] opens what went through one `Out` slot, on whichever broker.

Subscriber assertions, [Subscriber assertions](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/):

- [assert_called_once](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-called-once/), [assert_called](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-called/), [assert_not_called](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-not-called/): how often the handler ran ([`SubscriberAssertions::assert_called_once`], [`SubscriberAssertions::assert_called`], [`SubscriberAssertions::assert_not_called`]).
- [with](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/with/): [`SubscriberAssertions::with`], the decoded value of the latest call; [`SubscriberAssertions::with_codec`] names the codec.
- [with_raw](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/with-raw/): [`SubscriberAssertions::with_raw`], its bytes.
- [with_value](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/with-value/): [`SubscriberAssertions::with_value`], the value a delivery lent its handler.
- [settled](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/settled/): [`SubscriberAssertions::settled`], how the latest call settled.
- [assert_outcome](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-outcome/): [`SubscriberAssertions::assert_outcome`], the latest call's classified [`Outcome`].
- [assert_batch_sizes](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-batch-sizes/): [`SubscriberAssertions::assert_batch_sizes`], how the deliveries were cut into calls.
- [panicked](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/panicked/): [`SubscriberAssertions::panicked`].
- [assert_last_failed_to_decode](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/assert-last-failed-to-decode/): [`SubscriberAssertions::assert_last_failed_to_decode`].
- [received](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/received/): [`SubscriberAssertions::received`] and [`SubscriberAssertions::received_with`], every decoded payload.
- [received_raw](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/received-raw/): [`SubscriberAssertions::received_raw`], every payload's bytes.
- [received_values](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/received-values/): [`SubscriberAssertions::received_values`], every lent value, batch elements included.
- [batches](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/batches/): [`SubscriberAssertions::batches`] and [`SubscriberAssertions::batches_with`], one decoded list per call.
- [batches_raw](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/batches-raw/): [`SubscriberAssertions::batches_raw`].
- [outcomes](https://powersemmi.github.io/ruststream/latest/components/application/testing/subscriber-assertions/outcomes/): [`SubscriberAssertions::outcomes`], every call's settlement in order.

Published assertions, [Published assertions](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/):

- [assert_called_once](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/assert-called-once/), [assert_called](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/assert-called/), [assert_not_called](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/assert-not-called/): how many messages were published ([`PublishedAssertions::assert_called_once`], [`PublishedAssertions::assert_called`], [`PublishedAssertions::assert_not_called`]).
- [with](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/with/): [`PublishedAssertions::with`], the latest decoded payload; [`PublishedAssertions::with_codec`] names the codec.
- [with_raw](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/with-raw/): [`PublishedAssertions::with_raw`].
- [with_header](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/with-header/): [`PublishedAssertions::with_header`], one header of the latest message.
- [with_options](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/with-options/): [`PublishedAssertions::with_options`], the per-message settings of the latest publish.
- [assert_options_default](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/assert-options-default/): [`PublishedAssertions::assert_options_default`].
- [decoded](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/decoded/): [`PublishedAssertions::decoded`] and [`PublishedAssertions::decoded_with`], every payload as a value.
- [decoded_as](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/decoded-as/): [`PublishedAssertions::decoded_as`] re-types a record.
- [messages](https://powersemmi.github.io/ruststream/latest/components/application/testing/published-assertions/messages/): [`PublishedAssertions::messages`], every message with its name and headers.

Service state, [Service state](https://powersemmi.github.io/ruststream/latest/components/application/testing/service-state/):

- [assert_running](https://powersemmi.github.io/ruststream/latest/components/application/testing/service-state/assert-running/): [`TestApp::assert_running`].
- [assert_shut_down](https://powersemmi.github.io/ruststream/latest/components/application/testing/service-state/assert-shut-down/): [`TestApp::assert_shut_down`].
- [run_result](https://powersemmi.github.io/ruststream/latest/components/application/testing/service-state/run-result/): [`TestApp::run_result`], what `run` would return now.

# Hooks

- [Hooks](https://powersemmi.github.io/ruststream/latest/components/application/testing/hooks/): code a test runs at the moment something happens, instead of reading the record afterwards.
- [on_call](https://powersemmi.github.io/ruststream/latest/components/application/testing/hooks/on-call/): [`SubscriberAssertions::on_call`] runs an async closure each time the subscription's handler is called.
- [on_publish](https://powersemmi.github.io/ruststream/latest/components/application/testing/hooks/on-publish/): [`PublishedAssertions::on_publish`] runs one each time a message is published to the destination or through the slot.

```
# #[cfg(all(feature = "testing", feature = "memory", feature = "macros", feature = "json"))]
# mod demo {
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

#[subscriber("orders", reply)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { order_id: order.id }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm).out_reply(Publish)
    })
}

pub async fn every_order_is_confirmed_as_it_arrives() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |orders| {
            let count = seen.fetch_add(1, Ordering::SeqCst) + 1;
            orders.assert_called(count).settled(HandlerOutcome::ack());
        });

    let (audit, mut audited) = mpsc::channel(8);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .on_publish(async move |confirmations| {
            let last = confirmations.decoded().pop();
            let _ = audit.send(last).await;
        });

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .publish()
        .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(audited.recv().await.flatten(), Some(Confirmation { order_id: 7 }));
    Ok(())
}
# }
# fn main() {}
```

A hook gets the same handle it was registered on, borrowed for the call, so it reads the record as
it stood at the event with the assertions a test already uses. The publish that caused the event
returns once its hooks finished, and a failed assertion inside a hook fails that publish, or the shutdown or the end of a test that drove nothing further. Hooks run
under [`TestApp::start`] and [`TestApp::start_live`] alike.

# Against a running broker

- [start_live](https://powersemmi.github.io/ruststream/latest/components/application/testing/starting-and-stopping/start-live/): [`TestApp::start_live`] connects the same app to a running stand, and [`TestApp::start_live_within`] bounds the wait for a reaction.
