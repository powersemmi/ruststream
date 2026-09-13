//! Delayed redelivery from the Subscribers guide: `retry_after` for the not-ready-yet case,
//! and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example retry --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Names, Outgoing, PublishContext, Router, RouterDef};
use ruststream::{NamedCopies, Subscribe, SubscriptionSource};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
#[subscriber("payments")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    println!("payment {} settled", payment.id);
    HandlerOutcome::ack()
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the batch.
#[subscriber("payments")]
async fn reconcile_batch(payments: &[Payment]) -> Vec<HandlerOutcome> {
    payments
        .iter()
        .map(|payment| {
            if payment.settled {
                HandlerOutcome::ack()
            } else {
                HandlerOutcome::retry_after(Duration::from_secs(30))
            }
        })
        .collect()
}
// --8<-- [end:batch_retry_after]

// --8<-- [start:filter]
/// A subscription that reads many destinations and addresses none of them: a wildcard subject, an
/// MQTT filter, a Pulsar pattern. Its descriptor says so with `Copies = NamedCopies`, and the
/// mount site is then what names where a copy of a delivery goes.
#[derive(Debug, Clone)]
struct PaymentFilter;

impl<C: Subscribe> SubscriptionSource<C> for PaymentFilter {
    type Subscriber = C::Subscriber;
    type Copies = NamedCopies;

    // The returned lifetime is fixed by the trait, so it cannot be narrowed to `&'static str`.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "payments.*"
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe("payments.*").await
    }
}

/// The same handler on that subscription.
#[subscriber(PaymentFilter {})]
async fn reconcile_filtered(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    HandlerOutcome::ack()
}

/// Names where each copy goes from the delivery it is a copy of: a wildcard subscription reads
/// many topics, and this sends a copy back to the one it came in on.
struct ToDeliveryTopic;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToDeliveryTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        let topic = cx
            .headers()
            .get_str("x-topic")
            .unwrap_or_else(|| cx.name())
            .to_owned();
        out.set_name(topic);
    }
}
// --8<-- [end:filter]

// --8<-- [start:mount]
/// A transform on the retry position: it stamps every copy with the subscription the delivery
/// came from, so a redelivery is recognisable downstream. Transforms here read the delivery being
/// retried, the way a reply's do.
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // --8<-- [start:declaration]
        // A poison message is one that never settles, and the cap is what ends it: after five
        // deliveries the payment goes to the dead-letter subject instead of coming back.
        b.include(reconcile)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead");
        // --8<-- [end:declaration]
        // --8<-- [start:named]
        // The descriptor of this subscription addresses nothing, so the mount site names where a
        // copy goes. `.to(name)` comes before the publisher: a scope's registration commits when
        // the statement ends, so the chain has to say it before it can pass through.
        b.include(reconcile_filtered)
            .to("payments.retry")
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead");
        // --8<-- [end:named]
        // Every registration already has a publisher for its copies, taken from the broker's
        // default policy. Naming one replaces it: another policy, another codec, a transform.
        // The position is an `Out` slot, so it takes the slot steps, and a copy carries the
        // delivery's own bytes, so the codec named here resolves the position and encodes
        // nothing.
        b.include(reconcile_batch.batch(nonzero!(64)))
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead")
            .out_retry(Publish)
            .codec(JsonCodec)
            .transform(DeferredStamp);
        // The naming transform below rides a router chain, whose terminal is `.build()`.
        b.include_router(routed());
    })
}
// --8<-- [end:mount]

// --8<-- [start:naming_transform]
/// A naming transform names the destination per delivery, so it mounts only where nothing else
/// has: an unaddressed descriptor with no `.to(name)`. The chain here ends in `.build()`.
fn routed() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(reconcile_filtered)
        .out_retry(Publish)
        .transform(ToDeliveryTopic)
        .build()
}
// --8<-- [end:naming_transform]
