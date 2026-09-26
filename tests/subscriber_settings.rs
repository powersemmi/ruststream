//! Integration tests for the declarative settings a subscriber carries: the ones the attribute
//! fixes, the ones the mount site fills in through the builder, and the four source forms.
//!
//! Every case runs on the `TestApp` harness: an injection drives the whole reaction to a
//! standstill before it returns, so what a handler saw is read off the harness afterwards rather
//! than out of state the handler recorded for the test.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::time::Duration;

use common::{Event, Order, Wire};
use ruststream::memory::{
    MemoryBroker, MemoryPosition, MemoryPublish, MemorySource, Retaining, Retention,
};
use ruststream::runtime::{
    AppInfo, DefaultSlot, HandlerOutcome, Out, PublishExt, RustStream, SubscriberSettings,
};
use ruststream::testing::TestApp;
use ruststream::{Buffered, Deserialized, Name, Publisher, nonzero, subscriber};

/// The payload view the raw batch body below takes, one element per delivery in the batch.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A broker for the tests that open a subscription at a log position: `start_at(..)` replays
/// what the broker kept, so it has to keep something.
fn replaying() -> MemoryBroker<Retaining> {
    MemoryBroker::retaining(Retention::Messages(nonzero!(32)))
}

/// The shortest source form: the by-name source with its value left to the mount site.
#[subscriber]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_attribute_is_named_at_the_mount_site() {
    // The name is a value the service only knows here, which is the whole point of the form.
    let subject = format!("audit-{}", 7);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), {
        let subject = subject.clone();
        |b| {
            b.include(audit.name(subject));
        }
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 11 })
        .to(&*subject)
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber(&subject)
        .assert_called_once()
        .with(&Order { id: 11 })
        .settled(HandlerOutcome::ack());
}

/// A named kind carrying only what it needs to exist: the value arrives through the builder,
/// which constructs it through the kind's own from-name constructor.
#[subscriber(MemorySource)]
async fn record(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_kind_is_built_from_the_name_the_mount_site_gives() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // `map_source` is the hook a broker's own settings trait layers on: it sees the source
        // the name built and hands back the one the subscription opens.
        b.include(
            record
                .name("record-kind")
                .map_source(|_named| MemorySource::new("record-mapped")),
        );
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 3 })
        .to("record-mapped")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("record-mapped")
        .assert_called_once()
        .with(&Order { id: 3 })
        .settled(HandlerOutcome::ack());
}

/// The batch shape read off the signature; the mount site names how big a batch is and the broker
/// builds its batches to it. The body records nothing; the harness reports the batches it was
/// handed.
#[subscriber]
async fn paginate(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_batch_size() {
    let broker = replaying();
    let publisher = broker.publisher();
    // The whole run is in the log before the subscription opens, so the opening replay has three
    // entries to hand over and the size is what shapes them into batches. The harness injects
    // after startup and settles per message, so the entries are published here.
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("paginate")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            paginate
                .name("paginate")
                .start_at(MemoryPosition::start())
                .batch(nonzero!(2)),
        );
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batch settles");

    let subscriber = tb.broker::<MemoryBroker<Retaining>>();
    let subscriber = subscriber.subscriber("paginate");
    assert_eq!(
        subscriber.received::<Order>(),
        [Order { id: 0 }, Order { id: 1 }, Order { id: 2 }],
    );
    subscriber
        // The broker built the batches to the size the mount named: two, then the remainder.
        .assert_batch_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown failed");
}

/// A batch that answers: one call per delivered batch, with that batch's own reply vector.
#[subscriber(publish("batch-cap-confirmed"))]
async fn confirm_batches(orders: &[Order]) -> Vec<Event> {
    orders
        .iter()
        .map(|order| Event {
            id: u64::from(order.id),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_batch_size_reaches_a_replying_batch() {
    let broker = replaying();
    let publisher = broker.publisher();
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("batch-cap-reply")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            confirm_batches
                .name("batch-cap-reply")
                .start_at(MemoryPosition::start())
                .batch(nonzero!(2)),
        );
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batch settles");

    let handle = tb.broker::<MemoryBroker<Retaining>>();
    handle
        .subscriber("batch-cap-reply")
        .assert_batch_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());
    // Each batch answered for its own elements, and the replies leave in batch order.
    assert_eq!(
        handle.published::<Event>("batch-cap-confirmed").decoded(),
        [Event { id: 0 }, Event { id: 1 }, Event { id: 2 }],
    );
    tb.shutdown().await.expect("shutdown failed");
}

/// A batch that fans out through a slot: the arena rides every batch the broker delivers.
#[subscriber]
async fn fan_out_batches(orders: &[Order], Out(out): Out<impl Publisher>) -> HandlerOutcome {
    for order in orders {
        if out
            .message(&Event {
                id: u64::from(order.id),
            })
            .to("batch-cap-fanned")
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_batch_size_reaches_a_slot_carrying_batch() {
    let broker = replaying();
    let publisher = broker.publisher();
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("batch-cap-slots")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            fan_out_batches
                .name("batch-cap-slots")
                .start_at(MemoryPosition::start())
                .batch(nonzero!(2)),
        )
        .out(DefaultSlot, MemoryPublish)
        .build();
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batch settles");

    let handle = tb.broker::<MemoryBroker<Retaining>>();
    handle
        .subscriber("batch-cap-slots")
        .assert_batch_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());
    assert_eq!(
        handle.published::<Event>("batch-cap-fanned").decoded(),
        [Event { id: 0 }, Event { id: 1 }, Event { id: 2 }],
    );
    tb.shutdown().await.expect("shutdown failed");
}

/// The client-side buffer composes with a start position: a batch subscription assembled out of
/// single deliveries still opens where the mount site says. The adapter is what a broker crate
/// gives a transport with no native batches, named here by hand to pin the composition.
#[subscriber(Buffered::<Name>::new(Name::new("buffered-replay")).max_wait(Duration::from_millis(5)))]
async fn replay_batches(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_buffer_composes_with_a_start_position() {
    let broker = replaying();
    let publisher = broker.publisher();
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("buffered-replay")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            replay_batches
                .batch(nonzero!(8))
                .start_at(MemoryPosition::start()),
        );
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batches settle");

    let handle = tb.broker::<MemoryBroker<Retaining>>();
    let subscriber = handle.subscriber("buffered-replay");
    // Everything published before the subscription opened is replayed, through the buffer.
    assert_eq!(
        subscriber.received::<Order>(),
        [Order { id: 0 }, Order { id: 1 }, Order { id: 2 }],
    );
    subscriber
        .assert_batch_sizes(&[3])
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown failed");
}

/// A batch of payloads: the typed batch without the decode step, borrowed from the batch's own
/// messages.
#[subscriber("frames")]
async fn ingest(frames: &[Frame<'_>]) -> HandlerOutcome {
    let _ = frames.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raw_batch_handler_borrows_the_payloads() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(ingest.batch(nonzero!(8)));
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    for frame in [b"one".as_slice(), b"two".as_slice()] {
        tb.message(&Wire::of(frame))
            .to("frames")
            .publish()
            .await
            .expect("publish failed");
    }

    let subscriber = tb.broker::<MemoryBroker>();
    let subscriber = subscriber.subscriber("frames");
    // The bytes reach the body as they were published: nothing decodes a self-deserializing view.
    assert_eq!(
        subscriber.received_raw(),
        [b"one".as_slice(), b"two".as_slice()],
    );
    subscriber.settled(HandlerOutcome::ack());
}

/// The per-registration codec, the top rung of the codec ladder: a scope decoding with CBOR, and
/// registrations that name JSON for themselves.
///
/// One case per input kind the override resolves against - a decoded body, a body paired with its
/// typed header contract, and a self-deserializing one. The last reads no codec at all, so naming
/// one there changes nothing and the delivery's bytes still arrive untouched; the first two would
/// fail to decode the JSON payloads below if the scope's CBOR had won.
#[cfg(feature = "cbor")]
mod codec_override {
    use ruststream::codec::{CborCodec, JsonCodec};
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, HandlerOutcome, Message, RustStream, SubscriberSettings};
    use ruststream::testing::{Outcome, TestApp};
    use ruststream::{Outgoing, subscriber};
    use serde::{Deserialize, Serialize};

    use super::{Frame, Order, Wire};

    /// The contract the paired case reads off the delivery's headers.
    #[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
    struct Meta {
        shard: u8,
    }

    /// The outgoing side of the paired case: an [`Order`] body declaring the contract the
    /// subscriber pairs it with, so the publish is asked for the headers it must carry.
    #[derive(Serialize, Outgoing)]
    #[outgoing(headers = Meta)]
    struct PairedOrder {
        id: u32,
    }

    #[subscriber]
    async fn decoded(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber]
    async fn paired(order: &Message<Meta, Order>) -> HandlerOutcome {
        let _ = (order.body.id, order.headers.shard);
        HandlerOutcome::ack()
    }

    /// A self-deserializing body with a reply, which is the shape that resolves a codec for a
    /// byte input: the plain self-deserializing mount reads none at all.
    #[subscriber(publish("codec-provided-out"))]
    async fn provided(frame: &Frame<'_>) -> Order {
        Order {
            id: u32::try_from(frame.0.len()).unwrap_or(u32::MAX),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_builder_overrides_the_scope_codec_per_registration() {
        let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            CborCodec,
            |b| {
                b.include(decoded.name("codec-decoded").codec(JsonCodec));
                b.include(paired.name("codec-paired").codec(JsonCodec));
                b.include(provided.name("codec-provided").codec(JsonCodec));
            },
        );
        let tb = TestApp::start(app).await.expect("startup failed");

        tb.message(&Order { id: 1 })
            .to("codec-decoded")
            .publish()
            .await
            .expect("publish failed");
        tb.message(&PairedOrder { id: 2 })
            .to("codec-paired")
            .with_headers(&Meta { shard: 7 })
            .publish()
            .await
            .expect("publish failed");
        tb.message(&Wire::of(b"\x00\xffnot json"))
            .to("codec-provided")
            .publish()
            .await
            .expect("publish failed");

        let handle = tb.broker::<MemoryBroker>();
        // The registration's own codec decoded the body; the scope's CBOR would have failed here.
        handle
            .subscriber("codec-decoded")
            .assert_called_once()
            .with_codec(&JsonCodec, &Order { id: 1 })
            .settled(HandlerOutcome::ack());
        // The pair materialized whole: an unreadable header contract settles as a decode failure
        // before the body runs, so the ack is what says both halves arrived.
        handle
            .subscriber("codec-paired")
            .assert_called_once()
            .assert_outcome(Outcome::Ack)
            .with_codec(&JsonCodec, &Order { id: 2 });
        // A self-deserializing body reads no codec at all, so the bytes arrive untouched.
        handle
            .subscriber("codec-provided")
            .assert_called_once()
            .with_raw(b"\x00\xffnot json")
            .settled(HandlerOutcome::ack());
    }
}
