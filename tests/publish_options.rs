//! A broker's per-message settings: defaulted by the publish policy at the mount site, adjusted
//! per call by steps the broker adds to the publish builder.
//!
//! The broker half here is what a broker crate ships - an options type whose every field is
//! optional, a policy carrying the defaults, a live publisher resolving one against the other,
//! and an extension trait bounded on the options type so its steps appear on a builder over this
//! publisher and nowhere else. The in-memory bus has no protocol field to put a priority in, so
//! the publisher folds the resolved value into a header the assertions read back.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "cbor",
    feature = "testing"
))]

mod common;

use common::{Order, Receipt};

use ruststream::codec::CborCodec;
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryPublisher};
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, PairError};

// --8<-- [start:broker_side]
/// The broker's per-message settings. Every field optional: what a call leaves unset keeps what
/// the policy fixed. `Debug` and `PartialEq` are not part of the contract, they are what a test
/// naming this type needs to assert on it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PriorityOptions {
    priority: Option<u8>,
}

/// The publish policy: pure declaration, constructible anywhere, and the place the defaults are
/// configured.
#[derive(Debug, Clone, Copy, Default)]
struct PriorityPublish {
    priority: u8,
}

impl PriorityPublish {
    fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// The live publisher: the connection, plus the defaults the policy carried.
struct PriorityPublisher {
    inner: MemoryPublisher,
    default_priority: u8,
}

impl PublishPolicy<ConnectedMemoryBroker> for PriorityPublish {
    type Live = PriorityPublisher;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(PriorityPublisher {
            inner: Publish.pair(connected).await?,
            default_priority: self.priority,
        })
    }
}

impl Publisher for PriorityPublisher {
    type Error = MemoryError;
    type Options = PriorityOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let priority = options
            .and_then(|options| options.priority)
            .unwrap_or(self.default_priority);
        // A real broker hands the resolved value to its client as the protocol field it is. The
        // in-memory bus has no such field, so this one puts it where a test can read it back.
        let mut headers = msg.headers().clone();
        headers.insert("priority", priority.to_string());
        let stamped = OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers);
        self.inner.publish(stamped, None).await
    }
}

impl TransactionalPublisher for PriorityPublisher {
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.inner.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.abort().await
    }
}

/// The steps the broker puts in its prelude. The bound on the sink's options type is what keeps
/// them off a builder over any other broker's publisher.
trait PriorityPublishSteps {
    /// Sends this one message at `priority`, whatever the mount site's default is.
    #[must_use]
    fn priority(self, priority: u8) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> PriorityPublishSteps
    for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = PriorityOptions>,
{
    fn priority(mut self, priority: u8) -> Self {
        self.options_mut()
            .get_or_insert_with(PriorityOptions::default)
            .priority = Some(priority);
        self
    }
}
// --8<-- [end:broker_side]

#[derive(OutSlot)]
#[publishes(Order)]
struct Ledger;

// --8<-- [start:handler]
// A body that adjusts a per-message setting names the broker's step, so it imports that broker's
// prelude - the stated exception to "a body imports the framework prelude alone".
#[subscriber("options.in")]
async fn record(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = PriorityOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(order)
        .to("options.normal")
        .publish()
        .await
        .is_err()
        || ledger
            .message(order)
            .to("options.urgent")
            .priority(9)
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

/// What a call leaves alone is what the mount site fixed, and a step wins over it for that one
/// message. Both publishes leave the same slot, in the same handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_site_default_holds_until_a_step_overrides_it() {
    // --8<-- [start:mount]
    let app =
        RustStream::new(AppInfo::new("options", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(record)
                .out(Ledger, PriorityPublish::default().priority(3))
                .build();
        });
    // --8<-- [end:mount]
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("options.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("options.normal")
        .assert_called_once()
        .with(&Order { id: 7 })
        .with_header("priority", "3");
    tb.broker::<MemoryBroker>()
        .published::<Order>("options.urgent")
        .assert_called_once()
        .with(&Order { id: 7 })
        .with_header("priority", "9");
}

/// The defect this design closes: a step is a position on the builder, not a wrapper around the
/// publisher, so the publish it finishes still encodes with the codec the mount site named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_step_keeps_the_codec_the_mount_site_named() {
    let app = RustStream::new(AppInfo::new("options-codec", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(record)
                .out(Ledger, PriorityPublish::default().priority(3))
                .codec(CborCodec)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("options.in")
        .publish()
        .await
        .expect("publish");

    // Both messages are CBOR, the slot's codec - the stepped one included.
    tb.out::<Ledger>()
        .assert_called(2)
        .decoded_as::<Order>()
        .with_codec(&CborCodec, &Order { id: 7 });
    tb.broker::<MemoryBroker>()
        .published::<Order>("options.urgent")
        .assert_called_once()
        .with_codec(&CborCodec, &Order { id: 7 })
        .with_header("priority", "9");
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Staged;

/// A transaction opened on the slot publishes through the same builder, so the same step reaches
/// the buffered message.
#[subscriber("options.txn.in")]
async fn stage(
    order: &Order,
    Out(staged): Out<impl TransactionalPublisher<Options = PriorityOptions>, Staged>,
) -> HandlerOutcome {
    let Ok(scope) = staged.begin().await else {
        return HandlerOutcome::retry();
    };
    if scope
        .message(order)
        .to("options.staged")
        .priority(5)
        .publish()
        .await
        .is_err()
    {
        let _ = scope.abort().await;
        return HandlerOutcome::retry();
    }
    if scope.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_step_inside_a_transaction_scope_reaches_the_broker() {
    let app = RustStream::new(AppInfo::new("options-txn", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(stage)
                .out(Staged, PriorityPublish::default().priority(3))
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .to("options.txn.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("options.staged")
        .assert_called_once()
        .with(&Order { id: 4 })
        .with_header("priority", "5");
    // A transaction scope publishes through the slot's own publisher, so the slot view records
    // the step the buffered message carried.
    tb.out::<Staged>()
        .assert_called_once()
        .with_options(&PriorityOptions { priority: Some(5) });
}

/// The steps are on the builder, so they are there on a bare publisher too - here the one a
/// startup hook is handed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_publisher_takes_the_same_steps() {
    let app = RustStream::new(AppInfo::new("options-startup", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.after_startup(
                PriorityPublish::default().priority(3),
                async move |publisher: PriorityPublisher| {
                    publisher
                        .message(&Order { id: 1 })
                        .to("options.announce")
                        .publish()
                        .await?;
                    publisher
                        .message(&Order { id: 2 })
                        .to("options.announce.urgent")
                        .priority(8)
                        .publish()
                        .await
                },
            );
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Order>("options.announce")
        .assert_called_once()
        .with_header("priority", "3");
    tb.broker::<MemoryBroker>()
        .published::<Order>("options.announce.urgent")
        .assert_called_once()
        .with_header("priority", "8");
}

/// A reply adjusts nothing: it has no call site, so the policy the mount site names for the
/// `Reply` position is the whole answer.
#[subscriber("options.reply.in", publish("options.reply.out"))]
async fn acknowledge(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reply_position_takes_the_policy_defaults() {
    // --8<-- [start:reply_mount]
    let app = RustStream::new(AppInfo::new("options-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(acknowledge)
                .out_reply(PriorityPublish::default().priority(4));
        },
    );
    // --8<-- [end:reply_mount]
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 2 })
        .to("options.reply.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("options.reply.out")
        .assert_called_once()
        .with(&Receipt { id: 2 })
        .with_header("priority", "4");
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Notes;

/// A body that names no step: the publish carries no options, and the policy's own setting is the
/// whole answer.
#[subscriber("options.note.in")]
async fn note(
    order: &Order,
    Out(notes): Out<impl Publisher<Options = PriorityOptions>, Notes>,
) -> HandlerOutcome {
    if notes
        .message(order)
        .to("options.note.out")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The broker folds the options into its protocol and the publish log sees only the result, so
/// the slot view is where a test reads back what the call site asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_slot_view_reads_back_the_options_a_publish_carried() {
    let app = RustStream::new(AppInfo::new("options-view", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(record)
                .out(Ledger, PriorityPublish::default().priority(3))
                .build();
            b.include(note)
                .out(Notes, PriorityPublish::default().priority(3))
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("options.in")
        .publish()
        .await
        .expect("publish");
    tb.message(&Order { id: 7 })
        .to("options.note.in")
        .publish()
        .await
        .expect("publish");

    // --8<-- [start:options_assert]
    tb.out::<Ledger>()
        .assert_called(2)
        .with_options(&PriorityOptions { priority: Some(9) });
    tb.out::<Notes>()
        .assert_called_once()
        .assert_options_default();
    // --8<-- [end:options_assert]
}

/// One delivery through `record`: the slot view holds an unstepped publish followed by a stepped
/// one, and the broker's log holds both.
async fn one_delivery() -> TestApp<()> {
    let app = RustStream::new(AppInfo::new("options-panics", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(record)
                .out(Ledger, PriorityPublish::default().priority(3))
                .build();
            b.include(note)
                .out(Notes, PriorityPublish::default().priority(3))
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");
    tb.message(&Order { id: 7 })
        .to("options.in")
        .publish()
        .await
        .expect("publish");
    tb.message(&Order { id: 7 })
        .to("options.note.in")
        .publish()
        .await
        .expect("publish");
    tb
}

/// Another broker's options type, for the assertion that names the wrong one.
#[derive(Debug, PartialEq, Eq)]
struct TtlOptions {
    ttl: Option<u8>,
}

/// The publish log has consumed the options by the time it records the message, so asking it for
/// them is a test-authoring mistake and the panic says where to ask instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "does not record per-message options")]
async fn the_publish_log_sends_an_options_assertion_to_the_slot_view() {
    let tb = one_delivery().await;

    tb.broker::<MemoryBroker>()
        .published::<Order>("options.urgent")
        .assert_called_once()
        .with_options(&PriorityOptions { priority: Some(9) });
}

/// Naming another broker's options type is the same mistake, and the panic names the type that
/// was actually recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "PriorityOptions`, not `")]
async fn options_of_another_type_name_the_recorded_one() {
    let tb = one_delivery().await;

    tb.out::<Ledger>()
        .assert_called(2)
        .with_options(&TtlOptions { ttl: Some(9) });
}

/// Expecting a value where no step ran reads as a missing step, so the panic says the publish
/// took the policy's defaults.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "published with the policy's default options")]
async fn expecting_options_on_an_unstepped_publish_says_so() {
    let tb = one_delivery().await;

    tb.out::<Notes>()
        .assert_called_once()
        .with_options(&PriorityOptions { priority: Some(9) });
}

/// The mirror mistake: a step did run, so the assertion that nothing was set names what it found.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "published with per-message options set")]
async fn defaults_asserted_on_a_stepped_publish_name_the_options() {
    let tb = one_delivery().await;

    tb.out::<Ledger>().assert_called(2).assert_options_default();
}
