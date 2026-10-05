//! The carried lane: a handler takes the value a subscription's deliveries already hold, with no
//! codec between the broker and the body.

#![cfg(all(feature = "macros", feature = "memory", feature = "testing"))]

mod carrying;
mod common;

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};

use carrying::{PageContext, PageRows, Row, Rows};
use common::Wire;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

#[subscriber(Rows::new("rows"))]
async fn greet(row: &Row) -> HandlerOutcome {
    tracing::info!(row.id, "greeted");
    HandlerOutcome::ack()
}

fn greeting() -> RustStream {
    RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(greet);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_reads_the_value_its_delivery_carries() -> Result<(), Box<dyn std::error::Error>>
{
    let tb = TestApp::start(greeting()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(7, "alice").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .with_value(&Row::new(7, "alice"))
        .settled(HandlerOutcome::ack());
    Ok(())
}

#[subscriber(Rows::new("rows"))]
async fn tolerant(row: &Row) -> HandlerOutcome {
    tracing::info!(row.id, "handled");
    HandlerOutcome::ack()
}

/// A delivery whose row is gone carries no value, and the subscription's decode policy settles
/// it, exactly as it settles a payload that does not decode.
fn skipping() -> RustStream {
    RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(tolerant.on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)));
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_without_a_value_is_dropped_by_default() -> Result<(), Box<dyn std::error::Error>>
{
    let tb = TestApp::start(greeting()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of("gone"))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_without_a_value_settles_by_the_decode_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(skipping()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of("gone"))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .settled(HandlerOutcome::ack())
        .assert_last_failed_to_decode();
    Ok(())
}

/// The reply a carried handler answers with.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Outgoing)]
#[outgoing(name = "greetings")]
struct Greeting {
    text: String,
}

#[subscriber(Rows::new("rows"), reply)]
async fn welcome(row: &Row) -> Greeting {
    Greeting {
        text: format!("hello, {}", row.name),
    }
}

fn welcoming() -> RustStream {
    RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(welcome).out_reply(Publish);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_handler_replies() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(welcoming()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(3, "carol").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .with_value(&Row::new(3, "carol"));
    tb.broker::<MemoryBroker>()
        .published::<Greeting>("greetings")
        .assert_called_once()
        .with(&Greeting {
            text: "hello, carol".to_owned(),
        });
    Ok(())
}

/// The event a carried handler publishes through a slot.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Outgoing)]
#[outgoing(name = "audit")]
struct Seen {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Seen)]
struct Audit;

#[subscriber(Rows::new("rows"))]
async fn audit(row: &Row, Out(out): Out<impl Publisher, Audit>) -> HandlerOutcome {
    if out.message(&Seen { id: row.id }).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn auditing() -> RustStream {
    RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(audit).out(Audit, Publish).build();
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_handler_publishes_through_a_slot() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(auditing()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(4, "dave").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .with_value(&Row::new(4, "dave"))
        .settled(HandlerOutcome::ack());
    tb.out::<Audit>()
        .assert_called_once()
        .decoded_as::<Seen>()
        .with(&Seen { id: 4 });
    Ok(())
}

/// Bytes a carried handler answers with as they are, through no codec.
#[derive(Debug, Outgoing, Serialized)]
#[outgoing(name = "receipts")]
struct Receipt(Vec<u8>);

#[subscriber(Rows::new("rows"), reply)]
async fn receipt(row: &Row) -> Receipt {
    Receipt(format!("seen {}", row.id).into_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_handler_replies_with_its_own_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(receipt).out_reply(Publish);
        });
    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(5, "erin").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .published::<()>("receipts")
        .assert_called_once()
        .with_raw(b"seen 5");
    Ok(())
}

#[subscriber(Rows::new("rows"), reply)]
async fn announce(row: &Row, Out(out): Out<impl Publisher, Audit>) -> Greeting {
    if out.message(&Seen { id: row.id }).publish().await.is_err() {
        tracing::warn!(row.id, "the audit event was not published");
    }
    Greeting {
        text: format!("welcome, {}", row.name),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_handler_replies_and_publishes_through_a_slot()
-> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(announce)
                .out_reply(Publish)
                .out(Audit, Publish)
                .build();
        });
    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(6, "frank").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .published::<Greeting>("greetings")
        .assert_called_once()
        .with(&Greeting {
            text: "welcome, frank".to_owned(),
        });
    tb.out::<Audit>()
        .assert_called_once()
        .decoded_as::<Seen>()
        .with(&Seen { id: 6 });
    Ok(())
}

/// The manual path: a body implementing `Handle` for the carried type, mounted by value.
struct Greeter;

impl Handle<Row> for Greeter {
    fn handle(
        &self,
        row: &Row,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(row.id, "greeted by value");
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_body_mounts_by_value() -> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber(Rows::new("rows"), Greeter).build());
        });
    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(Row::new(8, "grace").payload()))
        .to("rows")
        .publish()
        .await?;

    tb.broker::<MemoryBroker>()
        .subscriber("rows")
        .assert_called_once()
        .with_value(&Row::new(8, "grace"))
        .settled(HandlerOutcome::ack());
    Ok(())
}

/// The generated document describes the carried type the way it describes a decoded one: its
/// schema comes from the type, and no codec reads it, so no media type is reported.
#[cfg(feature = "asyncapi")]
#[test]
fn the_document_describes_the_carried_type() {
    use ruststream::asyncapi::build_spec;

    let spec = build_spec(&greeting());
    let row = &spec.components.messages["Row"];
    let schema = row.payload.as_ref().expect("the carried type's schema");
    assert!(schema["properties"].get("id").is_some(), "{schema}");
    assert!(schema["properties"].get("name").is_some(), "{schema}");
    assert_eq!(row.content_type, None);
}

/// A value-path registration is documented by default, and `.undocumented()` lifts the schema.
#[cfg(feature = "asyncapi")]
#[test]
fn an_undocumented_carried_registration_reports_no_schema() {
    use ruststream::asyncapi::build_spec;

    let documented =
        RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber(Rows::new("rows"), Greeter).build());
        });
    assert!(
        build_spec(&documented)
            .components
            .messages
            .values()
            .any(|m| m.payload.is_some())
    );

    let undocumented =
        RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(
                subscriber(Rows::new("rows"), Greeter)
                    .undocumented()
                    .build(),
            );
        });
    assert!(
        build_spec(&undocumented)
            .components
            .messages
            .values()
            .all(|m| m.payload.is_none())
    );
}

#[subscriber(Rows::new("rows"))]
async fn tally(rows: &[Row]) -> HandlerOutcome {
    tracing::info!(count = rows.len(), "tallied");
    HandlerOutcome::ack()
}

/// A retaining bus that already holds `rows`: a subscription opened at the start of its log
/// replays them, so the first batch carries all of them.
async fn holding(rows: &[&str]) -> Result<MemoryBroker<Retaining>, Box<dyn std::error::Error>> {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for row in rows {
        publisher
            .message(&Wire::of(row))
            .to("rows")
            .publish()
            .await?;
    }
    Ok(broker)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_reads_the_values_its_batch_lends() -> Result<(), Box<dyn std::error::Error>>
{
    let broker = holding(&["1:ann", "2:bob", "3:cyd"]).await?;
    let app = RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(broker, |b| {
        b.include(tally.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    assert_eq!(
        broker.subscriber("rows").received_values::<Row>(),
        [Row::new(1, "ann"), Row::new(2, "bob"), Row::new(3, "cyd")],
    );
    broker
        .subscriber("rows")
        .assert_batch_sizes(&[3])
        .settled(HandlerOutcome::ack());
    Ok(())
}

/// A page whose middle row is gone: the batch lends the two rows it still holds, and the
/// delivery whose row is gone goes past the end of the slice, where the decode policy settles it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_delivery_without_a_value_is_dropped_by_default()
-> Result<(), Box<dyn std::error::Error>> {
    let broker = holding(&["1:ann", "gone", "3:cyd"]).await?;
    let app = RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(broker, |b| {
        b.include(tally.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    assert_eq!(
        broker.subscriber("rows").received_values::<Row>(),
        [Row::new(1, "ann"), Row::new(3, "cyd")],
    );
    broker
        .subscriber("rows")
        .assert_called_once()
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    tb.assert_running();
    Ok(())
}

/// The decode policy is the registration's own: under `fail_fast` a delivery whose row is gone
/// stops the service, naming what the delivery did not carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_delivery_without_a_value_settles_by_the_decode_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let broker = holding(&["1:ann", "gone"]).await?;
    let app = RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(broker, |b| {
        b.include(
            tally
                .batch(nonzero!(8))
                .start_at(MemoryPosition::start())
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::FailFast)),
        );
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    let failure = tb.run_result().expect_err("the gone row stops the service");
    assert!(
        failure.to_string().contains("carries no"),
        "the failure names what the delivery lacked: {failure}"
    );
    Ok(())
}

/// Acknowledges a page only when the page's own context counts the rows the body was lent.
#[subscriber(Rows::new("rows"))]
async fn count(rows: &[Row], ctx: &mut Context<'_, PageContext>) -> HandlerOutcome {
    if ctx.context(PageRows) == rows.len() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_batch_builds_its_context_from_the_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let broker = holding(&["1:ann", "2:bob", "3:cyd"]).await?;
    let app = RustStream::new(AppInfo::new("rows", "0.0.0")).with_broker(broker, |b| {
        b.include(count.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("rows")
        .assert_batch_sizes(&[3])
        .settled(HandlerOutcome::ack());
    Ok(())
}

/// Whether bob was already refused once: application state, as a service holds any dependency.
struct Refusals {
    bob: AtomicBool,
}

/// Refuses bob once and settles everyone else, one outcome per element.
#[subscriber(Rows::new("rows"))]
async fn sift(rows: &[Row], ctx: &mut Context<'_, (), Refusals>) -> Vec<HandlerOutcome> {
    let refused = &ctx.state().bob;
    rows.iter()
        .map(|row| {
            if row.name == "bob" && !refused.swap(true, Ordering::SeqCst) {
                HandlerOutcome::retry()
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}

/// The value at index `i` belongs to the `i`-th delivery: refusing bob brings back bob's delivery,
/// which carries bob again, and the deliveries around it stay settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_batch_settles_each_delivery_by_its_own_value()
-> Result<(), Box<dyn std::error::Error>> {
    let broker = holding(&["1:ann", "2:bob", "3:cyd"]).await?;
    let app = RustStream::new(AppInfo::new("rows", "0.0.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Refusals {
                bob: AtomicBool::new(false),
            })
        })
        .with_broker(broker, |b| {
            b.include(sift.batch(nonzero!(8)).start_at(MemoryPosition::start()));
        });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    let broker = tb.broker::<MemoryBroker<Retaining>>();
    assert_eq!(
        broker.subscriber("rows").received_values::<Row>(),
        [
            Row::new(1, "ann"),
            Row::new(2, "bob"),
            Row::new(3, "cyd"),
            Row::new(2, "bob"),
        ],
    );
    broker
        .subscriber("rows")
        .assert_batch_sizes(&[3, 1])
        .settled(HandlerOutcome::ack());
    Ok(())
}
