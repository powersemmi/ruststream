//! Out injection: a handler receives a live publisher as a parameter, paired by the runtime
//! from the source attached at the include site.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::time::Duration;

use common::{Event, Wire, connected, expect_id, observed_memory};

use ruststream::memory::prelude::*;
use ruststream::testing::{Outcome, TestApp, expect_published};

/// The destination is computed per message: exactly the case reply publishing cannot cover and
/// the injected publisher exists for.
#[subscriber("out.in")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let dest = if event.id.is_multiple_of(2) {
        "out.even"
    } else {
        "out.odd"
    };
    if out.message(event).to(dest).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_publisher_reaches_the_handler_live() {
    let (broker, ingress, observer) = observed_memory().await;

    let app = RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(broker, |b| {
        b.include(forward).out(DefaultSlot, Publish).build();
    });
    let running = app.start().await.expect("startup failed");

    for id in [2u64, 3u64] {
        ingress
            .message(&Event { id })
            .to("out.in")
            .publish()
            .await
            .expect("publish");
    }
    expect_id(&observer, "out.even", 2).await;
    expect_id(&observer, "out.odd", 3).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("out.crossing")]
async fn crossing(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out.message(event).to("out.other").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_failures_are_recorded_for_out_handlers() {
    let app =
        RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(forward).out(DefaultSlot, Publish).build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // Not valid JSON for `Event`: the Out wrapper fails to decode, the handler never runs, and
    // the harness must classify the delivery as a decode failure, exactly like the typed path.
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"not json"))
        .to("out.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("out.in")
        .assert_called_once()
        .assert_outcome(Outcome::DecodeFailed)
        .assert_last_failed_to_decode();
}

/// The cross-broker case: the handler consumes one broker and its injected publisher targets
/// another, through a token minted by the target broker's scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_token_injects_a_foreign_brokers_publisher() {
    let ingress_broker = MemoryBroker::new();
    let ingress = ingress_broker.publisher();
    let other = MemoryBroker::new().bindable();
    let observer = connected(other.broker()).await;

    // --8<-- [start:cross_broker]
    let to_other = other.bind(Publish);
    let app = RustStream::new(AppInfo::new("cross", "0.1.0"))
        .with_broker(other, |b| {
            let _ = b; // the target broker may mount its own handlers here
        })
        .with_broker(ingress_broker, |b| {
            b.include(crossing).out(DefaultSlot, to_other).build();
        });
    // --8<-- [end:cross_broker]
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 9 })
        .to("out.crossing")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "out.other", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The destination is computed per element, off the whole page: exactly what a reply form
/// cannot express and the injected publisher can - batch and Out compose.
#[subscriber("out.page")]
async fn forward_page(events: &[Event], Out(out): Out<impl Publisher>) -> HandlerOutcome {
    for event in events {
        if out.message(event).to("out.paged").publish().await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_composes_with_an_out_parameter() {
    let (broker, ingress, observer) = observed_memory().await;

    let app = RustStream::new(AppInfo::new("out-batch", "0.1.0")).with_broker(broker, |b| {
        b.include(forward_page.batch(nonzero!(8)))
            .out(DefaultSlot, Publish)
            .build();
    });
    let running = app.start().await.expect("startup failed");

    for id in [4u64, 5u64] {
        ingress
            .message(&Event { id })
            .to("out.page")
            .publish()
            .await
            .expect("publish");
    }
    let seen = expect_published(&observer, "out.paged", 2, Duration::from_secs(2)).await;
    let ids: Vec<u64> = seen
        .iter()
        .map(|m| {
            serde_json::from_slice::<Event>(m.payload())
                .expect("decodes")
                .id
        })
        .collect();
    assert_eq!(ids, [4, 5], "forwards in delivery order");

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The reply leaves through the fixed destination while an audit copy leaves through the
/// injected publisher: publish and Out compose, each side with its own attachment.
#[subscriber("out.gate", publish("out.gate.reply"))]
async fn gate(event: &Event, Out(out): Out<impl Publisher>) -> Result<Event, HandlerOutcome> {
    if out
        .message(event)
        .to("out.gate.audit")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(Event { id: event.id + 1 })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishing_handler_composes_with_an_out_parameter() {
    let (broker, ingress, observer) = observed_memory().await;

    let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(broker, |b| {
        b.include(gate).out(DefaultSlot, Publish).build();
    });
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 7 })
        .to("out.gate")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "out.gate.audit", 7).await;
    expect_id(&observer, "out.gate.reply", 8).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The batch replies leave through the fixed destination while a per-page audit copy leaves
/// through the injected publisher: the batch publishing form composes with Out.
#[subscriber("out.ledger", publish("out.ledger.receipts"))]
async fn settle_page(
    events: &[Event],
    Out(out): Out<impl Publisher>,
) -> Result<Vec<Event>, HandlerOutcome> {
    let page = Event {
        id: u64::try_from(events.len()).expect("a page fits in u64"),
    };
    if out
        .message(&page)
        .to("out.ledger.pages")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(events
        .iter()
        .map(|event| Event { id: event.id + 100 })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_publishing_handler_composes_with_an_out_parameter() {
    let (broker, ingress, observer) = observed_memory().await;

    let app = RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(settle_page.batch(nonzero!(8)))
            .out(DefaultSlot, Publish)
            .build();
    });
    let running = app.start().await.expect("startup failed");

    // One publish, one page: the audit copy and the receipt are both deterministic.
    ingress
        .message(&Event { id: 7 })
        .to("out.ledger")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "out.ledger.receipts", 107).await;
    expect_id(&observer, "out.ledger.pages", 1).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
