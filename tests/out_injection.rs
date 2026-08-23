//! Out injection: a handler receives a live publisher as a parameter, paired by the runtime
//! from the source attached at the include site.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use std::time::Duration;

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, DefaultSlot, HandlerResult, Out, RustStream};
use ruststream::testing::{Outcome, TestApp, expect_published};
use ruststream::{Broker, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: u64,
}

/// The destination is computed per message: exactly the case reply publishing cannot cover and
/// the injected publisher exists for.
#[subscriber("out.in")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerResult {
    let dest = if event.id.is_multiple_of(2) {
        "out.even"
    } else {
        "out.odd"
    };
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new(dest, payload.as_slice()))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

async fn expect_id(observer: &ConnectedMemoryBroker, name: &str, id: u64) {
    let seen = expect_published(observer, name, 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1, "expected one publish on {name}");
    let event: Event = serde_json::from_slice(seen[0].payload()).expect("decodes");
    assert_eq!(event.id, id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_publisher_reaches_the_handler_live() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    // The observing side needs the TestableBroker surface, which lives on the connected form.
    let observer = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

    let app = RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(broker, |b| {
        b.include(forward).publisher(MemoryPublish);
    });
    let running = app.start().await.expect("startup failed");

    for id in [2u64, 3u64] {
        ingress
            .publish(OutgoingMessage::new(
                "out.in",
                serde_json::to_vec(&Event { id }).unwrap().as_slice(),
            ))
            .await
            .expect("publish");
    }
    expect_id(&observer, "out.even", 2).await;
    expect_id(&observer, "out.odd", 3).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("out.crossing")]
async fn crossing(event: &Event, Out(out): Out<impl Publisher>) -> HandlerResult {
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new("out.other", payload.as_slice()))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_failures_are_recorded_for_out_handlers() {
    let app =
        RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(forward).publisher(MemoryPublish);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // Not valid JSON for `Event`: the Out wrapper fails to decode, the handler never runs, and
    // the harness must classify the delivery as a decode failure, exactly like the typed path.
    tb.broker::<MemoryBroker>()
        .publish_raw("out.in", b"not json")
        .await
        .expect("raw publish");

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
    let observer = Broker::connect(other.broker().clone())
        .await
        .expect("memory connect is infallible");

    // --8<-- [start:cross_broker]
    let to_other = other.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("cross", "0.1.0"))
        .with_broker(other, |b| {
            let _ = b; // the target broker may mount its own handlers here
        })
        .with_broker(ingress_broker, |b| {
            b.include(crossing).publisher(to_other);
        });
    // --8<-- [end:cross_broker]
    let running = app.start().await.expect("startup failed");

    ingress
        .publish(OutgoingMessage::new(
            "out.crossing",
            serde_json::to_vec(&Event { id: 9 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish");
    expect_id(&observer, "out.other", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The destination is computed per element, off the whole page: exactly what a reply form
/// cannot express and the injected publisher can - batch and Out compose.
#[subscriber(batch("out.page"))]
async fn forward_page(events: &[Event], Out(out): Out<impl Publisher>) -> HandlerResult {
    for event in events {
        let payload = serde_json::to_vec(event).expect("serializable");
        if out
            .publish(OutgoingMessage::new("out.paged", payload.as_slice()))
            .await
            .is_err()
        {
            return HandlerResult::retry();
        }
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_composes_with_an_out_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let observer = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

    let app = RustStream::new(AppInfo::new("out-batch", "0.1.0")).with_broker(broker, |b| {
        b.include(forward_page).publisher(MemoryPublish);
    });
    let running = app.start().await.expect("startup failed");

    for id in [4u64, 5u64] {
        ingress
            .publish(OutgoingMessage::new(
                "out.page",
                serde_json::to_vec(&Event { id }).unwrap().as_slice(),
            ))
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
async fn gate(event: &Event, Out(out): Out<impl Publisher>) -> Result<Event, HandlerResult> {
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new("out.gate.audit", payload.as_slice()))
        .await
        .is_err()
    {
        return Err(HandlerResult::retry());
    }
    Ok(Event { id: event.id + 1 })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishing_handler_composes_with_an_out_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let observer = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

    let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(broker, |b| {
        b.include(gate).out(DefaultSlot, MemoryPublish).mount();
    });
    let running = app.start().await.expect("startup failed");

    ingress
        .publish(OutgoingMessage::new(
            "out.gate",
            serde_json::to_vec(&Event { id: 7 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish");
    expect_id(&observer, "out.gate.audit", 7).await;
    expect_id(&observer, "out.gate.reply", 8).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The batch replies leave through the fixed destination while a per-page audit copy leaves
/// through the injected publisher: the batch publishing form composes with Out.
#[subscriber(batch("out.ledger"), publish("out.ledger.receipts"))]
async fn settle_page(
    events: &[Event],
    Out(out): Out<impl Publisher>,
) -> Result<Vec<Event>, HandlerResult> {
    let page = Event {
        id: u64::try_from(events.len()).expect("a page fits in u64"),
    };
    let payload = serde_json::to_vec(&page).expect("serializable");
    if out
        .publish(OutgoingMessage::new("out.ledger.pages", payload.as_slice()))
        .await
        .is_err()
    {
        return Err(HandlerResult::retry());
    }
    Ok(events
        .iter()
        .map(|event| Event { id: event.id + 100 })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_publishing_handler_composes_with_an_out_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let observer = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

    let app = RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(settle_page)
            .out(DefaultSlot, MemoryPublish)
            .mount();
    });
    let running = app.start().await.expect("startup failed");

    // One publish, one page: the audit copy and the receipt are both deterministic.
    ingress
        .publish(OutgoingMessage::new(
            "out.ledger",
            serde_json::to_vec(&Event { id: 7 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish");
    expect_id(&observer, "out.ledger.receipts", 107).await;
    expect_id(&observer, "out.ledger.pages", 1).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
