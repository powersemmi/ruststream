//! Pattern subscriptions on the in-memory broker, and the routing rule that decides who receives a
//! publish when an exact name and a pattern both match it.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::error::Error;

use ruststream::memory::{
    MemoryBroker, MemoryError, MemoryPattern, MemorySource, PatternError, Routing,
};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.eu")]
async fn europe(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(MemoryPattern::new("orders.*"))]
async fn regions(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The service: one region with a handler of its own, and a pattern over every region.
fn app(routing: Routing) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        MemoryBroker::new().routing(routing),
        |b| {
            b.include(europe);
            b.include(regions);
        },
    )
}

/// Publishes one order to `orders.eu` and one to `orders.us`.
async fn publish_both(tb: &TestApp<()>) -> Result<(), Box<dyn Error>> {
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .to("orders.eu")
        .publish()
        .await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 2 })
        .to("orders.us")
        .publish()
        .await?;
    tb.settle().await?;
    Ok(())
}

/// Every matching subscription receives the publish: the pattern sees both orders, the exact
/// name its own.
async fn every_match_reaches_the_name_and_the_pattern(
    tb: TestApp<()>,
) -> Result<(), Box<dyn Error>> {
    publish_both(&tb).await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders.eu")
        .assert_called_once()
        .with(&Order { id: 1 });
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders.*")
            .received::<Order>(),
        [Order { id: 1 }, Order { id: 2 }]
    );
    tb.shutdown().await?;
    Ok(())
}

// The default rule is `EveryMatch`, so the default broker is what runs the rule in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_match_is_the_default() -> Result<(), Box<dyn Error>> {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(europe);
            b.include(regions);
        });
    every_match_reaches_the_name_and_the_pattern(TestApp::start(app).await?).await
}

// Live, the harness waits on the broker's own answer of who is owed a publish: both
// subscriptions for `orders.eu`, the pattern alone for `orders.us`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_match_live() -> Result<(), Box<dyn Error>> {
    every_match_reaches_the_name_and_the_pattern(
        TestApp::start_live(app(Routing::EveryMatch)).await?,
    )
    .await
}

/// The exact name takes its own publish, and the pattern takes only what no name took.
async fn most_specific_leaves_the_pattern_the_rest(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
    publish_both(&tb).await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders.eu")
        .assert_called_once()
        .with(&Order { id: 1 });
    tb.broker::<MemoryBroker>()
        .subscriber("orders.*")
        .assert_called_once()
        .with(&Order { id: 2 });
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn most_specific_in_process() -> Result<(), Box<dyn Error>> {
    most_specific_leaves_the_pattern_the_rest(TestApp::start(app(Routing::MostSpecific)).await?)
        .await
}

// Live, a settle that expected the pattern to receive `orders.eu` too would wait out its deadline
// and fail: the harness reads the rule from the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn most_specific_live() -> Result<(), Box<dyn Error>> {
    most_specific_leaves_the_pattern_the_rest(
        TestApp::start_live(app(Routing::MostSpecific)).await?,
    )
    .await
}

#[subscriber(MemoryPattern::new("orders.>.eu"))]
async fn misplaced_tail(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A pattern that does not parse keeps the service from starting, and the error names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_pattern_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(misplaced_tail);
        });
    let Err(TestError::Subscribe(err)) = TestApp::start(app).await else {
        panic!("a bad pattern must refuse to start");
    };
    assert_eq!(
        err.downcast_ref::<MemoryError>(),
        Some(&MemoryError::InvalidPattern {
            pattern: "orders.>.eu".to_owned(),
            reason: PatternError::TailNotLast { token: 1 },
        })
    );
    assert_eq!(
        err.to_string(),
        "subscription pattern \"orders.>.eu\" is invalid: `>` at token 1 is not the last token; \
         it matches the rest of a name"
    );
}

#[subscriber(MemorySource::new("orders.*"))]
async fn wildcard_by_name(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A subscription by name reads one name, so a wildcard there refuses to start and points at the
// pattern descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wildcard_in_a_name_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(wildcard_by_name);
        });
    let Err(TestError::Subscribe(err)) = TestApp::start(app).await else {
        panic!("a wildcard name must refuse to start");
    };
    assert_eq!(
        err.to_string(),
        "subscription \"orders.*\" has a wildcard token and reads one name only; subscribe to a \
         pattern with `MemoryPattern::new(\"orders.*\")`"
    );
}
