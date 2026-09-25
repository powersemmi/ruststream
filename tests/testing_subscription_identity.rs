//! The harness tells subscriptions apart by which subscription of the app they are, not by the
//! name they report, in both modes: two subscriptions reporting one name (two subscriptions on one
//! Pulsar topic both report the topic) each owe a publish to it, and so do two namesakes on two
//! labeled brokers of one production type, each on its own broker.
//!
//! The slow subscription below holds its delivery in a client-side batch until a deadline, so it
//! has neither handled the publish nor started a handler while its namesake is done with it: a
//! settle that counted by name returned in that window.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::error::Error;
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPublish, MemorySource};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream, SubscriberSettings};
use ruststream::testing::TestApp;
use ruststream::{Broker, Buffered, OutSlot, Outgoing, Publisher, nonzero, subscriber};
use serde::{Deserialize, Serialize};

/// How long the slow subscription holds a partial batch before handling it.
const HOLD: Duration = Duration::from_millis(300);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Takes an order at once.
#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Takes orders in pairs, and a lone order once `HOLD` has passed; it reports the name `orders`.
#[subscriber(Buffered::<MemorySource>::new(MemorySource::new("orders")).max_wait(HOLD))]
async fn take_orders_in_pairs(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

async fn start(app: RustStream, live: bool) -> Result<TestApp<()>, Box<dyn Error>> {
    Ok(if live {
        TestApp::start_live(app).await?
    } else {
        TestApp::start(app).await?
    })
}

/// One broker, two subscriptions reporting `orders`.
fn namesakes_app() -> RustStream {
    RustStream::new(AppInfo::new("namesakes", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(take_order);
        b.include(take_orders_in_pairs.batch(nonzero!(2)));
    })
}

/// A publish owed to two subscriptions of one name settles once both handled it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_owed_to_two_namesakes_settles_once_both_handled_it() -> Result<(), Box<dyn Error>>
{
    for live in [false, true] {
        let tb = start(namesakes_app(), live).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        // The records of both subscriptions are read under the one name they report.
        let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("orders").received();
        assert_eq!(
            seen,
            [Order { id: 1 }, Order { id: 1 }],
            "the settle returned before both subscriptions handled the order (live: {live})",
        );
        tb.shutdown().await?;
    }
    Ok(())
}

/// The slot a relay publishes orders through: in this app, a token for the other broker.
#[derive(OutSlot)]
#[publishes(Order)]
struct Relayed;

/// A job, relayed as an order to the other broker.
#[derive(Debug, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "inbox")]
struct Job {
    id: u64,
}

/// Relays a job as an order, through whichever broker its slot was bound to.
#[subscriber("inbox")]
async fn relay(job: &Job, Out(out): Out<impl Publisher, Relayed>) -> HandlerOutcome {
    if out.message(&Order { id: job.id }).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Two brokers of one production type, labeled: east takes orders slowly, west at once, and each
/// relays its inbox to the other.
fn labeled_app() -> RustStream {
    let east = MemoryBroker::new().bindable();
    let west = MemoryBroker::new().bindable();
    let to_east = east.bind(MemoryPublish);
    let to_west = west.bind(MemoryPublish);
    RustStream::new(AppInfo::new("labeled", "0.1.0"))
        .with_broker_labeled("east", east, |b| {
            b.include(take_orders_in_pairs.batch(nonzero!(2)));
            b.include(relay).out(Relayed, to_west).build();
        })
        .with_broker_labeled("west", west, |b| {
            b.include(take_order);
            b.include(relay).out(Relayed, to_east).build();
        })
}

/// Each labeled broker is addressed by its label, a publish reaches only its own broker's
/// subscriptions, and a settle waits for the slow one on the broker it went to, whatever its
/// namesake on the other broker has handled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn labeled_brokers_of_one_type_are_counted_apart() -> Result<(), Box<dyn Error>> {
    for live in [false, true] {
        let tb = start(labeled_app(), live).await?;

        tb.broker_named("west")
            .message(&Order { id: 1 })
            .publish()
            .await?;
        tb.broker_named("west")
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 });
        tb.broker_named("east")
            .subscriber("orders")
            .assert_not_called();
        tb.broker_named("east")
            .published::<Order>("orders")
            .assert_not_called();

        // West has handled an `orders` delivery already; east's is still owed.
        tb.broker_named("east")
            .message(&Order { id: 2 })
            .publish()
            .await?;
        tb.broker_named("east")
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 2 });
        tb.broker_named("west")
            .subscriber("orders")
            .assert_called_once();
        tb.broker_named("east")
            .published::<Order>("orders")
            .assert_called_once();

        // A relay on west publishes to east through a token for east: east owes it.
        tb.broker_named("west")
            .message(&Job { id: 3 })
            .publish()
            .await?;
        tb.broker_named("east")
            .subscriber("orders")
            .assert_called(2);
        tb.broker_named("east")
            .published::<Order>("orders")
            .assert_called(2);
        tb.broker_named("west")
            .published::<Order>("orders")
            .assert_called_once();
        tb.shutdown().await?;
    }
    Ok(())
}
