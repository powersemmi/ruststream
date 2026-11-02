//! A batch subscription mounted on the compio app: the handler sees the deliveries in batches of
//! the size the mount named, whether it is mounted through a definition or through a router.

use std::error::Error;
use std::num::NonZeroUsize;

use compio::runtime::Runtime;
use futures::future::join_all;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::{App, AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::testing::compio::TestApp;
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u32,
}

#[subscriber(LocalMemorySource::new("jobs"))]
async fn settle(_jobs: &[Job]) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "digests")]
struct Digest {
    id: u32,
}

#[subscriber(LocalMemorySource::new("orders"), reply)]
async fn digest(orders: &[Order]) -> Vec<Digest> {
    orders.iter().map(|order| Digest { id: order.id }).collect()
}

fn replying() -> impl App {
    RustStream::new(AppInfo::new("digests", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.include(digest.batch(NonZeroUsize::new(2).expect("two")))
                .out_reply(LocalMemoryPublish)
        },
    )
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(LocalMemoryBroker::<8>::new(), |b| {
        b.include(settle.batch(NonZeroUsize::new(2).expect("two")))
    })
}

#[test]
fn a_batch_mount_hands_the_handler_the_batches_of_its_size() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        let jobs = tb.broker::<LocalMemoryBroker<8>>();
        let jobs_in = (0..4).map(|id| Job { id }).collect::<Vec<_>>();
        for published in join_all(jobs_in.iter().map(|job| jobs.message(job).publish())).await {
            published?;
        }
        tb.settle().await?;
        jobs.subscriber("jobs")
            .assert_batch_sizes(&[2, 2])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_replying_batch_publishes_one_reply_per_delivery() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(replying()).await?;
        let orders = tb.broker::<LocalMemoryBroker<8>>();
        let placed = (0..2).map(|id| Order { id }).collect::<Vec<_>>();
        for published in join_all(placed.iter().map(|order| orders.message(order).publish())).await
        {
            published?;
        }
        tb.settle().await?;
        orders.published::<Digest>("digests").assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}
