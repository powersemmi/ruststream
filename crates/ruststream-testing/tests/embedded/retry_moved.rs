//! A device service on a subscription whose broker moves a spent delivery itself, mounted with a
//! retry declaration: the broker applies the cap, no copy of this process ever waits, and the
//! deliveries run as on any mount.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemorySource,
    LocalMemorySubscriber,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, IntoSource, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    BrokerMoves, Deserialized, Outgoing, Serialized, SubscriptionSource, nonzero, subscriber,
};

/// The board's bus.
type Bus = LocalMemoryBroker<4>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The bus's own subscription, declared as one whose broker moves a spent delivery.
#[derive(Clone)]
struct Moved;

impl Moved {
    const fn new() -> Self {
        Self
    }
}

impl IntoSource for Moved {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedLocalMemoryBroker<4>> for Moved {
    type Subscriber = LocalMemorySubscriber<4>;
    type Copies = BrokerMoves;

    fn name(&self) -> &'static str {
        "jobs"
    }

    async fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<4>,
    ) -> Result<LocalMemorySubscriber<4>, LocalMemoryError> {
        LocalMemorySource::new("jobs").subscribe(connected).await
    }
}

#[subscriber(Moved::new())]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).max_attempts(nonzero!(3u32)))
}

#[test]
fn a_retry_declaration_on_a_subscription_whose_broker_moves_the_copies_leaves_it_running()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"job"))
            .to("jobs")
            .publish()
            .await?;

        tb.broker::<Bus>().subscriber("jobs").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
