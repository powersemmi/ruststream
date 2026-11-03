//! A batch handler of the embedded runner whose mount names the bytes a reply is encoded into
//! and keeps no room for retries: every reply of the batch still reaches the bus. Driven through
//! `TestApp` on embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several asks is
//! made the way a busy bus makes it: the first ask holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, MountSteps, Mounts, SubscriberSettings};
use ruststream::testing::embedded::TestApp;
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages, waiting or in hand.
type Bus = LocalMemoryBroker<4>;

/// An ask, decoded by the codec; `0` holds the handler for a second.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Ask {
    id: u8,
}

/// The answer to one ask.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "answers")]
struct Verdict {
    id: u8,
}

/// The id of the ask that holds the handler.
const GATE: u8 = 0;

/// Answers every ask with its id.
#[subscriber(LocalMemorySource::new("asks"), reply)]
async fn on_asks(asks: &[Ask]) -> Vec<Verdict> {
    if asks.iter().any(|ask| ask.id == GATE) {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
    asks.iter().map(|ask| Verdict { id: ask.id }).collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_asks.batch(nonzero!(3)))
            .out_reply(LocalMemoryPublish)
            .encode_capacity::<64>()
    })
}

#[test]
fn every_reply_of_a_batch_without_retry_room_reaches_the_bus() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        for id in once(GATE).chain([1, 2, 3]) {
            tb.broker::<Bus>()
                .message(&Ask { id })
                .to("asks")
                .publish()
                .await?;
        }
        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<Bus>()
            .subscriber("asks")
            .assert_batch_sizes(&[1, 3]);
        tb.broker::<Bus>()
            .published::<()>("answers")
            .assert_called(4);
        tb.shutdown().await?;
        Ok(())
    })
}
