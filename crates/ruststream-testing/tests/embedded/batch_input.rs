//! A batch handler of a device service that takes values the mount's codec decodes, beside the
//! batches of self-deserializing types: each element is decoded into the batch's slice, and a
//! retry of an element is kept in the mount's store like any other. Driven through `TestApp` on
//! embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several readings
//! is made the way a busy bus makes it: the first reading holds the handler for a second of the
//! mock clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, SubscriberSettings};
use ruststream::testing::embedded::TestApp;
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// What the handlers added up.
type Total = Arc<AtomicI32>;

/// A reading, decoded by the codec; `-1` holds the handler for a second.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Reading {
    celsius: i32,
}

/// Adds the readings of a batch to the total.
#[subscriber(LocalMemorySource::new("sum"))]
async fn on_sum(readings: &[Reading], ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    if readings.iter().any(|reading| reading.celsius < 0) {
        Timer::after(EmbassyDuration::from_secs(1)).await;
        return HandlerOutcome::ack();
    }
    ctx.state().fetch_add(
        readings.iter().map(|reading| reading.celsius).sum(),
        Ordering::Relaxed,
    );
    HandlerOutcome::ack()
}

/// Asks a retry for every reading above a hundred degrees, and adds the others to the total.
#[subscriber(LocalMemorySource::new("hot"))]
async fn on_hot(readings: &[Reading], ctx: &mut Context<'_, (), Total>) -> Vec<HandlerOutcome> {
    if readings.iter().any(|reading| reading.celsius < 0) {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
    readings
        .iter()
        .map(|reading| {
            if reading.celsius > 100 {
                HandlerOutcome::retry_after(Duration::from_secs(5))
            } else {
                ctx.state()
                    .fetch_add(reading.celsius.max(0), Ordering::Relaxed);
                HandlerOutcome::ack()
            }
        })
        .collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Total>> {
    EmbeddedApp::new()
        .with_state(Total::default())
        .with_broker(bus, |b| {
            b.include(on_sum.batch(nonzero!(3)))
                .include(on_hot.batch(nonzero!(3)))
                .retry_capacity::<3>()
        })
}

/// Starts the app with a total the test keeps a handle to.
async fn start() -> Result<(TestApp, Total), Box<dyn Error>> {
    let total = Total::default();
    let held = Arc::clone(&total);
    let tb = TestApp::with_state(app(Bus::new()), move |_| held).await?;
    Ok((tb, total))
}

/// Publishes a gate to `to`, then `readings` behind it, and lets the gate open: the readings
/// arrive as one batch.
async fn publish_batch(tb: &TestApp, to: &str, readings: [i32; 3]) -> Result<(), Box<dyn Error>> {
    for celsius in once(-1).chain(readings) {
        tb.broker::<Bus>()
            .message(&Reading { celsius })
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn a_batch_of_decoded_values_reaches_the_handler_as_one_slice() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, total) = start().await?;
        publish_batch(&tb, "sum", [10, 20, 30]).await?;

        tb.broker::<Bus>()
            .subscriber("sum")
            .assert_batch_sizes(&[1, 3]);
        assert_eq!(total.load(Ordering::Relaxed), 60);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_element_of_a_decoded_batch_that_retries_is_kept_and_comes_back() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let (tb, total) = start().await?;
        publish_batch(&tb, "hot", [10, 150, 30]).await?;
        assert_eq!(total.load(Ordering::Relaxed), 40);

        tb.advance(Duration::from_secs(5)).await?;
        let batches = tb.broker::<Bus>().subscriber("hot").batches::<Reading>();
        assert_eq!(
            batches.last(),
            Some(&vec![Reading { celsius: 150 }]),
            "the kept copy of the hot reading is handled again, alone"
        );
        tb.shutdown().await?;
        Ok(())
    })
}
