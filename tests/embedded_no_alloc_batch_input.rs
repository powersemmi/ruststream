//! A batch handler that takes values the mount's codec decodes, on the build without `alloc`: the
//! elements are decoded into the batch's fixed storage and a retry of an element waits in the
//! mount's store, all under an allocator that fails the framework's own allocations.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::error::Error;
use std::iter::once;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryBus, LocalMemorySource};
use ruststream::runtime::{
    EmbeddedApp, HandlerOutcome, MountSteps, Mounts, Outcomes, SubscriberSettings,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{Outgoing, Serialized, subscriber};
use serde::Deserialize;

/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// This test's bus.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// The stack a batch scenario with a store of retry copies is held to: the build without
/// optimisation lays every frame of the settling out in full.
const STACK: usize = 8 * 1024 * 1024;

/// A reading, decoded by the codec; `-1` holds the handler for a second.
#[derive(Deserialize)]
struct Reading {
    celsius: i32,
}

/// What the handlers added up.
#[derive(Clone, Default)]
struct Total(Rc<Cell<i32>>);

/// A reading as the sensor sends it: postcard bytes, to the destination the test names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The postcard bytes of the readings the scenarios send: a varint of the zigzag of the value.
const GATE: &[u8] = &[1];
const TEN: &[u8] = &[20];
const TWENTY: &[u8] = &[40];
const HOT: &[u8] = &[0xac, 0x02];
const THIRTY: &[u8] = &[60];

/// Adds the readings of a batch to the total.
#[subscriber(LocalMemorySource::new("sum"))]
async fn on_sum(readings: &[Reading], ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    if readings.iter().any(|reading| reading.celsius < 0) {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
        return HandlerOutcome::ack();
    }
    let total = ctx.state();
    total
        .0
        .set(total.0.get() + readings.iter().map(|reading| reading.celsius).sum::<i32>());
    HandlerOutcome::ack()
}

/// Asks a retry for every reading above a hundred degrees, and adds the others to the total.
#[subscriber(LocalMemorySource::new("hot"))]
async fn on_hot(
    readings: &[Reading],
    ctx: &mut Context<'_, (), Total>,
) -> Outcomes<HandlerOutcome, 3> {
    if readings.iter().any(|reading| reading.celsius < 0) {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
    }
    readings
        .iter()
        .map(|reading| {
            if reading.celsius > 100 {
                HandlerOutcome::retry_after(Duration::from_secs(5))
            } else {
                let total = ctx.state();
                total.0.set(total.0.get() + reading.celsius.max(0));
                HandlerOutcome::ack()
            }
        })
        .collect()
}

fn sum_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Total>> {
    EmbeddedApp::new()
        .with_state(Total::default())
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec).include(on_sum.batch::<3>())
        })
}

fn hot_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Total>> {
    EmbeddedApp::new()
        .with_state(Total::default())
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec)
                .include(on_hot.batch::<3>())
                .retry_capacity::<3>()
        })
}

/// Publishes a gate to `to`, then `readings` behind it, and lets the gate open: the readings
/// arrive as one batch.
async fn publish_batch(
    tb: &TestApp,
    to: &str,
    readings: [&'static [u8]; 3],
) -> Result<(), Box<dyn Error>> {
    for reading in once(GATE).chain(readings) {
        tb.broker::<Bus>()
            .message(&Sent(reading))
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

/// Runs a scenario on a thread of the stack [`STACK`].
fn on_big_stack(
    scenario: impl AsyncFnOnce() -> Result<(), Box<dyn Error>> + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || block_on(scenario()).map_err(|err| err.to_string()))?
        .join()
        .map_err(|_| "the batch scenario panicked")??;
    Ok(())
}

#[test]
fn a_batch_of_decoded_values_reaches_the_handler_without_allocation() -> Result<(), Box<dyn Error>>
{
    on_big_stack(async || {
        let total = Total::default();
        let shared = total.clone();
        let tb = TestApp::with_state(sum_app(Bus::new(&BUS)), move |_| shared).await?;
        publish_batch(&tb, "sum", [TEN, TWENTY, THIRTY]).await?;

        tb.broker::<Bus>()
            .subscriber("sum")
            .assert_batch_sizes(&[1, 3]);
        assert_eq!(total.0.get(), 60);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_element_of_a_decoded_batch_that_retries_is_kept_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let total = Total::default();
        let shared = total.clone();
        let tb = TestApp::with_state(hot_app(Bus::new(&BUS)), move |_| shared).await?;
        publish_batch(&tb, "hot", [TEN, HOT, THIRTY]).await?;
        assert_eq!(total.0.get(), 40);

        tb.advance(Duration::from_secs(5)).await?;
        tb.broker::<Bus>()
            .subscriber("hot")
            .assert_batch_sizes(&[1, 3, 1]);
        tb.shutdown().await?;
        Ok(())
    })
}
