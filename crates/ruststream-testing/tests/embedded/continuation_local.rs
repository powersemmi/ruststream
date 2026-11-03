//! A continuation on a device service's app may hold what is not `Send`: a handle on the local
//! bus and the `Rc` the handler shares with it. Only the embedded runtime alone allows it, so the
//! file is empty where the tokio runtime is on too.
#![cfg(not(feature = "tokio"))]
// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::rc::Rc;

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    Broker, BytesMut, LocalMemoryBroker, LocalMemorySource, OutgoingMessage,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Acks the job and leaves a continuation that answers on the bus once the ack is through.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_, (), Bus>) -> HandlerOutcome {
    let (bus, held) = (ctx.state().clone(), Rc::new(job.0.to_vec()));
    HandlerOutcome::ack().and_after(async move {
        let Ok(link) = bus.connect().await else {
            return;
        };
        let receipt = OutgoingMessage::produced("receipts", BytesMut::from(held.as_slice()));
        let _ = link.publisher().publish(receipt, None).await;
    })
}

/// Acks the job and registers a context hook that answers on the bus once the ack is through.
#[subscriber(LocalMemorySource::new("jobs/hook"))]
async fn on_hooked_job(job: &Job<'_>, ctx: &mut Context<'_, (), Bus>) -> HandlerOutcome {
    let (bus, held) = (ctx.state().clone(), Rc::new(job.0.to_vec()));
    ctx.after(HandlerOutcome::ack()).then(async move {
        let Ok(link) = bus.connect().await else {
            return;
        };
        let receipt = OutgoingMessage::produced("receipts", BytesMut::from(held.as_slice()));
        let _ = link.publisher().publish(receipt, None).await;
    });
    HandlerOutcome::ack()
}

/// Takes the receipt a continuation published.
#[subscriber(LocalMemorySource::new("receipts"))]
async fn on_receipt(receipt: &Job<'_>) -> HandlerOutcome {
    let _ = receipt.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Bus>> {
    EmbeddedApp::new()
        .with_state(bus.clone())
        .with_broker(bus, |b| {
            b.include(on_job).include(on_hooked_job).include(on_receipt)
        })
}

#[test]
fn a_continuation_publishes_through_the_local_bus_after_the_ack() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"7"))
            .to("jobs")
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<Bus>()
            .subscriber("receipts")
            .assert_called_once()
            .with_raw(b"7");
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_context_hook_publishes_through_the_local_bus_after_the_ack() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"5"))
            .to("jobs/hook")
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<Bus>()
            .subscriber("receipts")
            .assert_called_once()
            .with_raw(b"5");
        tb.shutdown().await?;
        Ok(())
    })
}
