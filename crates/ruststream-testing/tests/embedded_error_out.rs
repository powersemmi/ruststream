//! An `Out` publisher moves into an `.error` continuation on the embedded runner: the handler
//! clones its entry with no bound beyond `impl Publisher`, and the continuation publishes
//! through it over the local bus. Only the embedded runtime alone runs it, so the file is empty
//! where the tokio runtime is on too.
#![cfg(not(feature = "tokio"))]
// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, Out};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

/// A bus with room for a few messages per subscription.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The alert for a job whose retry copy was lost: the job's bytes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "alerts")]
struct Alert(Vec<u8>);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Asks for a retry later and, if the copy is lost, raises an alert through its own `Out`.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    let job = job.0.to_vec();
    let alerts = alerts.clone();
    HandlerOutcome::retry_after(LATER).error(async move |_err, _lost| {
        alerts
            .message(&Alert(job))
            .publish()
            .await
            .expect("the bus has room for the alert");
    })
}

#[ruststream::app(runtime = embassy)]
fn alerting_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job)
            .out(Alerts, LocalMemoryPublish)
            .retry_capacity::<1>()
    })
}

#[test]
fn a_cloned_out_publishes_from_the_error_continuation_over_the_local_bus()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(alerting_app(Bus::new())).await?;
        for job in [b"one", b"two"] {
            tb.broker::<Bus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }

        // The second job finds the one record of retry copies taken: its copy is lost, and the
        // continuation raises the alert through the cloned entry.
        tb.advance(Duration::from_secs(1)).await?;

        tb.out::<Alerts>().assert_called_once().with_raw(b"two");
        tb.shutdown().await?;
        Ok(())
    })
}
