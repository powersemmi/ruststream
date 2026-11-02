//! The room a device service's batch route takes in the app's future. Built where the framework's
//! `alloc` feature and the test harness are off, the build a device ships: the harness wraps every
//! handler call in a future of its own, which a device does not carry.

// The app and its handlers are not `Send`: the future is measured, not run.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]
#![cfg(not(feature = "testing"))]

use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{
    Control, EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, SubscriberSettings,
};
use ruststream::{Deserialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Takes the jobs that arrived together.
#[subscriber(LocalMemorySource::new("batched"))]
async fn on_batched(jobs: &[Job<'_>]) -> HandlerOutcome {
    let _ = jobs.iter().map(|job| job.0.len()).sum::<usize>();
    HandlerOutcome::ack()
}

fn batch_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_batched.batch::<3>()))
}

/// The size of what `run` returns, without running it: the future is a type to measure.
const fn size_of_returned<Run, Fut>(_run: &Run) -> usize
where
    Run: Fn(Bus, &'static Control) -> Fut,
{
    size_of::<Fut>()
}

/// What the app of one batch route may take, in bytes of a 64-bit host.
const BATCH_ROUTE_BUDGET: usize = 32 * 1024;

#[test]
fn a_batch_route_takes_room_for_its_batch_and_not_for_copies_of_its_settling() {
    let batch = size_of_returned(&|bus, control| batch_app(bus).run(control));
    assert!(batch <= BATCH_ROUTE_BUDGET, "{batch} bytes");
}

/// Retries the batch, and reads the copies the store could not publish.
#[subscriber(LocalMemorySource::new("errored"))]
async fn on_errored(jobs: &[Job<'_>]) -> HandlerOutcome {
    let _ = jobs.len();
    HandlerOutcome::retry_after(core::time::Duration::from_secs(1)).error(
        async move |failures: &Failures| {
            let _ = failures.len();
        },
    )
}

fn errored_app_of_4(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_errored.batch::<4>()).retry_capacity::<4>()
    })
}

fn errored_app_of_16(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_errored.batch::<16>()).retry_capacity::<16>()
    })
}

/// What the app of one batch route with a store and a uniform `.error` may take, for batches of
/// 4 and of 16, in bytes of a 64-bit host: the fan the mount holds, the failure buffer of its
/// lane and the store's records come on top of the plain route's room.
const ERRORED_BUDGETS: [usize; 2] = [48 * 1024, 72 * 1024];

#[test]
fn a_batch_route_with_a_store_and_an_error_takes_room_for_its_fan() {
    let four = size_of_returned(&|bus, control| errored_app_of_4(bus).run(control));
    let sixteen = size_of_returned(&|bus, control| errored_app_of_16(bus).run(control));
    assert!(four <= ERRORED_BUDGETS[0], "{four} bytes for a batch of 4");
    assert!(
        sixteen <= ERRORED_BUDGETS[1],
        "{sixteen} bytes for a batch of 16"
    );
}
