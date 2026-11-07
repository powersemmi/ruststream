//! The room a device service's routes take in the app's future: a batch route's, and the places a
//! route keeps for the messages of its refused publishes. Built where the framework's `alloc`
//! feature and the test harness are off, the build a device ships: the harness wraps every handler
//! call in a future of its own, which a device does not carry.

// The app and its handlers are not `Send`: the future is measured, not run.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]
#![cfg(not(feature = "testing"))]

use core::time::Duration;

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
    HandlerOutcome::retry_after(Duration::from_secs(1)).error(async move |failures: &Failures| {
        let _ = failures.len();
    })
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
/// worker and the store's records come on top of the plain route's room.
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

/// Takes one job at a time.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0.len();
    HandlerOutcome::ack()
}

fn one_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job))
}

fn one_app_with<const PLACES: usize>(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).refusal_capacity::<PLACES>())
}

fn pool_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).pool::<4>())
}

fn pool_app_with<const PLACES: usize>(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job).pool::<4>().refusal_capacity::<PLACES>()
    })
}

fn keyed_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).keyed::<3>())
}

fn keyed_app_with<const PLACES: usize>(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job).keyed::<3>().refusal_capacity::<PLACES>()
    })
}

fn batch_app_with<const PLACES: usize>(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_batched.batch::<3>())
            .refusal_capacity::<PLACES>()
    })
}

// A route keeps a place for each delivery it holds at once unless its mount names a count: the
// room of one place is what a second place adds to a route of one worker.
#[test]
fn a_route_keeps_a_place_for_each_delivery_it_holds_at_once() {
    let one = size_of_returned(&|bus, control| one_app_with::<1>(bus).run(control));
    let place = size_of_returned(&|bus, control| one_app_with::<2>(bus).run(control)) - one;
    assert!(place > 0, "a place takes room in the app");

    let default = size_of_returned(&|bus, control| one_app(bus).run(control));
    assert_eq!(default, one, "a handler of one delivery at a time keeps one place");

    let pool = size_of_returned(&|bus, control| pool_app(bus).run(control));
    let pool_of_one = size_of_returned(&|bus, control| pool_app_with::<1>(bus).run(control));
    assert_eq!(pool - pool_of_one, 3 * place, "a pool of four keeps four places");

    let keyed = size_of_returned(&|bus, control| keyed_app(bus).run(control));
    let keyed_of_one = size_of_returned(&|bus, control| keyed_app_with::<1>(bus).run(control));
    assert_eq!(keyed - keyed_of_one, 2 * place, "three keyed workers keep three places");

    let batch = size_of_returned(&|bus, control| batch_app(bus).run(control));
    let batch_of_one = size_of_returned(&|bus, control| batch_app_with::<1>(bus).run(control));
    assert_eq!(batch - batch_of_one, 2 * place, "a batch of three keeps three places");
}
