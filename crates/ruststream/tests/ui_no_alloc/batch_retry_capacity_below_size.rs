use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, SubscriberSettings};
use ruststream::{Deserialized, subscriber};

#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_jobs(_jobs: &[Job<'_>]) -> HandlerOutcome {
    HandlerOutcome::retry_after(core::time::Duration::from_secs(5))
}

// A batch of four may retry all four elements at once, and the store holds two copies.
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_jobs.batch::<4>()).retry_capacity::<2>())
}

fn main() {}
