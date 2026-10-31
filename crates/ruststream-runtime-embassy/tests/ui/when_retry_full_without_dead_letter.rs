use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{DeadLetter, EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Deserialized, subscriber};

#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry_after(core::time::Duration::from_secs(5))
}

#[ruststream::app]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job)
            .retry_capacity::<2>()
            .when_retry_full(DeadLetter)
    })
}

fn main() {}
