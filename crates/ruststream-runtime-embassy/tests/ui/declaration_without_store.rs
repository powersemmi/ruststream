use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::{Deserialized, nonzero, subscriber};

#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry()
}

#[ruststream::app]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job)
            .max_attempts(nonzero!(3u32))
            .dead_letter("bus/jobs.dead")
    })
}

fn main() {}
