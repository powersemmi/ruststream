use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
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
            .out_retry(LocalMemoryPublish)
            .to("bus/jobs.later")
    })
}

fn main() {}
