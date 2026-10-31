use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{
    EmbeddedApp, ForReply, HandlerOutcome, MountSteps, Mounts, Names, Outgoing as OutgoingMessage,
    PublishContext, PublishTransform,
};
use ruststream::{Deserialized, subscriber};

#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

struct ByTenant;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ByTenant {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessage<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.set_name("bus/jobs.north");
    }
}

#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry_after(core::time::Duration::from_secs(5))
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job)
            .out_retry(LocalMemoryPublish)
            .transform(ByTenant)
            .retry_capacity::<4>()
    })
}

fn main() {}
