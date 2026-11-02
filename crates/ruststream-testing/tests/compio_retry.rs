//! A delayed retry of a compio service's handler redelivers once the virtual clock has moved past
//! its delay, through the compio `TestApp`.

#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, RETRY_COUNT_HEADER};
use ruststream::testing::Outcome;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Asks for a retry later on a first delivery, and acknowledges the copy that carries its retry
/// count.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(LATER),
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(Bus::new(), |b| {
        b.include(on_job).out_retry(LocalMemoryPublish)
    })
}

#[test]
fn a_retry_is_redelivered_only_once_its_delay_has_passed() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"job"))
            .to("jobs")
            .publish()
            .await?;

        tb.advance(Duration::from_secs(4)).await?;
        tb.broker::<Bus>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(LATER));

        tb.advance(Duration::from_secs(1)).await?;
        let jobs = tb.broker::<Bus>();
        let calls = jobs.subscriber("jobs").assert_called(2);
        assert_eq!(
            calls.outcomes().last(),
            Some(&Outcome::Ack),
            "the copy carries its retry count to the handler"
        );
        tb.shutdown().await?;
        Ok(())
    })
}
