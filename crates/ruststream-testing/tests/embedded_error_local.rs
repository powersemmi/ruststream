//! An `.error` continuation on the embedded runner may hold what is not `Send`: an `Rc` here,
//! where the same closure is refused on the tokio runner. Only the embedded runtime alone allows
//! it, so the file is empty where the tokio runtime is on too.
#![cfg(not(feature = "tokio"))]
// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What the continuations recorded.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, line: &str) {
        self.0
            .lock()
            .expect("the handlers never panic")
            .push(line.to_owned());
    }

    fn told(&self) -> Vec<String> {
        self.0.lock().expect("the handlers never panic").clone()
    }
}

/// Asks for a retry later and leaves both continuations, which share an `Rc`.
#[subscriber(LocalMemorySource::new("rc-job"))]
async fn rc_job(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let log = Rc::new(ctx.state().clone());
    let on_error = Rc::clone(&log);
    HandlerOutcome::retry_after(LATER)
        .error(async move |_err, _lost| on_error.push("error"))
        .and_after(async move { log.push("then") })
}

#[ruststream::app(runtime = embassy)]
fn rc_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Log>> {
    EmbeddedApp::new()
        .with_state(Log::default())
        .with_broker(bus, |b| b.include(rc_job).retry_capacity::<1>())
}

#[test]
fn an_error_continuation_may_hold_a_value_that_is_not_send() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let log = Log::default();
        let shared = log.clone();
        let tb = TestApp::with_state(rc_app(Bus::new()), move |_| shared).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"1"))
            .to("rc-job")
            .publish()
            .await?;
        tb.advance(LATER).await?;

        assert_eq!(log.told(), ["then"]);
        tb.shutdown().await?;
        Ok(())
    })
}
