//! The compio `TestApp` settles an app that reaches rest, reports one that never does, and
//! surfaces a startup failure as a `TestError`.

#![allow(clippy::future_not_send)]

use std::error::Error;
use std::fmt;
use std::time::Duration;

use compio::runtime::Runtime;
use compio::time::timeout;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, PublishError};
use ruststream::testing::TestError;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A command, as its bytes.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// A command the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "commands")]
struct Sent(&'static [u8]);

/// Asks for a retry on every delivery, so the same command comes back at once, forever.
#[subscriber(LocalMemorySource::new("commands"))]
async fn on_command(command: &Command<'_>) -> HandlerOutcome {
    let _ = command.0;
    HandlerOutcome::retry()
}

/// The reason the service cannot start.
#[derive(Debug)]
struct NoConfig;

impl fmt::Display for NoConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no configuration")
    }
}

impl Error for NoConfig {}

#[test]
fn a_handler_that_retries_forever_fails_the_publish_that_set_it_off() -> Result<(), Box<dyn Error>>
{
    let outcome = Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("commands", "0.1.0"))
            .with_broker(Bus::new(), |b| b.include(on_command));
        let tb = TestApp::start(app)
            .await
            .map_err(|source| PublishError::Publish {
                source,
                message: None,
            })?;
        tb.broker::<Bus>().message(&Sent(b"go")).publish().await
    });
    assert!(
        matches!(
            outcome,
            Err(PublishError::Publish {
                source: TestError::NotQuiescent { processed: 10_000 },
                ..
            })
        ),
        "the publish returned {outcome:?}",
    );
    Ok(())
}

#[test]
fn an_app_at_rest_settles() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("commands", "0.1.0")).with_broker(Bus::new(), |b| b);
        let tb = TestApp::start(app).await?;
        tb.settle().await?;
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_failing_on_startup_is_a_test_error() -> Result<(), Box<dyn Error>> {
    let started = Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("commands", "0.1.0"))
            .on_startup(async move |(): ()| Err::<(), _>(NoConfig))
            .with_broker(Bus::new(), |b| b);
        TestApp::start(app).await.map(|_| ())
    });
    assert!(
        matches!(started, Err(TestError::Startup(_))),
        "the start returned {started:?}",
    );
    Ok(())
}

/// How long a startup hook sleeps on the app's clock.
const WARM_UP: Duration = Duration::from_secs(3600);

/// How long the test gives a start before it calls it hung, on compio's own clock.
const PATIENCE: Duration = Duration::from_secs(2);

#[test]
fn a_startup_that_sleeps_on_the_apps_clock_does_not_hang_the_start() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("commands", "0.1.0"))
            .on_startup(async move |(): ()| {
                sleep(WARM_UP).await;
                Ok::<_, NoConfig>(())
            })
            .with_broker(Bus::new(), |b| b);
        let tb = timeout(PATIENCE, TestApp::start(app))
            .await
            .map_err(|_| "the start waited on a clock nobody moved")??;
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_startup_that_sleeps_and_then_fails_is_a_test_error() -> Result<(), Box<dyn Error>> {
    let started = Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("commands", "0.1.0"))
            .on_startup(async move |(): ()| {
                sleep(WARM_UP).await;
                Err::<(), _>(NoConfig)
            })
            .with_broker(Bus::new(), |b| b);
        timeout(PATIENCE, TestApp::start(app))
            .await
            .map(|started| started.map(|_| ()))
    });
    assert!(
        matches!(started, Ok(Err(TestError::Startup(_)))),
        "the start returned {started:?}",
    );
    Ok(())
}
