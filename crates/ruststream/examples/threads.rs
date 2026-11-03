//! A CPU-bound subscription on dedicated threads, beside the rest of the service on a
//! current-thread runtime. The test runs the same builder on the in-process harness.
//!
//! ```text
//! cargo run --example threads --features testing,macros,memory,json -- run
//! cargo test --example threads --features testing,macros,memory,json
//! ```

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::task;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Resize {
    id: u64,
    width: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "images.resized")]
struct Resized {
    id: u64,
    checksum: u64,
}

/// Stands in for real work: a computation that holds its thread.
fn render(job: &Resize) -> u64 {
    (0..u64::from(job.width)).fold(job.id, |acc, x| acc.wrapping_mul(31).wrapping_add(x))
}

/// Stands in for a call to another service: I/O, which belongs on the app's runtime.
async fn notify(id: u64) {
    task::yield_now().await;
    tracing::info!(id, "resized");
}

// --8<-- [start:threads]
/// Runs on one of the subscription's eight threads, from the first poll to the end; the app's
/// runtime stays free for I/O and for the other subscriptions.
#[subscriber("images.resize", threads(8), reply)]
async fn resize(job: &Resize, Ctx(main): Ctx<MainRuntime>) -> Resized {
    let checksum = render(job);
    // The explicit way back to the app's runtime; what it sends must be `Send`.
    main.spawn(notify(job.id));
    Resized {
        id: job.id,
        checksum,
    }
}
// --8<-- [end:threads]

// --8<-- [start:app]
/// The dedicated threads carry the computation, so the app's runtime needs one thread for I/O.
#[ruststream::app(flavor = "current_thread")]
fn app() -> impl App {
    RustStream::new(AppInfo::new("images", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(resize))
}
// --8<-- [end:app]

// The generated `main` runs the builder; this test runs the same builder on the in-process
// harness, where the threads run as workers on the test's runtime.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Resize, Resized, app, render};

    #[tokio::test]
    async fn the_resize_is_rendered_and_answered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let job = Resize { id: 7, width: 640 };
        broker.message(&job).to("images.resize").publish().await?;

        broker
            .subscriber("images.resize")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        broker
            .published::<Resized>("images.resized")
            .assert_called_once()
            .with(&Resized {
                id: 7,
                checksum: render(&job),
            });
        Ok(())
    }
}
