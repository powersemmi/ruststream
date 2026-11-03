//! The service of `examples/threads.rs` written without the `macros` feature: the dedicated
//! threads are a setting on the mount chain, and `main` picks the app's runtime by hand.
//!
//! ```text
//! cargo run --example manual_threads --no-default-features --features testing,memory,json -- run
//! cargo test --example manual_threads --no-default-features --features testing,memory,json
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::{AppRuntime, run_main_on};
use serde::{Deserialize, Serialize};
use tokio::task;

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Resize {
    id: u64,
    width: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Resize {
    type Form = CallerName;
}

impl MessageHeaders for Resize {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Resized {
    id: u64,
    checksum: u64,
}

// `#[outgoing(name = "images.resized")]` by hand: the type fixes its destination.
impl OutgoingDestination for Resized {
    type Form = FixedName;

    const DESTINATION: &'static str = "images.resized";
}

impl MessageHeaders for Resized {
    type Contract = NoHeaders;
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
struct ResizeImage;

impl Handle<Resize, Resized> for ResizeImage {
    fn handle(
        &self,
        job: &Resize,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Resized, HandlerOutcome>> {
        let checksum = render(job);
        // The explicit way back to the app's runtime; what it sends must be `Send`.
        ctx.main_runtime().spawn(notify(job.id));
        ready(Ok(Resized {
            id: job.id,
            checksum,
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(
            subscriber("images.resize", ResizeImage)
                .threads(nonzero!(8))
                .reply()
                .build(),
        )
    })
}
// --8<-- [end:threads]

// --8<-- [start:app]
/// What `#[ruststream::app(flavor = "current_thread")]` writes: the dedicated threads carry the
/// computation, so the app's runtime needs one thread for I/O.
fn main() -> ExitCode {
    run_main_on(AppRuntime::CurrentThread, app)
}
// --8<-- [end:app]

// `main` runs the builder; this test runs the same builder on the in-process harness, where the
// threads run as workers on the test's runtime.
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
