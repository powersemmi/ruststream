//! A CPU-bound subscription on dedicated threads, beside the rest of the service on a
//! current-thread runtime.
//!
//! ```text
//! cargo run --example threads --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Resize {
    id: u64,
    width: u32,
}

#[derive(Debug, Deserialize)]
struct Audit {
    id: u64,
}

/// Stands in for real work: a computation that holds its thread.
fn render(job: &Resize) -> u64 {
    (0..u64::from(job.width)).fold(job.id, |acc, x| acc.wrapping_mul(31).wrapping_add(x))
}

/// Stands in for a call to another service: I/O, which belongs on the app's runtime.
async fn notify(id: u64) {
    tokio::time::sleep(Duration::from_millis(1)).await;
    println!("resized {id}");
}

// --8<-- [start:threads]
/// Runs on one of the subscription's eight threads, from the first poll to the end; the app's
/// runtime stays free for I/O and for the other subscriptions.
#[subscriber("images.resize", threads(8))]
async fn resize(job: &Resize, Ctx(main): Ctx<MainRuntime>) -> HandlerOutcome {
    let _ = render(job);
    // The one explicit way back to the app's runtime; what it sends must be `Send`.
    main.spawn(notify(job.id));
    HandlerOutcome::ack()
}
// --8<-- [end:threads]

/// I/O-bound, so it stays on the app's runtime.
#[subscriber("audit")]
async fn audit(entry: &Audit) -> HandlerOutcome {
    println!("audit {}", entry.id);
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// The dedicated threads carry the computation, so the app's runtime needs one thread for I/O.
#[ruststream::app(flavor = "current_thread")]
fn app() -> impl App {
    RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(resize);
        b.include(audit);
    })
}
// --8<-- [end:app]
