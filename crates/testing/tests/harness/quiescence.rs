//! A handler that asks for a retry on every delivery never lets the tokio app settle: the
//! harness reports it with the number of deliveries the step dispatched.

use std::error::Error;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishError, RustStream};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// A command the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "commands")]
struct Command {
    id: u64,
}

/// A note the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "notes")]
struct Note {
    id: u64,
}

/// Asks for a retry on every delivery, so the same command comes back at once, forever.
#[subscriber("commands")]
async fn on_command(command: &Command) -> HandlerOutcome {
    let _ = command.id;
    HandlerOutcome::retry()
}

/// Acknowledges every note.
#[subscriber("notes")]
async fn on_note(note: &Note) -> HandlerOutcome {
    let _ = note.id;
    HandlerOutcome::ack()
}

#[tokio::test]
async fn the_count_a_runaway_publish_reports_is_the_deliveries_of_that_step()
-> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_command).include(on_note)
    });
    let tb = TestApp::start(app).await?;
    // A thousand deliveries settle first; they are not part of the runaway step.
    for id in 0..1_000 {
        tb.broker::<MemoryBroker>()
            .message(&Note { id })
            .publish()
            .await?;
    }

    let outcome = tb
        .broker::<MemoryBroker>()
        .message(&Command { id: 1 })
        .publish()
        .await;

    let Err(PublishError::Publish {
        source: TestError::NotQuiescent { processed },
        ..
    }) = outcome
    else {
        panic!("the publish returned {outcome:?}");
    };
    // The step's budget is 10,000; the thousand deliveries before it would push a total past it.
    assert!(
        (10_000..11_000).contains(&processed),
        "the report counts {processed} deliveries",
    );
    tb.shutdown().await?;
    Ok(())
}
