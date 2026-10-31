//! A device handler that asks for an immediate retry on every delivery never lets the app settle:
//! the embedded `TestApp` reports it instead of running on.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, PublishError};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
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

/// A note the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "notes")]
struct Note(&'static [u8]);

/// Acknowledges every note.
#[subscriber(LocalMemorySource::new("notes"))]
async fn on_note(note: &Command<'_>) -> HandlerOutcome {
    let _ = note.0;
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_command).include(on_note))
}

#[test]
fn a_handler_that_retries_forever_fails_the_publish_that_set_it_off() {
    let outcome = block_on(async {
        let tb = TestApp::start(app(Bus::new()))
            .await
            .map_err(PublishError::Publish)?;
        tb.broker::<Bus>().message(&Sent(b"go")).publish().await
    });
    assert!(
        matches!(
            outcome,
            Err(PublishError::Publish(TestError::NotQuiescent {
                processed: 10_000
            }))
        ),
        "the publish returned {outcome:?}",
    );
}

#[test]
fn many_settled_steps_do_not_spend_the_budget_of_the_next() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        // More steps than the step budget holds in total, each one settled on its own.
        for _ in 0..6_000 {
            tb.broker::<Bus>().message(&Note(b"n")).publish().await?;
        }
        tb.broker::<Bus>().subscriber("notes").assert_called(6_000);
        tb.shutdown().await?;
        Ok(())
    })
}
