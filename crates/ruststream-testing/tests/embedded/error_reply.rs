//! A reply the embedded runner encodes with the default codec and the bus refuses: the delivery
//! is acknowledged, and the context's `.error` is handed the bytes the codec wrote.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, Failures, HandlerOutcome, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use serde::Serialize;

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

/// An ask, as its bytes.
#[derive(Deserialized)]
struct Ask<'a>(&'a [u8]);

/// A message the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The answer, which the default codec writes.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Verdict {
    ok: bool,
}

/// One answer the `.error` continuation was told about: where it was going, and its bytes.
type LostAnswer = (String, Vec<u8>);

/// What the `.error` continuation was told.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<LostAnswer>>>);

/// Answers an ask. If the answer cannot be published, the context's `.error` records it.
#[subscriber(LocalMemorySource::new("ask"), reply)]
async fn on_ask(ask: &Ask<'_>, ctx: &mut Context<'_, (), Lost>) -> Verdict {
    let lost = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            for failed in failures {
                let (_error, message) = failed.into_parts();
                lost.0.lock().expect("the handlers never panic").push((
                    message.destination().to_owned(),
                    message.payload().expect("the bytes").to_vec(),
                ));
            }
        });
    let _ = ask.0;
    Verdict { ok: true }
}

/// Holds its delivery for an hour of the mock clock, so the answers subscription stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(ask: &Ask<'_>) -> HandlerOutcome {
    let _ = ask.0;
    Timer::after(EmbassyDuration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn reply_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_answers_stuck)
                .include(on_ask)
                .out_reply(LocalMemoryPublish)
        })
}

#[test]
fn an_encoded_reply_the_bus_refuses_reaches_the_error_continuation_as_encoded()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(reply_app(TightBus::new()), move |_| shared).await?;
        // The first message fills the one slot of the answers subscription for the hour.
        for (destination, body) in [("answers", b"x"), ("ask", b"?")] {
            tb.broker::<TightBus>()
                .message(&Sent(body))
                .to(destination)
                .publish()
                .await?;
        }

        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(
            *lost.0.lock().expect("the handlers never panic"),
            [("answers".to_owned(), br#"{"ok":true}"#.to_vec())]
        );
        tb.shutdown().await?;
        Ok(())
    })
}
