//! A reply the compio runner publishes and the bus refuses reaches the context's `.error` once,
//! through the compio `TestApp`.

#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{Failures, HandlerOutcome, Identity};
use ruststream::testing::compio::TestApp;
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

/// The destinations the `.error` continuation was told about.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<String>>>);

/// Answers an ask. If the answer cannot be published, the context's `.error` records it.
#[subscriber(LocalMemorySource::new("ask"), reply)]
async fn on_ask(ask: &Ask<'_>, ctx: &mut Context<'_, (), Lost>) -> Verdict {
    let lost = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            for failed in failures {
                let (_error, message) = failed.into_parts();
                lost.0
                    .lock()
                    .expect("the handlers never panic")
                    .push(message.destination().to_owned());
            }
        });
    let _ = ask.0;
    Verdict { ok: true }
}

/// Holds its delivery for an hour of the virtual clock, so the answers subscription stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(ask: &Ask<'_>) -> HandlerOutcome {
    let _ = ask.0;
    sleep(Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

fn app() -> RustStream<Identity, Lost> {
    RustStream::new(AppInfo::new("asks", "0.1.0"))
        .on_startup(async move |(): ()| Ok::<_, Infallible>(Lost::default()))
        .with_broker(TightBus::new(), |b| {
            b.include(on_answers_stuck)
                .include(on_ask)
                .out_reply(LocalMemoryPublish)
        })
}

#[test]
fn a_reply_the_bus_refuses_reaches_the_error_continuation_once() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(app(), move |_| shared).await?;
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
            ["answers".to_owned()]
        );
        tb.advance(Duration::from_secs(3600)).await?;
        tb.shutdown().await?;
        Ok(())
    })
}
