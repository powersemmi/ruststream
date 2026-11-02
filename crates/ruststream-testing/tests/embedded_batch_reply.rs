//! The replies of a batch handler on the embedded runner: one per element, in the order of the
//! batch, and a reply the bus refuses reaches the handler's `.error` continuation with the ones
//! behind it. Driven through `TestApp` on embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several asks is
//! made the way a busy bus makes it: the first ask holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, Failures, HandlerOutcome, Mounts, SubscriberSettings};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages, waiting or in hand.
type Bus = LocalMemoryBroker<4>;

/// An ask, decoded by the codec; `0` holds the handler for a second.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Ask {
    id: u8,
}

/// The answer to one ask.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "answers")]
struct Verdict {
    id: u8,
}

/// A reply the `.error` continuation was told about: its destination and its bytes.
type LostReply = (String, Vec<u8>);

/// What the `.error` continuation was told.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<LostReply>>>);

impl Lost {
    fn told(&self) -> Vec<LostReply> {
        self.0.lock().expect("the handlers never panic").clone()
    }
}

/// Holds the handler for a second of the mock clock on the gate, so the asks behind it make one
/// batch.
async fn hold_on_gate(asks: &[Ask]) {
    if asks.iter().any(|ask| ask.id == GATE) {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
}

/// Answers every ask with its id.
#[subscriber(LocalMemorySource::new("asks"), reply)]
async fn on_asks(asks: &[Ask]) -> Vec<Verdict> {
    hold_on_gate(asks).await;
    asks.iter().map(|ask| Verdict { id: ask.id }).collect()
}

/// Answers every ask with its id, and records the replies it loses.
#[subscriber(LocalMemorySource::new("lossy"), reply)]
async fn on_lossy(asks: &[Ask], ctx: &mut Context<'_, (), Lost>) -> Vec<Verdict> {
    hold_on_gate(asks).await;
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
    asks.iter().map(|ask| Verdict { id: ask.id }).collect()
}

/// Takes the answers.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers(answer: &Verdict) -> HandlerOutcome {
    let _ = answer.id;
    HandlerOutcome::ack()
}

/// Holds its answer for an hour of the mock clock, so the answers subscription stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(answer: &Verdict) -> HandlerOutcome {
    let _ = answer.id;
    Timer::after(EmbassyDuration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_answers)
            .include(on_asks.batch(nonzero!(3)))
            .out_reply(LocalMemoryPublish)
    })
}

#[ruststream::app(runtime = embassy)]
fn lossy_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_answers_stuck)
                .include(on_lossy.batch(nonzero!(3)))
                .out_reply(LocalMemoryPublish)
        })
}

/// The id of the ask that holds the handler.
const GATE: u8 = 0;

/// Publishes the gate to `to`, then `asks` behind it, and lets the gate open: the asks arrive as
/// one batch.
async fn publish_batch(tb: &TestApp, to: &str, asks: [u8; 3]) -> Result<(), Box<dyn Error>> {
    for id in once(GATE).chain(asks) {
        tb.broker::<Bus>()
            .message(&Ask { id })
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn a_batch_answers_every_element_once_in_the_order_of_the_batch() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(reply_app(Bus::new())).await?;
        publish_batch(&tb, "asks", [1, 2, 3]).await?;

        let bus = tb.broker::<Bus>();
        assert_eq!(
            bus.subscriber("answers").received::<Verdict>(),
            [
                Verdict { id: GATE },
                Verdict { id: 1 },
                Verdict { id: 2 },
                Verdict { id: 3 },
            ],
            "the gate answers first, then the batch, element by element",
        );
        bus.subscriber("asks").assert_batch_sizes(&[1, 3]);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn replies_the_bus_refuses_reach_the_error_continuation_and_the_batch_is_still_acknowledged()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(lossy_app(Bus::new()), move |_| shared).await?;
        // The stuck consumer holds the first of these, so the answers subscription has room for
        // two of the next four replies: the gate's and the batch's first.
        for id in [7, 8] {
            tb.broker::<Bus>()
                .message(&Verdict { id })
                .publish()
                .await?;
        }
        publish_batch(&tb, "lossy", [1, 2, 3]).await?;

        assert_eq!(
            lost.told(),
            [
                ("answers".to_owned(), br#"{"id":2}"#.to_vec()),
                ("answers".to_owned(), br#"{"id":3}"#.to_vec()),
            ],
            "the reply at index 1 and the one behind it, in the order of the batch",
        );
        let bus = tb.broker::<Bus>();
        let outcomes = bus
            .subscriber("lossy")
            .assert_batch_sizes(&[1, 3])
            .outcomes();
        assert!(
            outcomes.iter().all(|outcome| *outcome == Outcome::Ack),
            "every element is acknowledged, whether or not its reply went out: {outcomes:?}",
        );
        tb.shutdown().await?;
        Ok(())
    })
}
