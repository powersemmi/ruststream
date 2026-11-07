//! What the `.error` continuation is told of a reply the bus refused, on a build without `alloc`.
//!
//! The refused reply keeps its bytes in a place of the subscription, which the mount sizes. A
//! reply a naming transform sent to a name past the fixed name room is told to have no
//! destination, so a republish never goes to a name the message was not sent to.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::error::Error;
use std::rc::Rc;
use std::time::Duration;

use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    EmbeddedApp, Failures, ForReply, HandlerOutcome, MountSteps, Mounts, Names,
    Outgoing as OutgoingMessage, PublishContext, PublishTransform,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use serde::Serialize;

/// A bus whose subscriptions hold one message each.
type Tight = LocalMemoryBroker<1>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Counts what a continuation was told.
#[derive(Clone, Default)]
struct Counter(Rc<Cell<u32>>);

/// A name past the fixed room a failed message holds a destination in.
const LONG_NAME: &str = "a-destination-name-that-is-longer-than-the-fixed-room-of-sixty-four-bytes";

/// A reply that leaves its destination to the mount site, so a transform may name one.
#[derive(Outgoing, Serialize)]
struct OpenAnswer {
    ok: bool,
}

/// Sends each reply to the long name.
#[derive(Clone)]
struct ToLongName;

impl<Conn, Options> PublishTransform<ForReply<Conn>, Options> for ToLongName {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessage<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, Conn>,
    ) {
        out.set_name(LONG_NAME);
    }
}

/// Asks to be answered on the long name, which a handler there holds for good, and counts the
/// refused answers that arrive without a destination.
#[subscriber(LocalMemorySource::new("redirect"), reply("answers"))]
async fn on_redirect(ask: &Job<'_>, ctx: &mut Context<'_, (), Counter>) -> OpenAnswer {
    let _ = ask.0;
    let nameless = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            let lost = failures
                .iter()
                .filter(|failed| failed.message().destination().is_none())
                .count();
            nameless
                .0
                .set(nameless.0.get() + u32::try_from(lost).unwrap_or(u32::MAX));
        });
    }
    OpenAnswer { ok: true }
}

/// Holds the long-named subscription full.
#[subscriber(LocalMemorySource::new(LONG_NAME))]
async fn on_long_stuck(held: &Job<'_>) -> HandlerOutcome {
    let _ = held.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// A message for the long-named subscription.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "a-destination-name-that-is-longer-than-the-fixed-room-of-sixty-four-bytes")]
struct ToLong(&'static [u8]);

/// A question for the redirecting handler.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "redirect")]
struct Redirect(&'static [u8]);

fn redirect_app(bus: Tight) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| {
            b.include(on_long_stuck)
                .include(on_redirect)
                .out_reply(LocalMemoryPublish)
                .codec(PostcardCodec)
                .transform(ToLongName)
        })
}

/// One bus for the redirect scenario.
static REDIRECT_BUS: LocalMemoryBus<1> = LocalMemoryBus::new();

#[test]
fn a_refused_reply_sent_to_a_name_past_the_room_reaches_error_with_no_destination()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let nameless = Counter::default();
        let shared = nameless.clone();
        let tb =
            TestApp::with_state(redirect_app(Tight::new(&REDIRECT_BUS)), move |_| shared).await?;
        tb.broker::<Tight>()
            .message(&ToLong(b"x"))
            .publish()
            .await?;
        tb.broker::<Tight>()
            .message(&Redirect(b"?"))
            .publish()
            .await?;
        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(nameless.0.get(), 1);
        tb.shutdown().await?;
        Ok(())
    })
}

/// The first byte of each refused answer a continuation read, in the order it read them.
#[derive(Clone, Default)]
struct Kept(Rc<Cell<Option<u8>>>);

/// The answer to an ask, on a name a handler holds full.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(&'static [u8]);

/// Answers every ask, and keeps the first byte of an answer the bus refused.
#[subscriber(LocalMemorySource::new("asks"), reply)]
async fn on_ask(ask: &Job<'_>, ctx: &mut Context<'_, (), Kept>) -> Answer {
    let _ = ask.0;
    let kept = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            for failed in failures {
                let first = failed
                    .message()
                    .payload()
                    .and_then(|payload| payload.first().copied());
                kept.0.set(first);
            }
        });
    }
    Answer(b"yes")
}

/// Holds the answers subscription full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(held: &Job<'_>) -> HandlerOutcome {
    let _ = held.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// A question for the answering handler.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "asks")]
struct Ask(&'static [u8]);

fn kept_app(bus: Tight) -> EmbeddedApp<impl Mounts<State = Kept>> {
    EmbeddedApp::new()
        .with_state(Kept::default())
        .with_broker(bus, |b| {
            b.include(on_answers_stuck)
                .include(on_ask)
                .out_reply(LocalMemoryPublish)
                .refusal_capacity::<1>()
        })
}

/// One bus for the kept-answer scenario.
static KEPT_BUS: LocalMemoryBus<1> = LocalMemoryBus::new();

#[test]
fn a_refused_reply_reaches_error_with_the_bytes_its_place_kept() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let kept = Kept::default();
        let shared = kept.clone();
        let tb = TestApp::with_state(kept_app(Tight::new(&KEPT_BUS)), move |_| shared).await?;
        // The first answer fills the one slot of the answers subscription for good.
        tb.broker::<Tight>()
            .message(&Answer(b"x"))
            .publish()
            .await?;
        tb.broker::<Tight>().message(&Ask(b"?")).publish().await?;
        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(kept.0.get(), Some(b'y'));
        tb.shutdown().await?;
        Ok(())
    })
}
