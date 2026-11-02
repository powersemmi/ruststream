//! A reply a naming transform sent to a name past the fixed name room of a build without
//! `alloc`, which the bus refused: the `.error` continuation is told the message has no
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
    EmbeddedApp, Failures, ForReply, HandlerOutcome, Mounts, Names, Outgoing as OutgoingMessage,
    PublishContext, PublishTransform,
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
