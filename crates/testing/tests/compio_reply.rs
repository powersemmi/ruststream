//! A compio service's reply and its `Out` slot are published, and read back through the compio
//! `TestApp`.

#![allow(clippy::future_not_send)]

use std::error::Error;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A level, as its bytes.
#[derive(Deserialized)]
struct Level<'a>(&'a [u8]);

/// A level the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What the display shows.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "display")]
struct Shown(Vec<u8>);

// --8<-- [start:service]
/// The alarm a full tank raises.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "alarms")]
struct Alarm(Vec<u8>);

/// The slot alarms leave through.
#[derive(OutSlot)]
#[publishes(Alarm)]
struct Alarms;

/// Shows every level, in percent.
#[subscriber(LocalMemorySource::new("levels"), reply)]
async fn show(level: &Level<'_>) -> Shown {
    Shown([level.0, b" %"].concat())
}

/// Raises an alarm for every level.
#[subscriber(LocalMemorySource::new("tank"))]
async fn watch(level: &Level<'_>, Out(alarms): Out<impl Publisher, Alarms>) -> HandlerOutcome {
    match alarms.message(&Alarm(level.0.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("tank", "0.1.0")).with_broker(Bus::new(), |b| {
        b.include(show)
            .out_reply(LocalMemoryPublish)
            .include(watch)
            .out(Alarms, LocalMemoryPublish)
    })
}
// --8<-- [end:service]

// --8<-- [start:published]
#[test]
fn a_reply_reaches_its_destination() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"80"))
            .to("levels")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .published::<()>("display")
            .assert_called_once()
            .with_raw(b"80 %");
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:published]

// --8<-- [start:out]
#[test]
fn a_slot_publish_is_read_back_through_the_slot() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"full"))
            .to("tank")
            .publish()
            .await?;

        tb.out::<Alarms>().assert_called_once().with_raw(b"full");
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:out]
